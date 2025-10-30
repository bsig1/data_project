# load_pgns_sqlite.py
# requirements:
#   pip install python-chess tqdm

import os, sys
import datetime as dt
import sqlite3
import chess.pgn
from tqdm import tqdm

DB_PATH = "lichess.db"
INPUT_PATH = ""
SQUARE_NONE = getattr(chess, "SQUARE_NONE", None)
SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS players (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  site TEXT NOT NULL DEFAULT '',
  start_date TEXT NOT NULL DEFAULT '',   -- ISO 'YYYY-MM-DD' or ''
  UNIQUE(name, site, start_date)
);

CREATE TABLE IF NOT EXISTS games (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER,
  white_id INTEGER NOT NULL,
  black_id INTEGER NOT NULL,
  result TEXT NOT NULL CHECK (result IN ('1-0','0-1','1/2-1/2','*')),
  round TEXT,
  date TEXT,                      -- ISO 'YYYY-MM-DD'
  white_elo INTEGER,
  black_elo INTEGER,
  time_control TEXT,
  termination TEXT,
  eco TEXT,                       -- e.g., 'C65'
  opening TEXT,
  ply_count INTEGER,
  FOREIGN KEY(event_id) REFERENCES events(id),
  FOREIGN KEY(white_id) REFERENCES players(id),
  FOREIGN KEY(black_id) REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS moves (
  game_id INTEGER NOT NULL,
  ply INTEGER NOT NULL,                 -- 1-based
  move_number INTEGER,                  -- full-move number
  color TEXT NOT NULL CHECK (color IN ('W','B')),
  san TEXT NOT NULL,
  uci TEXT NOT NULL,                    -- e.g., e2e4, e1g1
  from_sq TEXT NOT NULL,                -- e.g., e2
  to_sq TEXT NOT NULL,                  -- e.g., e4
  piece TEXT NOT NULL,                  -- PNBRQK
  capture INTEGER NOT NULL DEFAULT 0,
  check   INTEGER NOT NULL DEFAULT 0,
  mate    INTEGER NOT NULL DEFAULT 0,
  promotion TEXT,
  fen_before TEXT,
  PRIMARY KEY (game_id, ply),
  FOREIGN KEY(game_id) REFERENCES games(id)
);

CREATE INDEX IF NOT EXISTS idx_games_event   ON games(event_id);
CREATE INDEX IF NOT EXISTS idx_games_players ON games(white_id, black_id);
CREATE INDEX IF NOT EXISTS idx_games_date    ON games(date);
CREATE INDEX IF NOT EXISTS idx_games_elo     ON games(white_elo, black_elo);
CREATE INDEX IF NOT EXISTS idx_games_eco     ON games(eco);
"""
def connect():
    import sqlite3
    conn = sqlite3.connect("chess.db")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA cache_size = -50000;")  # ~50MB
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.executescript(SCHEMA_SQL)
    return conn

def get_or_create_player_id(cur, name: str) -> int:
    cur.execute("SELECT id FROM players WHERE name=?", (name,))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("INSERT INTO players(name) VALUES(?)", (name,))
    return cur.lastrowid

def get_or_create_event_id(cur, name, site, date_iso):
    if not name:
        return None
    cur.execute("""
        SELECT id FROM events
        WHERE name=? AND COALESCE(site,'')=COALESCE(?, '') AND COALESCE(start_date,'')=COALESCE(?, '')
    """, (name, site, date_iso))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("INSERT INTO events(name, site, start_date) VALUES(?,?,?)",
                (name, site, date_iso))
    return cur.lastrowid

def parse_pgn_date(s):
    if not s or s.startswith("?"): return None
    parts = s.split(".")
    try:
        y = int(parts[0])
        m = 1 if parts[1] == "??" else int(parts[1])
        d = 1 if parts[2] == "??" else int(parts[2])
        return dt.date(y, m, d).isoformat()
    except Exception:
        return None

def bool_i(b): return 1 if b else 0

def insert_game(cur, g):
    cur.execute("""
        INSERT INTO games(event_id, white_id, black_id, result, round, date,
                          white_elo, black_elo, time_control, termination, eco, opening, ply_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (g["event_id"], g["white_id"], g["black_id"], g["result"], g["round"], g["date"],
          g["white_elo"], g["black_elo"], g["time_control"], g["termination"],
          g["eco"], g["opening"], g["ply_count"]))
    return cur.lastrowid

def insert_moves(cur, rows):
    cur.executemany("""
        INSERT INTO moves(game_id, ply, move_number, color, san, uci, from_sq, to_sq, piece,
                          capture, is_check, mate, promotion, fen_before)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

def process_pgn(path, commit_every=1000):
    conn = connect()
    cur = conn.cursor()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        game = chess.pgn.read_game(f)
        pbar = tqdm(total=None, desc=f"Parsing {os.path.basename(path)}")
        count = 0
        while game:
            tags = game.headers
            white = (tags.get("White") or "Unknown").strip()
            black = (tags.get("Black") or "Unknown").strip()
            result = tags.get("Result") or "*"
            rnd = tags.get("Round")
            date_iso = parse_pgn_date(tags.get("Date"))
            time_control = tags.get("TimeControl")
            termination = tags.get("Termination")
            eco = tags.get("ECO")
            opening = tags.get("Opening")
            event_name = tags.get("Event")
            site = tags.get("Site")

            def to_int(x):
                try: return int(x)
                except: return None
            white_elo = to_int(tags.get("WhiteElo"))
            black_elo = to_int(tags.get("BlackElo"))

            white_id = get_or_create_player_id(cur, white)
            black_id = get_or_create_player_id(cur, black)
            event_id = get_or_create_event_id(cur, event_name, site, date_iso)

            board = game.board()
            moves_rows = []
            ply = 0
            for move in game.mainline_moves():
                ply += 1
                fen_before = board.fen()

                # Compute SAN first (works even if from-square is empty)
                try:
                    san = board.san(move)
                except Exception:
                    san = None  # super-malformed move; we'll still store UCI and keep going

                uci = move.uci()

                # from/to squares (handle drops/none)
                if SQUARE_NONE is not None and move.from_square == SQUARE_NONE:
                    from_sq = "--"  # sentinel for "no origin" (keeps NOT NULL)
                else:
                    from_sq = chess.square_name(move.from_square)

                to_sq = chess.square_name(move.to_square)

                # Piece: prefer board lookup; if not present, derive from SAN
                piece_obj = None
                if SQUARE_NONE is None or move.from_square != SQUARE_NONE:
                    piece_obj = board.piece_at(move.from_square)

                if piece_obj is not None:
                    piece = piece_obj.symbol().upper()
                else:
                    # SAN starts with K,Q,R,B,N for non-pawns; otherwise it's a pawn move
                    if san and len(san) > 0 and san[0] in "KQRBN":
                        piece = san[0]
                    else:
                        piece = "P"

                # Promotion
                promo = chess.piece_symbol(move.promotion).upper() if move.promotion else None

                # Flags (check/capture/mate are computed AFTER pushing the move)
                is_capture = board.is_capture(move)
                board.push(move)
                is_chk = board.is_check()
                is_mate = board.is_checkmate()

                move_no = (ply + 1) // 2
                color = 'W' if ply % 2 == 1 else 'B'

                moves_rows.append((
                    None, ply, move_no, color, san, uci, from_sq, to_sq, piece,
                    1 if is_capture else 0,
                    1 if is_chk else 0,
                    1 if is_mate else 0,
                    promo, fen_before
                ))
            ginfo = dict(
                event_id=event_id, white_id=white_id, black_id=black_id, result=result, round=rnd,
                date=date_iso, white_elo=white_elo, black_elo=black_elo, time_control=time_control,
                termination=termination, eco=eco, opening=opening, ply_count=ply
            )
            game_id = insert_game(cur, ginfo)
            if moves_rows:
                moves_rows = [(game_id,) + r[1:] for r in moves_rows]
                insert_moves(cur, moves_rows)

            count += 1
            if count % commit_every == 0:
                conn.commit()

            pbar.update(1)
            game = chess.pgn.read_game(f)

        conn.commit()
        pbar.close()
    cur.close()
    conn.close()

if __name__ == "__main__":
    process_pgn(INPUT_PATH)

