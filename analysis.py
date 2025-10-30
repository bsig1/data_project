# analyze_into_chess_db.py
# ------------------------------------------------------------
# pip install python-chess tqdm
#
# What it does:
#   - Reads PGN (many games)
#   - Runs Stockfish per game (parallel, 1 thread per engine)
#   - Computes ACPL + "accuracy-like" (0..100) for White/Black
#   - Resolves each PGN game to your existing chess.db -> games.id
#   - Upserts rows into chess.db: analysis(game_id ...), FK -> games(id)
#
# Safe to re-run: it resumes/updates existing rows.
# ------------------------------------------------------------

import os, sys, time, math, hashlib, sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import chess
import chess.pgn
import chess.engine
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

# ====== EDIT THESE ======
CHESS_DB_PATH = r"C:\Users\mrsig\Desktop\data_project\chess.db"
PGN_PATH      = r"C:\Users\mrsig\Desktop\data_project\all_games.pgn"
ENGINE_PATH   = r"C:\Program Files\Stockfish\stockfish.exe"
# ========================

# Analysis knobs
MOVETIME_MS     = 20                      # 5–50ms typical
WORKERS         = max(1, cpu_count() // 2)  # start safely (half your cores)
SKIP_PLIES      = 0                       # e.g., 10–14 to skip opening
MAX_PLIES       = None                    # None or int
ENGINE_HASH_MB  = 256                     # per worker engine hash

# ---- schema for 'analysis' table in the SAME DB ----
SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS analysis (
  game_id         INTEGER PRIMARY KEY, 
  plies_analyzed  INTEGER NOT NULL,
  acpl_white      REAL NOT NULL,
  acpl_black      REAL NOT NULL,
  accuracy_white  REAL NOT NULL,
  accuracy_black  REAL NOT NULL,
  ms_total        INTEGER NOT NULL,
  engine          TEXT,
  movetime_ms     INTEGER,
  skipped_plies   INTEGER,
  notes           TEXT,
  FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
);
"""

# ----------------- SQLite helpers -----------------
def db_connect_rw():
    con = sqlite3.connect(CHESS_DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(SCHEMA_SQL)
    return con

def upsert_analysis_batch(rows: List[Tuple]):
    """
    rows: (game_id, plies_analyzed, acpl_w, acpl_b, acc_w, acc_b, ms_total, engine, movetime_ms, skipped_plies, notes)
    """
    if not rows:
        return
    con = db_connect_rw()
    cur = con.cursor()
    cur.executemany("""
        INSERT INTO analysis
        (game_id, plies_analyzed, acpl_white, acpl_black, accuracy_white, accuracy_black,
         ms_total, engine, movetime_ms, skipped_plies, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(game_id) DO UPDATE SET
          plies_analyzed=excluded.plies_analyzed,
          acpl_white=excluded.acpl_white, acpl_black=excluded.acpl_black,
          accuracy_white=excluded.accuracy_white, accuracy_black=excluded.accuracy_black,
          ms_total=excluded.ms_total, engine=excluded.engine,
          movetime_ms=excluded.movetime_ms, skipped_plies=excluded.skipped_plies,
          notes=excluded.notes
    """, rows)
    con.commit()
    con.close()

def parse_pgn_date_to_iso(s: Optional[str]) -> Optional[str]:
    if not s or s.startswith("?"): return None
    parts = s.split(".")
    try:
        y = int(parts[0])
        m = 1 if parts[1]=="??" else int(parts[1])
        d = 1 if parts[2]=="??" else int(parts[2])
        return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None

def resolve_player_id(cur, name: str) -> Optional[int]:
    cur.execute("SELECT id FROM players WHERE name=?", (name,))
    row = cur.fetchone()
    return row[0] if row else None

def resolve_game_id(conn: sqlite3.Connection, tags: Dict[str,str], uci_moves: List[str]) -> Optional[int]:
    """Try to find the games.id row this PGN game corresponds to."""
    cur = conn.cursor()
    white = (tags.get("White") or "Unknown").strip()
    black = (tags.get("Black") or "Unknown").strip()
    res   = tags.get("Result") or "*"
    date_iso = parse_pgn_date_to_iso(tags.get("Date"))
    ply_count = len(uci_moves)

    w_id = resolve_player_id(cur, white)
    b_id = resolve_player_id(cur, black)
    if w_id is None or b_id is None:
        return None

    if date_iso:
        cur.execute("""
            SELECT id FROM games
            WHERE white_id=? AND black_id=? AND result=? AND date=? AND ply_count=?
            ORDER BY id
        """, (w_id, b_id, res, date_iso, ply_count))
    else:
        cur.execute("""
            SELECT id FROM games
            WHERE white_id=? AND black_id=? AND result=? AND (date IS NULL OR date='') AND ply_count=?
            ORDER BY id
        """, (w_id, b_id, res, ply_count))
    cands = [r[0] for r in cur.fetchall()]
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0]

    # Disambiguate by first 10 UCIs
    probe = uci_moves[:10]
    for gid in cands:
        cur.execute("SELECT uci FROM moves WHERE game_id=? ORDER BY ply LIMIT ?", (gid, len(probe)))
        ucis = [r[0] for r in cur.fetchall()]
        if ucis == probe:
            return gid
    return cands[0]  # fallback

# ----------------- PGN → jobs -----------------
@dataclass
class Job:
    game_id: int
    initial_fen: str
    uci_moves: List[str]
    tags: Dict[str,str]

def scan_pgn_and_resolve_jobs(pgn_path: str, conn_for_lookup: sqlite3.Connection) -> List[Job]:
    jobs: List[Job] = []
    with open(pgn_path, encoding="utf-8", errors="ignore") as f, \
         tqdm(desc=f"Scanning {os.path.basename(pgn_path)}", unit="game") as bar:
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            tags = dict(game.headers)
            board = game.board()
            uci_moves: List[str] = []
            ok = True
            for mv in game.mainline_moves():
                try:
                    uci_moves.append(mv.uci())
                except Exception:
                    ok = False
                    break
            if ok and uci_moves:
                gid = resolve_game_id(conn_for_lookup, tags, uci_moves)
                if gid is not None:
                    jobs.append(Job(gid, board.fen(), uci_moves, tags))
            bar.update(1)
    return jobs

# ----------------- Engine analysis -----------------
_engine = None

def _init_engine():
    global _engine
    _engine = chess.engine.SimpleEngine.popen_uci(ENGINE_PATH)
    _engine.configure({"Threads": 1, "Hash": ENGINE_HASH_MB})

def _shutdown_engine():
    global _engine
    try:
        if _engine:
            _engine.quit()
    except Exception:
        pass
    _engine = None

def _score_cp_white(eval_):
    if eval_.is_mate():
        mate_ply = eval_.white().mate()
        return 100000 if mate_ply and mate_ply > 0 else -100000
    return eval_.white().score(mate_score=100000)

def _acpl_to_accuracy(acpl: float) -> float:
    # Simple monotone map: 0 ACPL -> 100; increases reduce score gently
    return max(0.0, 100.0 - 0.5 * math.sqrt(max(0.0, acpl)))

def _analyze_job(job: Job) -> Tuple[int, int, float, float, float, float, int, str, int, int, Optional[str]]:
    """
    Returns:
      (game_id, plies_analyzed, acpl_w, acpl_b, acc_w, acc_b, ms_total, engine_name, movetime_ms, skipped_plies, notes)
    """
    t0 = time.time()
    notes = []
    acpl_w = 0.0
    acpl_b = 0.0
    n_w = 0
    n_b = 0
    engine_name = "Stockfish"

    try:
        board = chess.Board(job.initial_fen)
    except Exception as e:
        return (job.game_id, 0, 0.0, 0.0, 100.0, 100.0, int((time.time()-t0)*1000), engine_name, MOVETIME_MS, SKIP_PLIES, f"bad FEN: {e}")

    limit = chess.engine.Limit(time=MOVETIME_MS/1000)
    ply_idx = 0
    try:
        for u in job.uci_moves:
            ply_idx += 1
            if SKIP_PLIES and ply_idx <= SKIP_PLIES:
                try:
                    board.push_uci(u)
                except Exception:
                    notes.append(f"illegal at ply {ply_idx}")
                    break
                continue
            if MAX_PLIES and ply_idx > MAX_PLIES:
                break

            stm_is_white = board.turn == chess.WHITE

            info_best = _engine.analyse(board, limit)
            s_best_white = _score_cp_white(info_best["score"])
            s_best_stm = s_best_white if stm_is_white else -s_best_white

            try:
                board.push_uci(u)
            except Exception:
                notes.append(f"illegal move {u} at ply {ply_idx}")
                break

            info_played = _engine.analyse(board, limit)
            s_played_white = _score_cp_white(info_played["score"])
            s_after_stm = s_played_white if (board.turn == chess.WHITE) else -s_played_white
            s_after_mover = -s_after_stm

            loss = max(0, s_best_stm - s_after_mover)
            if stm_is_white:
                acpl_w += abs(loss); n_w += 1
            else:
                acpl_b += abs(loss); n_b += 1

    except chess.engine.EngineTerminatedError:
        notes.append("engine terminated")
    except chess.engine.EngineError as e:
        notes.append(f"engine error: {e}")
    except Exception as e:
        notes.append(f"exception: {e}")

    acpl_w = acpl_w / max(1, n_w)
    acpl_b = acpl_b / max(1, n_b)
    acc_w = _acpl_to_accuracy(acpl_w)
    acc_b = _acpl_to_accuracy(acpl_b)
    ms = int((time.time() - t0) * 1000)
    note_str = "; ".join(notes) if notes else None
    return (job.game_id, (n_w+n_b), float(acpl_w), float(acpl_b), float(acc_w), float(acc_b),
            ms, engine_name, MOVETIME_MS, SKIP_PLIES, note_str)

# ----------------- Main driver -----------------
def main():
    # Basic checks
    if not Path(ENGINE_PATH).exists():
        print(f"ERROR: Stockfish not found at:\n  {ENGINE_PATH}")
        sys.exit(1)
    if not Path(PGN_PATH).exists():
        print(f"ERROR: PGN not found at:\n  {PGN_PATH}")
        sys.exit(1)
    if not Path(CHESS_DB_PATH).exists():
        print(f"ERROR: chess.db not found at:\n  {CHESS_DB_PATH}")
        sys.exit(1)

    # Ensure analysis table exists
    con = db_connect_rw()
    con.close()

    # Build jobs (and resolve to games.id now, in parent)
    con_lookup = sqlite3.connect(CHESS_DB_PATH)
    jobs = scan_pgn_and_resolve_jobs(PGN_PATH, con_lookup)
    con_lookup.close()

    if not jobs:
        print("No analyzable/resolvable games found.")
        return

    print(f"DB:       {CHESS_DB_PATH}")
    print(f"PGN:      {PGN_PATH}")
    print(f"ENGINE:   {ENGINE_PATH}")
    print(f"WORKERS:  {WORKERS}")
    print(f"SETTINGS: MOVETIME_MS={MOVETIME_MS}, SKIP_PLIES={SKIP_PLIES}, MAX_PLIES={MAX_PLIES}")

    # Analyze in parallel (workers write nothing; parent batches DB writes)
    rows: List[Tuple] = []
    with Pool(processes=WORKERS, initializer=_init_engine) as pool:
        try:
            for row in tqdm(pool.imap_unordered(_analyze_job, jobs, chunksize=2),
                            total=len(jobs), desc="Analyzing", unit="game"):
                rows.append(row)
        finally:
            # workers are terminated by Pool; engines quit in atexit but we guard anyway
            pass

    # Batch upserts
    rows.sort(key=lambda r: r[0])  # by game_id
    BATCH = 400
    for i in range(0, len(rows), BATCH):
        upsert_analysis_batch(rows[i:i+BATCH])

    print(f"Done. Analyzed {len(rows)} games.")
    print(f"Results are in {CHESS_DB_PATH} (table: analysis).")

if __name__ == "__main__":
    try:
        main()
    finally:
        # best-effort shutdown for non-pool engine use
        try:
            _shutdown_engine()
        except Exception:
            pass
