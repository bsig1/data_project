import sqlite3, hashlib, json, sys

DB_PATH = "chess.db"
TABLE = "games"
PK = "id"

def compute_row_hash(row_dict):
    # exclude id + self
    row_dict = {k: v for k, v in row_dict.items() if k not in (PK, "row_hash")}
    # stable JSON (no spaces, sorted keys)
    serialized = json.dumps(row_dict, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

def ensure_hash_column(cur):
    cur.execute(f"PRAGMA table_info({TABLE});")
    cols = {r[1] for r in cur.fetchall()}
    if "row_hash" not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN row_hash TEXT;")

def populate_hashes(cur):
    cur.execute(f"SELECT * FROM {TABLE};")
    rows = cur.fetchall()
    total = len(rows)
    for i, row in enumerate(rows, 1):
        rd = dict(row)
        h = compute_row_hash(rd)
        cur.execute(f"UPDATE {TABLE} SET row_hash=? WHERE {PK}=?", (h, rd[PK]))
        if i % 1000 == 0:
            print(f"Processed {i}/{total} rows...")
    print(f"Updated {total} rows.")

def count_duplicates(cur):
    cur.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT row_hash FROM {TABLE}
          WHERE row_hash IS NOT NULL
          GROUP BY row_hash
          HAVING COUNT(*) > 1
        );
    """)
    return cur.fetchone()[0]

def add_unique_index(cur):
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE}_row_hash ON {TABLE}(row_hash);")

def main():
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")

            ensure_hash_column(cur)
            populate_hashes(cur)

            dups = count_duplicates(cur)
            if dups > 0:
                print(f"[!] Found {dups} duplicate hash bucket(s). Not creating UNIQUE index.")
                print("    Run a cleanup to delete duplicates, then re-run to add the index.")
            else:
                add_unique_index(cur)
                print("✓ Unique index on row_hash created.")

            conn.commit()
            print("✓ Done.")

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
