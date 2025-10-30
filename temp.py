
import sqlite3

DB_PATH = "chess.db"
def main():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    query = "SELECT * FROM eco_map"
    cursor.execute(query)
    rows = cursor.fetchall()
    m = {}
    for row in rows:
        m["eco"] = row[0]
        m["name"] = row[1]


if __name__ == "__main__":
    main()
