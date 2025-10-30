import sqlite3
import numpy as np
import matplotlib.pyplot as plt

DB_PATH = "chess.db"
PLAYER_ID = 2

def get_data(player_id=PLAYER_ID):
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    query = """
    WITH games_union AS (
        SELECT g.id,
               g.date AS played_at,
               g.white_elo AS elo
        FROM main.games g
        JOIN analysis a ON a.game_id = g.id
        WHERE g.date >= '2022-01-01' AND g.white_id = ?
        UNION ALL
        SELECT g.id,
               g.date AS played_at,
               g.black_elo AS elo
        FROM main.games g
        JOIN analysis a ON a.game_id = g.id
        WHERE g.date >= '2022-01-01' AND g.black_id = ?
    )
    SELECT
        id,
        played_at,
        ROUND(julianday(played_at) - MIN(julianday(played_at)) OVER (), 3) AS days,
        elo
    FROM games_union
    ORDER BY julianday(played_at);
    """
    cursor.execute(query, (player_id, player_id))
    rows = cursor.fetchall()
    connection.close()

    # Return a list of plain dicts
    return [
        {"id": r["id"], "played_at": r["played_at"], "days": float(r["days"]), "elo": int(r["elo"])}
        for r in rows
        if r["elo"] is not None and r["played_at"] is not None
    ]

def main():
    data = get_data()
    matrix = np.array([[row["days"], row["elo"]] for row in data])
    days = matrix[:, 0].astype(float)
    elo = matrix[:, 1].astype(float)


    # Least squares fit: elo = m * days + b
    m, b = np.polyfit(days, elo, 1)
    print(f"Slope (m): {m:.4f}")
    print(f"Intercept (b): {b:.4f}")

    # Predictions (use days, not elo)
    y_pred = m * days + b

    # R^2
    ss_res = np.sum((elo - y_pred) ** 2)
    ss_tot = np.sum((elo - np.mean(elo)) ** 2)
    r2 = 1 - (ss_res / ss_tot)
    print(f"R² = {r2:.4f}")

    # Plot
    plt.scatter(days, elo, alpha=0.5, label='Data')
    # Sort by days for a clean line plot
    idx = np.argsort(days)
    plt.plot(days[idx], y_pred[idx], label='Least Squares Line')
    plt.xlabel("Days since first 2022 game")
    plt.ylabel("ELO")
    plt.title("Least Squares Fit: ELO vs Days)")
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    main()