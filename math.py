import sqlite3
import time

import numpy as np
import matplotlib.pyplot as plt
from numpy import average

DB_PATH = "chess.db"
PLAYER_ID = 2

def get_data(player_id=PLAYER_ID):
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    # Query returns all games played after 2022 with elo always being PLAYER_ID's and days being days
    # Since that date. The returned dates are also sorted.
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

    # Return a list of dicts
    return [
        {"id": r["id"], "played_at": r["played_at"], "days": float(r["days"]), "elo": int(r["elo"])}
        for r in rows
        if r["elo"] is not None and r["played_at"] is not None
    ]

def forward_substitution(L, b):
    """Solve L z = b for z where L is lower-triangular."""
    n = L.shape[0]
    z = np.zeros_like(b, dtype=float)
    for i in range(n):
        z[i] = (b[i] - L[i, :i] @ z[:i]) / L[i, i]
    return z

def back_substitution(U, z):
    """Solve U x = z for x where U is upper-triangular."""
    n = U.shape[0]
    x = np.zeros_like(z, dtype=float)
    for i in range(n - 1, -1, -1):
        x[i] = (z[i] - U[i, i+1:] @ x[i+1:]) / U[i, i]
    return x

def fit_line_cholesky(days, elo):
    """
    Manually constructs A, forms normal equations, and solves via Cholesky.
    Returns (intercept, slope).
    """
    days = np.asarray(days, dtype=float).ravel()  # n,
    y = np.asarray(elo, dtype=float).ravel()      # n,

    # A = [1  x]
    A = np.column_stack([np.ones_like(days), days])  # n x 2

    # Normal equations
    ATA = A.T @ A            # 2 x 2 (symmetric, SPD if A has full rank)
    ATy = A.T @ y            # 2,

    # Cholesky factorization: ATA = L L^T
    L = np.linalg.cholesky(ATA)

    # Solve L z = ATy  (forward)
    z = forward_substitution(L, ATy)

    # Solve L^T theta = z  (back)
    theta = back_substitution(L.T, z)

    intercept, slope = theta[0], theta[1]
    return intercept, slope

def fit_line_qr(days, elo):
    """
    Construct A = [1  x], compute reduced QR decomposition A = Q R,
    then solve R theta = Q^T y for theta = [intercept, slope]^T
    via manual back-substitution.
    """
    # Ensure 1-D float arrays
    x = np.asarray(days, dtype=float).ravel()   # shape (n,)
    y = np.asarray(elo,  dtype=float).ravel()   # shape (n,)

    # Design matrix A = [1  x]  (n x 2)
    A = np.column_stack([np.ones_like(x), x])

    # Reduced (economy) QR: A = Q R, with Q (n x 2), R (2 x 2)
    Q, R = np.linalg.qr(A, mode='reduced')

    # Right-hand side for triangular solve: Q^T y  (shape (2,))
    rhs = Q.T @ y

    # Solve R theta = Q^T y (upper-triangular) by back-substitution
    theta = back_substitution(R, rhs)

    intercept, slope = theta[0], theta[1]
    return intercept, slope

def main():
    data = get_data()
    days = np.array([row["days"] for row in data], dtype=float)
    elo = np.array([row["elo"] for row in data], dtype=float)

    X = np.column_stack((np.ones_like(days), days))
    k = np.linalg.cond(X)
    print("--Chess Data Project--")
    print(f"Condition Number: {k}")

    print()

    # solvers
    start = time.time()
    b0, b1 = fit_line_cholesky(days, elo) # slope, intercept
    print("Cholesky solved in", time.time() - start, "seconds.")
    start = time.time()
    c0, c1 = fit_line_qr(days, elo)
    print("QR solved in", time.time() - start, "seconds.")
    start = time.time()
    d1, d0 = np.polyfit(days, elo, 1)
    print("Built In solved in", time.time() - start, "seconds.")

    print("Cholesky:")
    print("intercept:", b0)
    print("slope:", b1)

    print()

    print("QR:")
    print("intercept:", c0)
    print("slope:", c1)

    print()

    print("Built in:")
    print("intercept:", d0)
    print("slope:", d1)

    print()

    y_pred = b0 + b1 * days
    ss_res = np.sum((elo - y_pred) ** 2)  # residual sum of squares
    ss_tot = np.sum((elo - np.mean(elo)) ** 2)  # total sum of squares
    r2 = 1 - (ss_res / ss_tot)
    print("Normal Equation R^2:", r2)

    y_pred = c0 + c1 * days
    ss_res = np.sum((elo - y_pred) ** 2)  # residual sum of squares
    ss_tot = np.sum((elo - np.mean(elo)) ** 2)  # total sum of squares
    r2 = 1 - (ss_res / ss_tot)
    print("QR R^2:", r2)

    y_pred = d0 + d1 * days
    ss_res = np.sum((elo - y_pred) ** 2)  # residual sum of squares
    ss_tot = np.sum((elo - np.mean(elo)) ** 2)  # total sum of squares
    r2 = 1 - (ss_res / ss_tot)
    print("Built in R^2:", r2)




    # Plot
    plt.scatter(days, elo, label='Data')
    plt.plot(days, (b0 + b1 * days), label='Cholesky')
    plt.plot(days, (c0 + c1 * days), label='QR')
    plt.plot(days, (d0 + d1 * days), label='Built in')
    plt.xlabel('Days')
    plt.ylabel('Elo')
    plt.title('Least Squares Regression Line')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()