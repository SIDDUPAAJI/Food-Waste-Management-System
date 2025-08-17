# ============================================
# Local Food Wastage Management System
# Step 1: Data Cleaning & SQL Schema Load
# Author: Sid + ChatGPT
# ============================================

import os
from pathlib import Path
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# ---------- CONFIG ----------
# If your files are elsewhere, change DATA_DIR or the filenames below.
DATA_DIR = Path("./")  # e.g., Path("/mnt/data") if you're running in that environment

FILES = {
    "providers": DATA_DIR / "C:/Users/sidde/Downloads/providers_data.csv",
    "receivers": DATA_DIR / "C:/Users/sidde/Downloads/receivers_data.csv",
    "food": DATA_DIR / "C:/Users/sidde/Downloads/food_listings_data.csv",
    "claims": DATA_DIR / "C:/Users/sidde/Downloads/claims_data.csv",
}

OUTPUT_DIR = DATA_DIR / "cleaned_outputs"
REJECTS_DIR = OUTPUT_DIR / "rejects"
DB_PATH = OUTPUT_DIR / "food_waste.db"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REJECTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------- HELPER: clean text ----------
def clean_text(series, title=False):
    s = series.astype(str).str.strip()
    # collapse multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True)
    if title:
        s = s.str.title()
    return s

# ---------- LOAD ----------
print("Loading CSVs...")
providers = pd.read_csv(FILES["providers"])
receivers = pd.read_csv(FILES["receivers"])
food = pd.read_csv(FILES["food"])
claims = pd.read_csv(FILES["claims"])

# ---------- BASIC NORMALIZATION ----------
# Providers
providers.rename(columns=lambda c: c.strip(), inplace=True)
expected_prov_cols = ["Provider_ID","Name","Type","Address","City","Contact"]
missing = set(expected_prov_cols) - set(providers.columns)
if missing:
    raise ValueError(f"Providers: Missing columns {missing}")

providers["Provider_ID"] = pd.to_numeric(providers["Provider_ID"], errors="coerce")
providers["Name"] = clean_text(providers["Name"])
providers["Type"] = clean_text(providers["Type"], title=True)
providers["Address"] = clean_text(providers["Address"])
providers["City"] = clean_text(providers["City"], title=True)
providers["Contact"] = clean_text(providers["Contact"])

# Receivers
receivers.rename(columns=lambda c: c.strip(), inplace=True)
expected_recv_cols = ["Receiver_ID","Name","Type","City","Contact"]
missing = set(expected_recv_cols) - set(receivers.columns)
if missing:
    raise ValueError(f"Receivers: Missing columns {missing}")

receivers["Receiver_ID"] = pd.to_numeric(receivers["Receiver_ID"], errors="coerce")
receivers["Name"] = clean_text(receivers["Name"])
receivers["Type"] = clean_text(receivers["Type"], title=True)
receivers["City"] = clean_text(receivers["City"], title=True)
receivers["Contact"] = clean_text(receivers["Contact"])

# Food Listings
food.rename(columns=lambda c: c.strip(), inplace=True)
expected_food_cols = [
    "Food_ID","Food_Name","Quantity","Expiry_Date","Provider_ID",
    "Provider_Type","Location","Food_Type","Meal_Type"
]
missing = set(expected_food_cols) - set(food.columns)
if missing:
    raise ValueError(f"Food Listings: Missing columns {missing}")

food["Food_ID"] = pd.to_numeric(food["Food_ID"], errors="coerce")
food["Food_Name"] = clean_text(food["Food_Name"], title=True)
food["Quantity"] = pd.to_numeric(food["Quantity"], errors="coerce")
food["Provider_ID"] = pd.to_numeric(food["Provider_ID"], errors="coerce")
food["Provider_Type"] = clean_text(food["Provider_Type"], title=True)
food["Location"] = clean_text(food["Location"], title=True)
food["Food_Type"] = clean_text(food["Food_Type"], title=True)
food["Meal_Type"] = clean_text(food["Meal_Type"], title=True)

# Parse Expiry_Date to date
food["Expiry_Date"] = pd.to_datetime(food["Expiry_Date"], errors="coerce").dt.date

# Claims
claims.rename(columns=lambda c: c.strip(), inplace=True)
expected_claims_cols = ["Claim_ID","Food_ID","Receiver_ID","Status","Timestamp"]
missing = set(expected_claims_cols) - set(claims.columns)
if missing:
    raise ValueError(f"Claims: Missing columns {missing}")

claims["Claim_ID"] = pd.to_numeric(claims["Claim_ID"], errors="coerce")
claims["Food_ID"] = pd.to_numeric(claims["Food_ID"], errors="coerce")
claims["Receiver_ID"] = pd.to_numeric(claims["Receiver_ID"], errors="coerce")
claims["Status"] = clean_text(claims["Status"], title=True)

# Parse Timestamp to datetime
claims["Timestamp"] = pd.to_datetime(claims["Timestamp"], errors="coerce")

# ---------- VALIDATION & FIXUPS ----------
def drop_na_ids(df, id_col, name):
    before = len(df)
    df = df.dropna(subset=[id_col])
    after = len(df)
    if before != after:
        print(f"[WARN] {name}: Dropped {before-after} rows with null {id_col}")
    df[id_col] = df[id_col].astype(int, errors="ignore")
    return df

providers = drop_na_ids(providers, "Provider_ID", "Providers")
receivers = drop_na_ids(receivers, "Receiver_ID", "Receivers")
food = drop_na_ids(food, "Food_ID", "Food_Listings")
food = drop_na_ids(food, "Provider_ID", "Food_Listings")
claims = drop_na_ids(claims, "Claim_ID", "Claims")
claims = drop_na_ids(claims, "Food_ID", "Claims")
claims = drop_na_ids(claims, "Receiver_ID", "Claims")

# Duplicates on primary keys
def drop_dupe_keys(df, key, name):
    dupe = df[df.duplicated(key, keep="first")]
    if not dupe.empty:
        print(f"[WARN] {name}: Found {len(dupe)} duplicate {key} rows -> moving to rejects")
        dupe.to_csv(REJECTS_DIR / f"{name.lower()}_duplicate_{key.lower()}.csv", index=False)
        df = df.drop_duplicates(key, keep="first")
    return df

providers = drop_dupe_keys(providers, "Provider_ID", "Providers")
receivers = drop_dupe_keys(receivers, "Receiver_ID", "Receivers")
food = drop_dupe_keys(food, "Food_ID", "Food_Listings")
claims = drop_dupe_keys(claims, "Claim_ID", "Claims")

# Quantity must be non-negative integer
bad_qty = food[food["Quantity"].isna() | (food["Quantity"] < 0)]
if not bad_qty.empty:
    print(f"[WARN] Food_Listings: {len(bad_qty)} rows with invalid Quantity -> moving to rejects")
    bad_qty.to_csv(REJECTS_DIR / "food_invalid_quantity.csv", index=False)
    food = food[~food.index.isin(bad_qty.index)]
food["Quantity"] = food["Quantity"].astype(int)

# Normalize categorical vocab
PROVIDER_TYPE_MAP = {
    "Restaurant": "Restaurant",
    "Grocery Store": "Grocery Store",
    "Supermarket": "Supermarket",
}
FOOD_TYPE_MAP = {
    "Vegetarian": "Vegetarian",
    "Non-Vegetarian": "Non-Vegetarian",
    "Vegan": "Vegan",
}
MEAL_TYPE_MAP = {
    "Breakfast": "Breakfast",
    "Lunch": "Lunch",
    "Dinner": "Dinner",
    "Snacks": "Snacks",
}
STATUS_MAP = {
    "Pending": "Pending",
    "Completed": "Completed",
    "Cancelled": "Cancelled",
    "Canceled": "Cancelled",  # unify US/UK spelling
}

food["Provider_Type"] = food["Provider_Type"].map(lambda x: PROVIDER_TYPE_MAP.get(x, x))
food["Food_Type"] = food["Food_Type"].map(lambda x: FOOD_TYPE_MAP.get(x, x))
food["Meal_Type"] = food["Meal_Type"].map(lambda x: MEAL_TYPE_MAP.get(x, x))
claims["Status"] = claims["Status"].map(lambda x: STATUS_MAP.get(x, x))

# ---------- FOREIGN KEY VALIDATION ----------
valid_provider_ids = set(providers["Provider_ID"].unique())
valid_receiver_ids = set(receivers["Receiver_ID"].unique())

# Food must reference existing Provider_ID
bad_food_fk = food[~food["Provider_ID"].isin(valid_provider_ids)]
if not bad_food_fk.empty:
    print(f"[WARN] Food_Listings: {len(bad_food_fk)} rows with unknown Provider_ID -> rejects")
    bad_food_fk.to_csv(REJECTS_DIR / "food_bad_provider_fk.csv", index=False)
    food = food[food["Provider_ID"].isin(valid_provider_ids)]

valid_food_ids = set(food["Food_ID"].unique())

# Claims must reference existing Food_ID and Receiver_ID
bad_claims_fk = claims[
    (~claims["Food_ID"].isin(valid_food_ids)) | (~claims["Receiver_ID"].isin(valid_receiver_ids))
]
if not bad_claims_fk.empty:
    print(f"[WARN] Claims: {len(bad_claims_fk)} rows with bad FK -> rejects")
    bad_claims_fk.to_csv(REJECTS_DIR / "claims_bad_fk.csv", index=False)
    claims = claims[
        (claims["Food_ID"].isin(valid_food_ids)) & (claims["Receiver_ID"].isin(valid_receiver_ids))
    ]

# Null/invalid dates
bad_expiry = food[food["Expiry_Date"].isna()]
if not bad_expiry.empty:
    print(f"[WARN] Food_Listings: {len(bad_expiry)} rows with invalid Expiry_Date -> rejects")
    bad_expiry.to_csv(REJECTS_DIR / "food_bad_expiry.csv", index=False)
    food = food[food["Expiry_Date"].notna()]

bad_ts = claims[claims["Timestamp"].isna()]
if not bad_ts.empty:
    print(f"[WARN] Claims: {len(bad_ts)} rows with invalid Timestamp -> rejects")
    bad_ts.to_csv(REJECTS_DIR / "claims_bad_timestamp.csv", index=False)
    claims = claims[claims["Timestamp"].notna()]

# Optional: align Location (food) with provider's City if missing
food["Location"] = np.where(
    food["Location"].isna() | (food["Location"] == ""),
    food["Provider_ID"].map(providers.set_index("Provider_ID")["City"]),
    food["Location"],
)

# ---------- SAVE CLEANED CSVs ----------
providers.to_csv(OUTPUT_DIR / "providers_clean.csv", index=False)
receivers.to_csv(OUTPUT_DIR / "receivers_clean.csv", index=False)
food.to_csv(OUTPUT_DIR / "food_listings_clean.csv", index=False)
claims.to_csv(OUTPUT_DIR / "claims_clean.csv", index=False)

print("Cleaned CSVs saved to:", OUTPUT_DIR.resolve())

# ---------- BUILD SQLITE DB ----------
if DB_PATH.exists():
    DB_PATH.unlink()  # rebuild cleanly
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# PRAGMA for FK enforcement
cur.execute("PRAGMA foreign_keys = ON;")

# Create tables
cur.executescript("""
DROP TABLE IF EXISTS Claims;
DROP TABLE IF EXISTS Food_Listings;
DROP TABLE IF EXISTS Receivers;
DROP TABLE IF EXISTS Providers;

CREATE TABLE Providers (
    Provider_ID INTEGER PRIMARY KEY,
    Name TEXT,
    Type TEXT,
    Address TEXT,
    City TEXT,
    Contact TEXT
);

CREATE TABLE Receivers (
    Receiver_ID INTEGER PRIMARY KEY,
    Name TEXT,
    Type TEXT,
    City TEXT,
    Contact TEXT
);

CREATE TABLE Food_Listings (
    Food_ID INTEGER PRIMARY KEY,
    Food_Name TEXT,
    Quantity INTEGER,
    Expiry_Date DATE,
    Provider_ID INTEGER,
    Provider_Type TEXT,
    Location TEXT,
    Food_Type TEXT,
    Meal_Type TEXT,
    FOREIGN KEY (Provider_ID) REFERENCES Providers(Provider_ID)
);

CREATE TABLE Claims (
    Claim_ID INTEGER PRIMARY KEY,
    Food_ID INTEGER,
    Receiver_ID INTEGER,
    Status TEXT,
    Timestamp DATETIME,
    FOREIGN KEY (Food_ID) REFERENCES Food_Listings(Food_ID),
    FOREIGN KEY (Receiver_ID) REFERENCES Receivers(Receiver_ID)
);

CREATE INDEX IF NOT EXISTS idx_food_provider ON Food_Listings(Provider_ID);
CREATE INDEX IF NOT EXISTS idx_claims_food ON Claims(Food_ID);
CREATE INDEX IF NOT EXISTS idx_claims_receiver ON Claims(Receiver_ID);
""")

conn.commit()

# Insert data
providers.to_sql("Providers", conn, if_exists="append", index=False)
receivers.to_sql("Receivers", conn, if_exists="append", index=False)
food.to_sql("Food_Listings", conn, if_exists="append", index=False)
claims.to_sql("Claims", conn, if_exists="append", index=False)

conn.commit()
print("SQLite DB created at:", DB_PATH.resolve())

# ---------- QUICK SANITY QUERIES ----------
def q(sql, params=None, head=10):
    df = pd.read_sql_query(sql, conn, params=params or {})
    print("\n---")
    print(sql)
    print(df.head(head))
    return df

# 1) Counts
q("SELECT COUNT(*) AS Providers FROM Providers;")
q("SELECT COUNT(*) AS Receivers FROM Receivers;")
q("SELECT COUNT(*) AS Food_Listings FROM Food_Listings;")
q("SELECT COUNT(*) AS Claims FROM Claims;")

# 2) Providers by City
q("""
SELECT City, COUNT(*) AS Total_Providers
FROM Providers
GROUP BY City
ORDER BY Total_Providers DESC, City ASC;
""")

# 3) Total food quantity available
q("""
SELECT SUM(Quantity) AS Total_Quantity
FROM Food_Listings;
""")

# 4) Most common food types
q("""
SELECT Food_Type, COUNT(*) AS Listings
FROM Food_Listings
GROUP BY Food_Type
ORDER BY Listings DESC;
""")

# 5) Claims status distribution
q("""
SELECT Status, COUNT(*) AS Count
FROM Claims
GROUP BY Status
ORDER BY Count DESC;
""")

# 6) Top receivers by claims count
q("""
SELECT r.Receiver_ID, r.Name, COUNT(*) AS Claims_Count
FROM Claims c
JOIN Receivers r ON r.Receiver_ID = c.Receiver_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY Claims_Count DESC, r.Name ASC
LIMIT 10;
""")

# 7) City with most listings
q("""
SELECT Location AS City, COUNT(*) AS Listings
FROM Food_Listings
GROUP BY Location
ORDER BY Listings DESC, City ASC;
""")

# 8) Claims per food item
q("""
SELECT f.Food_ID, f.Food_Name, COUNT(c.Claim_ID) AS Claims_Count
FROM Food_Listings f
LEFT JOIN Claims c ON c.Food_ID = f.Food_ID
GROUP BY f.Food_ID, f.Food_Name
ORDER BY Claims_Count DESC, f.Food_Name ASC
LIMIT 10;
""")

# 9) Provider with highest successful (Completed) claims
q("""
SELECT p.Provider_ID, p.Name, COUNT(*) AS Completed_Claims
FROM Claims c
JOIN Food_Listings f ON f.Food_ID = c.Food_ID
JOIN Providers p ON p.Provider_ID = f.Provider_ID
WHERE c.Status = 'Completed'
GROUP BY p.Provider_ID, p.Name
ORDER BY Completed_Claims DESC, p.Name ASC
LIMIT 10;
""")

# 10) % of claims by status
q("""
WITH totals AS (
  SELECT COUNT(*) AS total FROM Claims
)
SELECT Status,
       COUNT(*) AS cnt,
       ROUND(100.0 * COUNT(*) / (SELECT total FROM totals), 2) AS pct
FROM Claims
GROUP BY Status
ORDER BY cnt DESC;
""")

# 11) Avg quantity claimed per receiver (proxy: number of claims *avg food quantity per claim basis)
# NOTE: If you track exact claimed quantity, replace logic accordingly.
q("""
WITH claim_qty AS (
  SELECT c.Claim_ID, c.Receiver_ID, f.Quantity
  FROM Claims c
  JOIN Food_Listings f ON f.Food_ID = c.Food_ID
)
SELECT r.Receiver_ID, r.Name,
       ROUND(AVG(claim_qty.Quantity), 2) AS Avg_Claimed_Quantity_Proxy
FROM claim_qty
JOIN Receivers r ON r.Receiver_ID = claim_qty.Receiver_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY Avg_Claimed_Quantity_Proxy DESC, r.Name ASC
LIMIT 10;
""")

# 12) Most claimed meal type
q("""
SELECT f.Meal_Type, COUNT(*) AS Claims_Count
FROM Claims c
JOIN Food_Listings f ON f.Food_ID = c.Food_ID
GROUP BY f.Meal_Type
ORDER BY Claims_Count DESC, f.Meal_Type ASC;
""")

# 13) Total quantity donated by each provider
q("""
SELECT p.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity_Donated
FROM Providers p
JOIN Food_Listings f ON f.Provider_ID = p.Provider_ID
GROUP BY p.Provider_ID, p.Name
ORDER BY Total_Quantity_Donated DESC, p.Name ASC
LIMIT 10;
""")

# 14) Listings nearing expiry (next 2 days from today)
today = datetime.now().date()
q("""
SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Location
FROM Food_Listings
WHERE julianday(Expiry_Date) - julianday(date('now')) BETWEEN 0 AND 2
ORDER BY Expiry_Date ASC, Quantity DESC;
""")

# 15) Daily claim trend
q("""
SELECT DATE(Timestamp) AS Claim_Date, COUNT(*) AS Claims_Count
FROM Claims
GROUP BY DATE(Timestamp)
ORDER BY Claim_Date ASC;
""")

conn.close()
print("\nAll done ✅")
print("Cleaned CSVs + SQLite DB are in:", OUTPUT_DIR.resolve())
print("Rejected rows (FK issues, bad dates, duplicates, etc.) are in:", REJECTS_DIR.resolve())

# ============================================
# QUERY TESTER: Runs 15 business queries
# ============================================
import pandas as pd
import sqlite3

print("\n================ QUERY TESTER ================\n")

def run_query(conn, sql, title):
    print(f"\n--- {title} ---")
    df = pd.read_sql_query(sql, conn)
    print(df.head(15))   # show first 15 rows if long
    return df

with sqlite3.connect(DB_PATH) as conn:

    # 1. Providers per city
    run_query(conn, """
        SELECT City, COUNT(*) AS Total_Providers
        FROM Providers
        GROUP BY City
        ORDER BY Total_Providers DESC;
    """, "Q1: Providers per city")

    # 2. Receivers per city
    run_query(conn, """
        SELECT City, COUNT(*) AS Total_Receivers
        FROM Receivers
        GROUP BY City
        ORDER BY Total_Receivers DESC;
    """, "Q1: Receivers per city")

    # 3. Provider type contributions
    run_query(conn, """
        SELECT Provider_Type, COUNT(*) AS Total_Listings
        FROM Food_Listings
        GROUP BY Provider_Type
        ORDER BY Total_Listings DESC;
    """, "Q2: Provider type contributions")

    # 4. Contact info by city (example: New Carol)
    run_query(conn, """
        SELECT Name, Type, Address, Contact
        FROM Providers
        WHERE City = 'New Carol';
    """, "Q3: Provider contacts in New Carol")

    # 5. Top receivers by claims
    run_query(conn, """
        SELECT r.Receiver_ID, r.Name, COUNT(*) AS Total_Claims
        FROM Claims c
        JOIN Receivers r ON r.Receiver_ID = c.Receiver_ID
        GROUP BY r.Receiver_ID, r.Name
        ORDER BY Total_Claims DESC
        LIMIT 10;
    """, "Q4: Top receivers by claims")

    # 6. Total food quantity
    run_query(conn, """
        SELECT SUM(Quantity) AS Total_Quantity
        FROM Food_Listings;
    """, "Q5: Total food quantity available")

    # 7. City with most listings
    run_query(conn, """
        SELECT Location AS City, COUNT(*) AS Listings
        FROM Food_Listings
        GROUP BY Location
        ORDER BY Listings DESC
        LIMIT 1;
    """, "Q6: City with most listings")

    # 8. Common food types
    run_query(conn, """
        SELECT Food_Type, COUNT(*) AS Listings
        FROM Food_Listings
        GROUP BY Food_Type
        ORDER BY Listings DESC;
    """, "Q7: Common food types")

    # 9. Claims per food item
    run_query(conn, """
        SELECT f.Food_ID, f.Food_Name, COUNT(c.Claim_ID) AS Claims_Count
        FROM Food_Listings f
        LEFT JOIN Claims c ON f.Food_ID = c.Food_ID
        GROUP BY f.Food_ID, f.Food_Name
        ORDER BY Claims_Count DESC
        LIMIT 10;
    """, "Q8: Claims per food item")

    # 10. Provider with most successful claims
    run_query(conn, """
        SELECT p.Provider_ID, p.Name, COUNT(*) AS Completed_Claims
        FROM Claims c
        JOIN Food_Listings f ON f.Food_ID = c.Food_ID
        JOIN Providers p ON p.Provider_ID = f.Provider_ID
        WHERE c.Status = 'Completed'
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Completed_Claims DESC
        LIMIT 1;
    """, "Q9: Top provider by completed claims")

    # 11. Claim status distribution %
    run_query(conn, """
        WITH totals AS (
          SELECT COUNT(*) AS total FROM Claims
        )
        SELECT Status,
               COUNT(*) AS cnt,
               ROUND(100.0 * COUNT(*) / (SELECT total FROM totals), 2) AS pct
        FROM Claims
        GROUP BY Status
        ORDER BY cnt DESC;
    """, "Q10: Claims status distribution")

    # 12. Avg claimed quantity per receiver
    run_query(conn, """
        WITH claim_qty AS (
          SELECT c.Claim_ID, c.Receiver_ID, f.Quantity
          FROM Claims c
          JOIN Food_Listings f ON f.Food_ID = c.Food_ID
        )
        SELECT r.Receiver_ID, r.Name,
               ROUND(AVG(claim_qty.Quantity), 2) AS Avg_Claimed_Quantity
        FROM claim_qty
        JOIN Receivers r ON r.Receiver_ID = claim_qty.Receiver_ID
        GROUP BY r.Receiver_ID, r.Name
        ORDER BY Avg_Claimed_Quantity DESC
        LIMIT 10;
    """, "Q11: Avg quantity per receiver")

    # 13. Meal type most claimed
    run_query(conn, """
        SELECT f.Meal_Type, COUNT(*) AS Claims_Count
        FROM Claims c
        JOIN Food_Listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Meal_Type
        ORDER BY Claims_Count DESC;
    """, "Q12: Meal type most claimed")

    # 14. Total quantity donated per provider
    run_query(conn, """
        SELECT p.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity_Donated
        FROM Providers p
        JOIN Food_Listings f ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Total_Quantity_Donated DESC
        LIMIT 10;
    """, "Q13: Total donated per provider")

    # 15. Listings nearing expiry (next 2 days)
    run_query(conn, """
        SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Location
        FROM Food_Listings
        WHERE julianday(Expiry_Date) - julianday(date('now')) BETWEEN 0 AND 2
        ORDER BY Expiry_Date ASC;
    """, "Q14: Listings nearing expiry (next 2 days)")

    # 16. Daily claims trend
    run_query(conn, """
        SELECT DATE(Timestamp) AS Claim_Date, COUNT(*) AS Claims_Count
        FROM Claims
        GROUP BY DATE(Timestamp)
        ORDER BY Claim_Date ASC;
    """, "Q15: Daily claims trend")

print("\n================ END OF QUERY TESTER ================\n")
