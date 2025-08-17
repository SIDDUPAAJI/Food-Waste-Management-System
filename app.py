# app.py
# Local Food Wastage Management System – Full Streamlit App
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
import pandas as pd
import streamlit as st

DB_PATH = Path("cleaned_outputs/food_waste.db")  # adjust if needed

# -------------------------------
# Utilities
# -------------------------------
@st.cache_resource(show_spinner=False)
def get_conn():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at: {DB_PATH.resolve()}")
    conn = sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")
    return conn

def run_df(sql: str, params: tuple | dict = ()):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    return df

def run_exec(sql: str, params: tuple | dict = ()):
    conn = get_conn()
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute(sql, params)
        conn.commit()

def invalidate_caches():
    get_conn.clear()  # rebuild connection
    st.cache_data.clear()

@st.cache_data(show_spinner=False)
def list_values():
    # return config values for controls
    cities = sorted(set(run_df("SELECT City FROM Providers UNION SELECT Location AS City FROM Food_Listings;")["City"]))
    providers = run_df("SELECT Provider_ID, Name FROM Providers ORDER BY Name;")
    provider_options = dict(zip(providers["Name"], providers["Provider_ID"]))
    food_types = sorted(set(run_df("SELECT Food_Type FROM Food_Listings;")["Food_Type"]))
    meal_types = sorted(set(run_df("SELECT Meal_Type FROM Food_Listings;")["Meal_Type"]))
    receivers = run_df("SELECT Receiver_ID, Name FROM Receivers ORDER BY Name;")
    receiver_options = dict(zip(receivers["Name"], receivers["Receiver_ID"]))
    return {
        "cities": cities,
        "providers": provider_options,
        "food_types": food_types,
        "meal_types": meal_types,
        "receivers": receiver_options,
    }

# -------------------------------
# Layout
# -------------------------------
st.set_page_config(page_title="Local Food Wastage Management", layout="wide")
st.title("Local Food Wastage Management System")

if not DB_PATH.exists():
    st.error(f"Database not found at {DB_PATH.resolve()}. Run the cleaner/loader script first.")
    st.stop()

tabs = st.tabs(["Home", "Providers & Receivers", "Food Listings", "Claims", "Analytics", "CRUD"])

# -------------------------------
# HOME
# -------------------------------
with tabs[0]:
    st.subheader("Project Overview")
    st.markdown(
        """
        This application connects to the cleaned SQLite database and provides:
        - Dashboards with dynamic filters (city, provider, food type, meal type)
        - 15 curated analytical queries with visualization support
        - Full CRUD operations: add listings, update quantities, add/delete claims
        The goal is to **reduce food wastage** by improving visibility of providers, receivers, food availability, and claims activity in real-time.
        """
    )
    col1, col2, col3, col4 = st.columns(4)
    k1 = int(run_df("SELECT COUNT(*) AS n FROM Providers;")["n"].iloc[0])
    k2 = int(run_df("SELECT COUNT(*) AS n FROM Receivers;")["n"].iloc[0])
    k3 = int(run_df("SELECT COUNT(*) AS n FROM Food_Listings;")["n"].iloc[0])
    k4 = int(run_df("SELECT COUNT(*) AS n FROM Claims;")["n"].iloc[0])
    col1.metric("Providers", k1)
    col2.metric("Receivers", k2)
    col3.metric("Food Listings", k3)
    col4.metric("Claims", k4)

# -------------------------------
# PROVIDERS & RECEIVERS
# -------------------------------
with tabs[1]:
    st.subheader("Providers & Receivers")
    vals = list_values()
    city = st.selectbox("Filter by City (optional)", ["All"] + vals["cities"])
    # Q1 Providers per city
    st.markdown("**Q1: Providers per city**")
    df = run_df("""
        SELECT City, COUNT(*) AS Total_Providers
        FROM Providers
        GROUP BY City
        ORDER BY Total_Providers DESC, City ASC;
    """)
    if city != "All":
        df = df[df["City"] == city]
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("City"))
    except Exception:
        pass
    # Q1b Receivers per city
    st.markdown("**Q1b: Receivers per city**")
    df = run_df("""
        SELECT City, COUNT(*) AS Total_Receivers
        FROM Receivers
        GROUP BY City
        ORDER BY Total_Receivers DESC, City ASC;
    """)
    if city != "All":
        df = df[df["City"] == city]
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("City"))
    except Exception:
        pass
    # Q2 Provider type contributions
    st.markdown("**Q2: Which provider types contribute the most listings?**")
    df = run_df("""
       SELECT Name, Type, Address, Contact
       FROM Providers
       WHERE City = :city
       ORDER BY Name;
    """, {"city": city} if city != "All" else {})
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("Type"))
    except Exception:
        pass
    # Q3 Provider contacts
    st.markdown("**Q3: Provider contacts in selected city**")
    city_contact = st.selectbox("Choose city", vals["cities"])
    df = run_df("""
        SELECT Name, Provider_Type, Address, Contact
        FROM Providers
        WHERE City = :city
        ORDER BY Name;
    """, {"city": city_contact})
    st.dataframe(df, use_container_width=True)
    # Q4 Top receivers by claims
    st.markdown("**Q4: Top receivers by total claims**")
    df = run_df("""
        SELECT r.Receiver_ID, r.Name, COUNT(*) AS Total_Claims
        FROM Claims c
        JOIN Receivers r ON r.Receiver_ID = c.Receiver_ID
        GROUP BY r.Receiver_ID, r.Name
        ORDER BY Total_Claims DESC
        LIMIT 15;
    """)
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("Name")["Total_Claims"])
    except Exception:
        pass

# -------------------------------
# FOOD LISTINGS
# -------------------------------
with tabs[2]:
    st.subheader("Food Listings")
    vals = list_values()
    c1, c2, c3 = st.columns(3)
    filt_city = c1.selectbox("City", ["All"] + vals["cities"], index=0)
    filt_food_type = c2.selectbox("Food Type", ["All"] + vals["food_types"], index=0)
    filt_meal_type = c3.selectbox("Meal Type", ["All"] + vals["meal_types"], index=0)
    base_sql = """
        SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID,
        Provider_Type, Location AS City, Food_Type, Meal_Type
        FROM Food_Listings
        WHERE 1=1
    """
    params = {}
    if filt_city != "All":
        params["city"] = filt_city
        base_sql += " AND Location = :city"
    if filt_food_type != "All":
        params["ft"] = filt_food_type
        base_sql += " AND Food_Type = :ft"
    if filt_meal_type != "All":
        params["mt"] = filt_meal_type
        base_sql += " AND Meal_Type = :mt"
    df = run_df(base_sql + " ORDER BY Expiry_Date ASC, Quantity DESC;", params)
    st.dataframe(df, use_container_width=True, height=420)
    # Q5 Total quantity available
    st.markdown("**Q5: Total quantity available**")
    df_q5 = run_df("SELECT SUM(Quantity) AS Total_Quantity FROM Food_Listings;")
    st.dataframe(df_q5, use_container_width=True)
    # Q6 city with most listings
    st.markdown("**Q6: City with most listings**")
    df_q6 = run_df("""
        SELECT Location AS City, COUNT(*) AS Listings
        FROM Food_Listings
        GROUP BY Location
        ORDER BY Listings DESC
        LIMIT 10;
    """)
    st.dataframe(df_q6, use_container_width=True)
    try:
        st.bar_chart(df_q6.set_index("City"))
    except Exception:
        pass
    # Q7 most common food types
    st.markdown("**Q7: Most common food types**")
    df_q7 = run_df("""
        SELECT Food_Type, COUNT(*) AS Listings
        FROM Food_Listings
        GROUP BY Food_Type
        ORDER BY Listings DESC;
    """)
    st.dataframe(df_q7, use_container_width=True)
    try:
        st.bar_chart(df_q7.set_index("Food_Type"))
    except Exception:
        pass

# -------------------------------
# CLAIMS
# -------------------------------
with tabs[3]:
    st.subheader("Claims")
    # Q8 claims per food item
    st.markdown("**Q8: Claims per food item**")
    df = run_df("""
        SELECT f.Food_ID, f.Food_Name, COUNT(c.Claim_ID) AS Claims_Count
        FROM Food_Listings f
        LEFT JOIN Claims c ON c.Food_ID = f.Food_ID
        GROUP BY f.Food_ID, f.Food_Name
        ORDER BY Claims_Count DESC
        LIMIT 20;
    """)
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("Food_Name")["Claims_Count"])
    except Exception:
        pass
    # Q9 provider by completed claims
    st.markdown("**Q9: Provider with most successful (Completed) claims**")
    df = run_df("""
        SELECT p.Provider_ID, p.Name, COUNT(*) AS Completed_Claims
        FROM Claims c
        JOIN Food_Listings f ON f.Food_ID = c.Food_ID
        JOIN Providers p ON p.Provider_ID = f.Provider_ID
        WHERE c.Status = 'Completed'
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Completed_Claims DESC
        LIMIT 10;
    """)
    st.dataframe(df, use_container_width=True)
    # Q10 claim status distribution
    st.markdown("**Q10: Claim status distribution (%)**")
    df = run_df("""
        WITH totals AS (SELECT COUNT(*) AS total FROM Claims)
        SELECT Status, COUNT(*) AS cnt,
               ROUND(100.0 * COUNT(*) / (SELECT total FROM totals), 2) AS pct
        FROM Claims
        GROUP BY Status
        ORDER BY cnt DESC;
    """)
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("Status")["cnt"])
    except Exception:
        pass

# -------------------------------
# ANALYTICS
# -------------------------------
with tabs[4]:
    st.subheader("Analytics")
    # Q11 average quantity per receiver
    st.markdown("**Q11: Average claimed quantity per receiver**")
    df = run_df("""
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
        LIMIT 20;
    """)
    st.dataframe(df, use_container_width=True)
    # Q12 meal type most claimed
    st.markdown("**Q12: Meal type most claimed**")
    df = run_df("""
        SELECT f.Meal_Type, COUNT(*) AS Claims_Count
        FROM Claims c
        JOIN Food_Listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Meal_Type
        ORDER BY Claims_Count DESC;
    """)
    st.dataframe(df, use_container_width=True)
    try:
        st.bar_chart(df.set_index("Meal_Type")["Claims_Count"])
    except Exception:
        pass
    # Q13 total quantity donated by provider
    st.markdown("**Q13: Total quantity donated by provider**")
    df = run_df("""
        SELECT p.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity_Donated
        FROM Providers p
        JOIN Food_Listings f ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Total_Quantity_Donated DESC
        LIMIT 20;
    """)
    st.dataframe(df, use_container_width=True)
    # Q14 listings nearing expiry
    st.markdown("**Q14: Listings nearing expiry (next 2 days)**")
    df = run_df("""
        SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Location
        FROM Food_Listings
        WHERE julianday(Expiry_Date) - julianday(date('now')) BETWEEN 0 AND 2
        ORDER BY Expiry_Date ASC;
    """)
    st.dataframe(df, use_container_width=True)
    # Q15 daily claims trend
    st.markdown("**Q15: Daily claims trend**")
    df = run_df("""
        SELECT DATE(Timestamp) AS Claim_Date, COUNT(*) AS Claims_Count
        FROM Claims
        GROUP BY DATE(Timestamp)
        ORDER BY Claim_Date ASC;
    """)
    st.dataframe(df, use_container_width=True)
    try:
        st.line_chart(df.set_index("Claim_Date"))
    except Exception:
        pass

# -------------------------------
# CRUD
# -------------------------------
with tabs[5]:
    st.subheader("CRUD Operations")
    vals = list_values()
    crud_tabs = st.tabs(["Add Listing", "Update Quantity", "Add Claim", "Delete Claim"])

    # -------------------------------
    # Add Listing
    # -------------------------------
    with crud_tabs[0]:
        st.markdown("**Create a new Food Listing**")
        food_name = st.text_input("Food Name")
        quantity = st.number_input("Quantity", min_value=1, step=1)
        expiry_date = st.date_input("Expiry Date", value=date.today(), key="expiry_date_add")
        food_type = st.selectbox("Food Type", vals["food_types"])
        meal_type = st.selectbox("Meal Type", vals["meal_types"])
        provider_name = st.selectbox("Provider", list(vals["providers"].keys()))
        provider_id = vals["providers"][provider_name]
        location = st.text_input("Location")
        if st.button("Add Listing"):
            try:
                df_next = run_df("SELECT COALESCE(MAX(Food_ID), 0)+1 AS next_id FROM Food_Listings;")
                food_id = int(df_next["next_id"].iloc[0])
                run_exec("""
                    INSERT INTO Food_Listings (Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID,
                        Provider_Type, Location, Food_Type, Meal_Type)
                    VALUES (?, ?, ?, ?, ?,
                        (SELECT Type FROM Providers WHERE Provider_ID = ?),
                        ?, ?, ?);
                    """, (food_id, food_name, quantity, expiry_date.isoformat(), provider_id,
                          provider_id, location, food_type, meal_type))
                invalidate_caches()
                st.success(f"Food Listing {food_id} added successfully.")
            except Exception as e:
                st.error(f"Failed to add listing: {e}")

    # -------------------------------
    # Update Quantity
    # -------------------------------
    with crud_tabs[1]:
        st.markdown("**Update Quantity for a Food Listing**")
        df = run_df("SELECT Food_ID, Food_Name, Quantity FROM Food_Listings ORDER BY Food_ID LIMIT 100;")
        if df.empty:
            st.info("No listings available.")
        else:
            row = st.selectbox("Select Listing", df.apply(lambda r: f"{r['Food_ID']} – {r['Food_Name']} (Qty {r['Quantity']})", axis=1))
            food_id = int(row.split("–")[0].strip())
            new_qty = st.number_input("New Quantity", min_value=0, step=1)
            if st.button("Update Quantity"):
                try:
                    run_exec("UPDATE Food_Listings SET Quantity = ? WHERE Food_ID = ?;", (new_qty, food_id))
                    invalidate_caches()
                    st.success(f"Quantity updated for Food_ID {food_id}.")
                except Exception as e:
                    st.error(f"Failed to update quantity: {e}")

    # -------------------------------
    # Add Claim
    # -------------------------------
    with crud_tabs[2]:
        st.markdown("**Create a Claim**")
        df_food = run_df("SELECT Food_ID, Food_Name FROM Food_Listings ORDER BY Expiry_Date ASC LIMIT 100;")
        df_recv = run_df("SELECT Receiver_ID, Name FROM Receivers ORDER BY Name LIMIT 100;")
        if df_food.empty or df_recv.empty:
            st.info("Need both listings and receivers to create a claim.")
        else:
            food_pick = st.selectbox("Food", df_food.apply(lambda r: f"{r['Food_ID']} – {r['Food_Name']}", axis=1))
            receiver_pick = st.selectbox("Receiver", df_recv.apply(lambda r: f"{r['Receiver_ID']} – {r['Name']}", axis=1))
            status = st.selectbox("Status", ["Pending", "Completed", "Cancelled"], index=0)
            col1, col2 = st.columns(2)
            claim_date = col1.date_input("Claim Date", value=date.today(), key="claim_date_add")
            claim_time = col2.time_input("Claim Time", value=datetime.now().time(), key="claim_time_add")
            ts = datetime.combine(claim_date, claim_time)
            if st.button("Add Claim"):
                try:
                    df_next = run_df("SELECT COALESCE(MAX(Claim_ID), 0)+1 AS next_id FROM Claims;")
                    claim_id = int(df_next["next_id"].iloc[0])
                    food_id = int(food_pick.split("–")[0].strip())
                    receiver_id = int(receiver_pick.split("–")[0].strip())
                    run_exec("""
                        INSERT INTO Claims (Claim_ID, Food_ID, Receiver_ID, Status, Timestamp)
                        VALUES (?, ?, ?, ?, ?);
                    """, (claim_id, food_id, receiver_id, status, ts.isoformat(sep=" ")))
                    invalidate_caches()
                    st.success(f"Claim {claim_id} created.")
                except Exception as e:
                    st.error(f"Failed to add claim: {e}")

    # -------------------------------
    # Delete Claim
    # -------------------------------
    with crud_tabs[3]:
        st.markdown("**Delete a Claim**")
        df = run_df("""
            SELECT Claim_ID, Food_ID, Receiver_ID, Status, Timestamp
            FROM Claims ORDER BY Timestamp DESC LIMIT 50;
        """)
        if df.empty:
            st.info("No claims available.")
        else:
            row = st.selectbox(
                "Select Claim",
                df.apply(lambda r: f"Claim {r['Claim_ID']} – Food {r['Food_ID']} – Receiver {r['Receiver_ID']} – {r['Status']} – {r['Timestamp']}", axis=1)
            )
            claim_id = int(row.split("–")[0].replace("Claim", "").strip())
            if st.button("Delete Claim"):
                try:
                    run_exec("DELETE FROM Claims WHERE Claim_ID = ?;", (claim_id,))
                    invalidate_caches()
                    st.success(f"Claim {claim_id} deleted.")
                except Exception as e:
                    st.error(f"Failed to delete claim: {e}")
