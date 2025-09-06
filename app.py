import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = (
    f"postgresql+psycopg://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
    f"@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
)
engine = create_engine(DB_URL)

st.set_page_config(page_title="Airbnb Vancouver • Dashboard", layout="wide")

# -------- Helpers --------
@st.cache_data(ttl=300)
def load_neighbourhoods():
    q = """
    SELECT DISTINCT neighbourhood
    FROM analytics.listings
    WHERE neighbourhood IS NOT NULL
    ORDER BY neighbourhood;
    """
    return pd.read_sql(q, engine)["neighbourhood"].dropna().tolist()

@st.cache_data(ttl=300)
def load_room_types():
    q = "SELECT DISTINCT room_type FROM analytics.listings WHERE room_type IS NOT NULL ORDER BY 1;"
    return pd.read_sql(q, engine)["room_type"].dropna().tolist()

@st.cache_data(ttl=300)
def load_price_range():
    q = "SELECT MIN(price)::float AS min_p, MAX(price)::float AS max_p FROM analytics.listings WHERE price IS NOT NULL;"
    r = pd.read_sql(q, engine)
    if r.empty or pd.isna(r.loc[0, "min_p"]) or pd.isna(r.loc[0, "max_p"]):
        return 0.0, 1000.0
    return float(r.loc[0, "min_p"]), float(r.loc[0, "max_p"])

@st.cache_data(ttl=300)
def get_kpis(neigh, rooms, pmin, pmax):
    q = text("""
        SELECT
          COUNT(*)                                                AS n_listings,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)      AS median_price,
          AVG(review_scores_rating)                               AS avg_rating
        FROM analytics.listings
        WHERE price BETWEEN :pmin AND :pmax
          AND (:all_neigh OR neighbourhood = ANY(:neigh))
          AND (:all_room  OR room_type    = ANY(:rooms))
    """)
    with engine.begin() as c:
        row = c.execute(q, {
            "pmin": pmin, "pmax": pmax,
            "neigh": neigh, "rooms": rooms,
            "all_neigh": len(neigh) == 0,
            "all_room": len(rooms) == 0
        }).fetchone()
    if not row:
        return {"n_listings": 0, "median_price": None, "avg_rating": None}
    return dict(row._mapping)

@st.cache_data(ttl=300)
def price_by_neighbourhood(neigh, rooms, pmin, pmax, limit=20):
    q = text("""
        SELECT neighbourhood,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
               COUNT(*) AS n
        FROM analytics.listings
        WHERE price BETWEEN :pmin AND :pmax
          AND (:all_neigh OR neighbourhood = ANY(:neigh))
          AND (:all_room  OR room_type    = ANY(:rooms))
        GROUP BY neighbourhood
        HAVING COUNT(*) >= 20
        ORDER BY median_price DESC
        LIMIT :limit;
    """)
    with engine.begin() as c:
        df = pd.DataFrame(c.execute(q, {
            "pmin": pmin, "pmax": pmax,
            "neigh": neigh, "rooms": rooms,
            "all_neigh": len(neigh) == 0,
            "all_room": len(rooms) == 0,
            "limit": limit
        }))
        df.columns = ["neighbourhood","median_price","n"]
    return df

@st.cache_data(ttl=300)
def roomtype_prices(neigh, rooms, pmin, pmax):
    q = text("""
        SELECT room_type,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
               COUNT(*) AS n
        FROM analytics.listings
        WHERE price BETWEEN :pmin AND :pmax
          AND (:all_neigh OR neighbourhood = ANY(:neigh))
          AND (:all_room  OR room_type    = ANY(:rooms))
        GROUP BY room_type
        ORDER BY median_price DESC;
    """)
    with engine.begin() as c:
        df = pd.DataFrame(c.execute(q, {
            "pmin": pmin, "pmax": pmax,
            "neigh": neigh, "rooms": rooms,
            "all_neigh": len(neigh) == 0,
            "all_room": len(rooms) == 0
        }))
        df.columns = ["room_type","median_price","n"]
    return df

@st.cache_data(ttl=300)
def availability_series(neigh, rooms, year, pmin, pmax):
    sql = """
        SELECT c.date,
               AVG(CASE WHEN c.available THEN 1 ELSE 0 END)::float AS availability_rate
        FROM analytics.calendar c
        JOIN analytics.listings l USING (listing_id)
        WHERE c.date >= make_date(%(yr)s,1,1) AND c.date < make_date(%(yrp1)s,1,1)
          AND l.price BETWEEN %(pmin)s AND %(pmax)s
          AND (%(all_neigh)s OR l.neighbourhood = ANY(%(neigh)s))
          AND (%(all_room)s  OR l.room_type     = ANY(%(rooms)s))
        GROUP BY c.date
        ORDER BY c.date;
    """
    params = dict(
        yr=year, yrp1=year+1, pmin=pmin, pmax=pmax,
        neigh=neigh, rooms=rooms,
        all_neigh=len(neigh) == 0, all_room=len(rooms) == 0
    )
    # Use pandas.read_sql so we always get columns; works even when 0 rows
    df = pd.read_sql(sql, engine, params=params)
    # Ensure the expected columns exist even if empty
    if df.empty:
        df = pd.DataFrame(columns=["date", "availability_rate"])
    return df

# -------- UI --------
st.title("🏙️ Airbnb Market — Vancouver")
st.caption("PostgreSQL + Python (Streamlit) • Interactive exploration")

with st.sidebar:
    st.header("Filters")
    all_neighs = load_neighbourhoods()
    all_rooms = load_room_types()
    min_p, max_p = load_price_range()

    pick_neighs = st.multiselect("Neighbourhood(s)", all_neighs, [])
    pick_rooms  = st.multiselect("Room type(s)", all_rooms, [])
    price_rng   = st.slider("Price range ($)", min_value=int(min_p), max_value=int(max_p), value=(int(min_p), int(max_p)))
    year        = st.number_input("Calendar Year", value=2025, step=1, min_value=2015, max_value=2030)

# KPIs
kpis = get_kpis(pick_neighs, pick_rooms, price_rng[0], price_rng[1])
col1, col2, col3 = st.columns(3)
col1.metric("Listings", f"{int(kpis['n_listings'] or 0):,}")
col2.metric("Median price", f"${(kpis['median_price'] or 0):.0f}")
col3.metric("Avg rating", f"{(kpis['avg_rating'] or 0):.1f}")

# Charts
c1, c2 = st.columns([2,1])

df_neigh = price_by_neighbourhood(pick_neighs, pick_rooms, price_rng[0], price_rng[1])
fig1 = px.bar(df_neigh, x="median_price", y="neighbourhood", orientation="h",
              title="Top Neighbourhoods by Median Price", labels={"median_price":"Median price ($)","neighbourhood":""})
c1.plotly_chart(fig1, use_container_width=True)

df_room = roomtype_prices(pick_neighs, pick_rooms, price_rng[0], price_rng[1])
fig2 = px.bar(df_room, x="room_type", y="median_price", title="Median Price by Room Type",
              labels={"median_price":"Median price ($)","room_type":"Room type"})
c2.plotly_chart(fig2, use_container_width=True)

# Availability chart
df_avail = availability_series(pick_neighs, pick_rooms, year, price_rng[0], price_rng[1])
if df_avail.empty:
    st.info("No calendar data matches the current filters/year.")
else:
    fig3 = px.line(df_avail, x="date", y="availability_rate", title=f"Availability Rate — {year}",
                   labels={"availability_rate": "Availability rate"})
    st.plotly_chart(fig3, use_container_width=True)

st.caption("Data source: Inside Airbnb (Vancouver). Tables: analytics.listings / analytics.calendar / analytics.reviews.")