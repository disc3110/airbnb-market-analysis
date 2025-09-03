import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

ENGINE = create_engine(
    f"postgresql+psycopg://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
    f"@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
)

RAW = "data/raw"
CHUNK = 100_000  # tune if memory is low

def load_listings():
    path = f"{RAW}/listings.csv.gz"
    usecols = [
        "id","host_id","neighbourhood_cleansed","neighbourhood_group_cleansed",
        "latitude","longitude","room_type","accommodates","bedrooms","beds",
        "price","minimum_nights","number_of_reviews","review_scores_rating"
    ]
    for i, df in enumerate(pd.read_csv(path, usecols=usecols, low_memory=False, compression="infer", chunksize=CHUNK)):
        df.to_sql("listings", ENGINE, schema="staging", if_exists="append", index=False, method="multi")
        print(f"listings chunk {i} -> {len(df)} rows")

def load_reviews():
    path = f"{RAW}/reviews.csv.gz"
    usecols = ["listing_id","id","date","reviewer_id","reviewer_name","comments"]
    for i, df in enumerate(pd.read_csv(path, usecols=usecols, parse_dates=["date"], low_memory=False, compression="infer", chunksize=CHUNK)):
        df.to_sql("reviews", ENGINE, schema="staging", if_exists="append", index=False, method="multi")
        print(f"reviews chunk {i} -> {len(df)} rows")

def load_calendar():
    path = f"{RAW}/calendar.csv.gz"
    usecols = ["listing_id","date","available","price","adjusted_price","minimum_nights","maximum_nights"]
    for i, df in enumerate(pd.read_csv(path, usecols=usecols, parse_dates=["date"], low_memory=False, compression="infer", chunksize=CHUNK)):
        df.to_sql("calendar", ENGINE, schema="staging", if_exists="append", index=False, method="multi")
        print(f"calendar chunk {i} -> {len(df)} rows")

def transform():
    with ENGINE.begin() as conn:
        # listings: strip $ and commas from price
        conn.execute(text("""
            INSERT INTO analytics.listings
            SELECT
              id AS listing_id,
              host_id,
              COALESCE(neighbourhood_cleansed, neighbourhood_group_cleansed) AS neighbourhood,
              latitude, longitude, room_type, accommodates, bedrooms, beds,
              NULLIF(REPLACE(REPLACE(price,'$',''),',',''),'')::numeric(10,2) AS price,
              minimum_nights, number_of_reviews, review_scores_rating
            FROM staging.listings
            ON CONFLICT (listing_id) DO NOTHING;
        """))

        # calendar: convert available + prices
        conn.execute(text("""
            INSERT INTO analytics.calendar
            SELECT
              listing_id,
              date::date,
              CASE WHEN LOWER(available) IN ('t','true','yes') THEN TRUE ELSE FALSE END AS available,
              NULLIF(REPLACE(REPLACE(price,'$',''),',',''),'')::numeric(10,2) AS price,
              NULLIF(REPLACE(REPLACE(adjusted_price,'$',''),',',''),'')::numeric(10,2) AS adjusted_price,
              minimum_nights, maximum_nights
            FROM staging.calendar;
        """))

        # reviews: keep lean table
        conn.execute(text("""
            INSERT INTO analytics.reviews (review_id, listing_id, date, reviewer_id)
            SELECT id, listing_id, date::date, reviewer_id
            FROM staging.reviews
            ON CONFLICT (review_id) DO NOTHING;
        """))

if __name__ == "__main__":
    print("Loading listings...")
    load_listings()
    print("Loading calendar...")
    load_calendar()
    print("Loading reviews...")
    load_reviews()
    print("Transforming to analytics schema...")
    transform()
    print("Done ✅")