-- Namespaces
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging tables: 
DROP TABLE IF EXISTS staging.listings CASCADE;
CREATE TABLE staging.listings (
  id BIGINT PRIMARY KEY,
  host_id BIGINT,
  neighbourhood_cleansed TEXT,
  neighbourhood_group_cleansed TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  room_type TEXT,
  accommodates INT,
  bedrooms REAL,
  beds REAL,
  price TEXT,
  minimum_nights INT,
  number_of_reviews INT,
  review_scores_rating REAL
);

DROP TABLE IF EXISTS staging.reviews CASCADE;
CREATE TABLE staging.reviews (
  listing_id BIGINT,
  id BIGINT PRIMARY KEY,
  date DATE,
  reviewer_id BIGINT,
  reviewer_name TEXT,
  comments TEXT
);

DROP TABLE IF EXISTS staging.calendar CASCADE;
CREATE TABLE staging.calendar (
  listing_id BIGINT,
  date DATE,
  available TEXT,          -- 't' / 'f'
  price TEXT,
  adjusted_price TEXT,
  minimum_nights INT,
  maximum_nights INT
);

-- Cleaned analytics tables (typed & normalized)
DROP TABLE IF EXISTS analytics.listings CASCADE;
CREATE TABLE analytics.listings (
  listing_id BIGINT PRIMARY KEY,
  host_id BIGINT,
  neighbourhood TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  room_type TEXT,
  accommodates INT,
  bedrooms REAL,
  beds REAL,
  price NUMERIC(10,2),
  minimum_nights INT,
  number_of_reviews INT,
  review_scores_rating REAL
);

DROP TABLE IF EXISTS analytics.calendar CASCADE;
CREATE TABLE analytics.calendar (
  listing_id BIGINT,
  date DATE,
  available BOOLEAN,
  price NUMERIC(10,2),
  adjusted_price NUMERIC(10,2),
  minimum_nights INT,
  maximum_nights INT
);

DROP TABLE IF EXISTS analytics.reviews CASCADE;
CREATE TABLE analytics.reviews (
  review_id BIGINT PRIMARY KEY,
  listing_id BIGINT,
  date DATE,
  reviewer_id BIGINT
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_a_listings_neigh ON analytics.listings (neighbourhood);
CREATE INDEX IF NOT EXISTS idx_a_calendar_listing_date ON analytics.calendar (listing_id, date);
CREATE INDEX IF NOT EXISTS idx_a_reviews_listing ON analytics.reviews (listing_id);