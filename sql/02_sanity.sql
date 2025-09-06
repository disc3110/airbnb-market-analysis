-- Row counts
SELECT 'listings' AS t, COUNT(*) FROM analytics.listings
UNION ALL SELECT 'calendar', COUNT(*) FROM analytics.calendar
UNION ALL SELECT 'reviews', COUNT(*) FROM analytics.reviews;

-- Top 10 most expensive neighborhoods (median price)
SELECT neighbourhood,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
       COUNT(*) AS n_listings
FROM analytics.listings
WHERE price IS NOT NULL
GROUP BY neighbourhood
HAVING COUNT(*) >= 30
ORDER BY median_price DESC
LIMIT 10;

-- Availability rate 
SELECT l.neighbourhood,
       AVG(CASE WHEN c.available THEN 1 ELSE 0 END)::numeric(5,2) AS availability_rate
FROM analytics.calendar c
JOIN analytics.listings l USING (listing_id)
GROUP BY l.neighbourhood
HAVING COUNT(*) >= 500
ORDER BY availability_rate ASC
LIMIT 10;