--
CREATE MATERIALIZED VIEW trip_time_statistics AS
    WITH trip_stats AS (
        SELECT
            PULocationID,
            DOLocationID,
            AVG(trip_time) AS avg_trip_time,
            MIN(trip_time) AS min_trip_time,
            MAX(trip_time) AS max_trip_time
        FROM (
            SELECT
                PULocationID,
                DOLocationID,
                tpep_dropoff_datetime - tpep_pickup_datetime AS trip_time
            FROM
                trip_data
        ) AS trips
        GROUP BY
            PULocationID,
            DOLocationID
    )
SELECT
    t1.Zone AS pickup_zone,
    t2.Zone AS dropoff_zone,
    avg_trip_time,
    min_trip_time,
    max_trip_time
FROM
    trip_stats
JOIN
    taxi_zone AS t1 ON trip_stats.PULocationID = t1.location_id
JOIN
    taxi_zone AS t2 ON trip_stats.DOLocationID = t2.location_id;

-- 
SELECT *
FROM trip_time_statistics
ORDER BY avg_trip_time DESC
LIMIT 1;


--
CREATE MATERIALIZED VIEW trip_time_statistics_with_count AS
    WITH trip_stats AS (
        SELECT
            PULocationID,
            DOLocationId,
            COUNT(*) AS trip_count,
            AVG(trip_time) AS avg_trip_time,
            MIN(trip_time) AS min_trip_time,
            MAX(trip_time) AS max_trip_time,
            ROW_NUMBER() OVER (ORDER BY AVG(trip_time) DESC) AS rank
        FROM (
            SELECT
                PULocationID,
                DOLocationID,
                EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS trip_time
            FROM
                trip_data
        ) AS trips
        GROUP BY
            PULocationID,
            DOLocationID
    )
SELECT
    t1.Zone AS pickup_zone,
    t2.Zone AS dropoff_zone,
    trip_count,
    avg_trip_time,
    min_trip_time,
    max_trip_time
FROM
    trip_stats
JOIN
    taxi_zone AS t1 ON trip_stats.PULocationID = t1.location_id
JOIN
    taxi_zone AS t2 ON trip_stats.DOLocationID = t2.location_id;

--
SELECT *
FROM trip_time_statistics_with_count
ORDER BY avg_trip_time DESC
LIMIT 1;

--
WITH latest_pickup AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
    FROM trip_data
),
top_zones AS (
    SELECT
        PULocationID,
        COUNT(*) AS pickup_count
    FROM
        trip_data
    WHERE
        tpep_pickup_datetime >= (SELECT latest_pickup_time - INTERVAL '17 hours' FROM latest_pickup)
        AND tpep_pickup_datetime <= (SELECT latest_pickup_time FROM latest_pickup)
    GROUP BY
        PULocationID
    ORDER BY
        pickup_count DESC
    LIMIT 3
)
SELECT *
FROM
    top_zones
JOIN
    taxi_zone AS tz ON top_zones.PULocationID = tz.location_id;



--