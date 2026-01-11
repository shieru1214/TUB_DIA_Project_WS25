-- Task 2.1
SELECT
  station_name,lat,lon,eva AS identifier
FROM dim_station
WHERE lower(trim(station_name)) = lower(trim(:station_name));

-- Task 2.2 (PostgreSQL fast KNN; uses GiST index on point(lon, lat))
SELECT station_name
FROM dim_station
ORDER BY point(lon, lat) <-> point(:lon, :lat)
LIMIT 1;

