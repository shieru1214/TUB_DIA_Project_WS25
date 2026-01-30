-- Task 2.1
-- Input: a station name (here I use an example alexanderplatz).
-- Output: its coordinates and identifier (EVA).

SELECT station_name, lat, lon, eva AS identifier
FROM dim_station
WHERE station_name ILIKE '%' || 'alexanderplatz' || '%'
ORDER BY station_name
LIMIT 1;

