-- Task 2.1
SELECT station_name, lat, lon, eva AS identifier
FROM dim_station
WHERE station_name ILIKE '%' || 'alexanderplatz' || '%'
ORDER BY station_name;


-- Task 2.2
WITH input(lat, lon) AS (
  VALUES (52.5200, 13.4050)  -- example: Berlin city center
)
SELECT
  s.station_name
FROM dim_station s
CROSS JOIN input i
ORDER BY
  (s.lat - i.lat)^2 + (s.lon - i.lon)^2
LIMIT 1;


