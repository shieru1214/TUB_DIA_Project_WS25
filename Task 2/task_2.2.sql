-- Task 2.2
-- Input: a latitude/longitude (here I use an example location near Berlin city center).
-- Output: the name of the closest station.
-- Idea: I use a simple squared-distance in (lat, lon) space.

WITH input(lat, lon) AS (
  VALUES (52.5200, 13.4050)
)
SELECT s.station_name
FROM dim_station s
CROSS JOIN input i
WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
ORDER BY (s.lat - i.lat)^2 + (s.lon - i.lon)^2
LIMIT 1;