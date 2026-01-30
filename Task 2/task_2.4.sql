-- Task 2.4
-- Input: a station name (I use ILIKE to be robust to prefixes like "Berlin ...").
-- Output: average delay (minutes) at that station.
-- Idea: I use departures only (event_type='D') to avoid counting the same train twice. I also exclude cancelled events and NULL delays.

SELECT
  s.station_name,
  ROUND(AVG(f.delay_minutes)::numeric, 2) AS avg_delay_minutes,
  COUNT(*) AS num_events_used
FROM dim_station s
JOIN fact_train_movement f
  ON f.station_key = s.station_key
WHERE s.station_name ILIKE '%' || 'alexanderplatz' || '%'
  AND f.event_type = 'D'
  AND f.is_cancelled = FALSE
  AND f.delay_minutes IS NOT NULL
GROUP BY s.station_name
ORDER BY num_events_used DESC;