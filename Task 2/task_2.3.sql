-- Task 2.3
-- Input: snapshot folder name in YYMMDDHHMM (example: 2509021800).
-- Output: total number of cancelled trains over all stations at that snapshot.
-- Idea: I count DISTINCT (station_key, stop_id) to avoid double counting the same stop
-- and if both arrival (A) and departure (D) are flagged as cancelled.

WITH params AS (
  SELECT ('20' || '2509021800')::bigint AS snapshot_time_key
)
SELECT COUNT(DISTINCT (f.station_key, f.stop_id)) AS cancelled_trains
FROM fact_train_movement f
JOIN params p ON f.snapshot_time_key = p.snapshot_time_key
WHERE f.is_cancelled = TRUE;