### Task 1 — Create schema + Ingest data

1) Create tables (run once)

   Run the schema SQL on your database (example DB name: DIA).

   ```
   psql -d DIA -f task_1.1_star_schema.sql
   ```

   

2) Ingest planned timetables (hourly snapshots)

   This loads planned movements from the `timetables` weekly folder.

   ```
   python task_1.2_ingest_timetables.py \
     --week-dir "/path/to/dataset/250902_250909_timetable" \
     --station-json "/path/to/dataset/station_data.json" \
     --pg-host "localhost" --pg-port 5432 \
     --pg-db "DIA" --pg-user "YOUR_USER"
   
   ```

   

3) Ingest timetable changes (15-min snapshots)

   This updates the same fact grain with delay/cancellation info.

   ```
   python task_1.2_ingest_timetable_changes.py \
     --week-dir "/path/to/dataset/250902_250909_timetable_changes" \
     --pg-host "localhost" --pg-port 5432 \
     --pg-db "DIA" --pg-user "YOUR_USER"
   
   ```

   