### Task 2 – Data Analysis (PostgreSQL)

This folder contains the four SQL queries for Task 2. I ran them in PostgreSQL using Navicat (query editor). No extensions are required.

#### Files
- task_2.1.sql  (station name -> coordinates + EVA)
- task_2.2.sql  (lat/lon -> closest station)
- task_2.3.sql  (date_hour snapshot -> total cancelled trains)
- task_2.4.sql  (station name -> average delay in minutes)

#### Requirements
- PostgreSQL (database already created, e.g. "DIA")
- The star schema + ingested data from Task 1 (dim_station, dim_time, dim_train, fact_train_movement)
- Any SQL client is fine (I used Navicat)

#### How to run
1) Open Navicat and connect to your PostgreSQL server.
2) Select the database (e.g. DIA) and schema.
3) Open ONE file (e.g. task_2.2.sql) in the query editor.
4) Run the whole script as a single block.

Tip: I keep each task in its own file so I can run them independently.

#### How to change the input
Each query uses simple literal inputs. To test another case, just replace:
- station keyword (e.g. 'alexanderplatz')
- coordinates (lat/lon)
- snapshot folder timestamp (YYMMDDHHMM)

Note: In my schema, snapshot_time_key is stored as YYYYMMDDHHMM,
so I convert the folder timestamp by prefixing '20' (e.g. '2509021800' -> '202509021800').