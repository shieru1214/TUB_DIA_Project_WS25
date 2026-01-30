-- =========================================================
-- DIA DBahn Berlin - Star Schema (Task 1.1)
-- 3 DIMENSIONS TABLES and 1 FACT TABLE
-- =========================================================


-- Station dimension: keep EVA as the single external identifier
CREATE TABLE dim_station (
  station_key   BIGSERIAL PRIMARY KEY,
  eva           BIGINT NOT NULL UNIQUE,     -- identifier used in xml
  station_name  TEXT NOT NULL,
  lat           DOUBLE PRECISION,
  lon           DOUBLE PRECISION
);

-- Train dimension (minimal; you can extend later)
CREATE TABLE dim_train (
  train_key     BIGSERIAL PRIMARY KEY,
  category      TEXT NOT NULL,              -- tl.c
  train_number  TEXT NOT NULL,              -- tl.n
  owner         TEXT,                       -- tl.o
  trip_type     TEXT,                       -- tl.t
  filter_flags  TEXT,                       -- tl.f
  UNIQUE (category, train_number, owner, trip_type, filter_flags)
);

-- Time dimension (minute granularity is enough)
CREATE TABLE dim_time (
  time_key   BIGINT PRIMARY KEY,            -- e.g., 202509051116
  ts         TIMESTAMP WITHOUT TIME ZONE NOT NULL UNIQUE,
  date       DATE NOT NULL,
  hour       SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  minute     SMALLINT NOT NULL CHECK (minute BETWEEN 0 AND 59),
  dow        SMALLINT NOT NULL CHECK (dow BETWEEN 1 AND 7),
  is_weekend BOOLEAN NOT NULL
);



-- Fact table: one row per (station, train, stop_id, event_type) observed at a snapshot
CREATE TABLE fact_train_movement (
  movement_key       BIGSERIAL PRIMARY KEY,

  station_key        BIGINT NOT NULL REFERENCES dim_station(station_key),
  train_key          BIGINT NOT NULL REFERENCES dim_train(train_key),

  snapshot_time_key  BIGINT NOT NULL REFERENCES dim_time(time_key),

  stop_id            TEXT NOT NULL,         -- xml stop @id
  event_type         CHAR(1) NOT NULL CHECK (event_type IN ('A','D')),

  planned_time_key   BIGINT REFERENCES dim_time(time_key),  -- pt
  changed_time_key   BIGINT REFERENCES dim_time(time_key),  -- ct (nullable)

  event_status       CHAR(1) CHECK (event_status IN ('p','a','c')), -- cs
  planned_platform   TEXT,
  changed_platform   TEXT,
  line               TEXT,
  planned_path       TEXT,

  delay_minutes      INT,
  is_cancelled       BOOLEAN NOT NULL DEFAULT FALSE,

  CONSTRAINT uq_fact_natural UNIQUE (snapshot_time_key, station_key, stop_id, event_type)
);

-- =========================================================
-- INDEXES FOR TASK 2 
-- =========================================================

-- Task 2.1 / 2.4: station_name lookup
CREATE INDEX idx_station_name ON dim_station (station_name);


-- Task 2.2: closest station using KNN with GiST on a point expression
CREATE INDEX idx_station_point_gist ON dim_station USING gist (point(lon, lat));

-- Task 2.3: find snapshot time_key(s) by date+hour
CREATE INDEX idx_time_date_hour ON dim_time (date, hour);

-- Task 2.3: count cancelled trains quickly (partial index)
CREATE INDEX idx_fact_cancel_by_snapshot ON fact_train_movement (snapshot_time_key) WHERE is_cancelled = true;

-- Task 2.4: average delay by station (partial index for valid delays)
CREATE INDEX idx_fact_station_delay_valid ON fact_train_movement (station_key) WHERE delay_minutes IS NOT NULL AND is_cancelled = false;
