-- Problem 1: Data Modeling (Using SQLite)

-- Weather data table for storing daily weather records from stations
CREATE TABLE IF NOT EXISTS weather_records (
    station_id TEXT NOT NULL,
    date TEXT NOT NULL CHECK (
        date LIKE '____-__-__' 
        AND date(date) IS NOT NULL
        AND CAST(substr(date, 1, 4) AS INTEGER) BETWEEN 1800 AND 2100
    ), -- ISO 8601 format (YYYY-MM-DD)
    max_temp INTEGER,   -- tenths of a degree Celsius
    min_temp INTEGER,   -- tenths of a degree Celsius
    precipitation INTEGER, -- tenths of a millimeter
    PRIMARY KEY (station_id, date) -- To ensure unique entry for a given station and date
);

-- Problem 3: Annual Weather Statistics Table
CREATE TABLE IF NOT EXISTS annual_weather_stats (
    station_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    avg_max_temp REAL,         -- Average max temperature in degrees Celsius
    avg_min_temp REAL,         -- Average min temperature in degrees Celsius
    total_precipitation REAL,  -- Total precipitation in centimeters
    PRIMARY KEY (station_id, year)
);