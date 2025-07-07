#!/usr/bin/env python3
"""
Database utility functions for weather data project.

Provides setup_database to create tables from weather_schema.sql.
"""
import argparse
import sqlite3


def setup_database(db_path: str) -> None:
    """
    Set up the SQLite database with required tables.
    Args:
        db_path: Path to the SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_records (
            station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            max_temp INTEGER,
            min_temp INTEGER,
            precipitation INTEGER,
            PRIMARY KEY (station_id, date)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS annual_weather_stats (
            station_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            avg_max_temp REAL,
            avg_min_temp REAL,
            total_precipitation REAL,
            PRIMARY KEY (station_id, year)
        )
        """
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up the weather database schema.")
    parser.add_argument(
        "--db-path",
        type=str,
        default="db/weather_data.db",
        help="Path to SQLite database file (default: db/weather_data.db)",
    )
    parser.add_argument(
        "--schema-path",
        type=str,
        default="weather_schema.sql",
        help="Path to SQL schema file (default: weather_schema.sql)",
    )
    args = parser.parse_args()
    setup_database(args.db_path)
    print(f"Database schema created at {args.db_path}")
