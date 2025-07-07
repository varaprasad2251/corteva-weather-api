#!/usr/bin/env python3
"""
Database utility functions for weather data project.

Provides setup_database to create tables from weather_schema.sql.
"""
import os
import sqlite3
import argparse


def setup_database(db_path: str = "db/weather_data.db", schema_path: str = "weather_schema.sql"):
    """
    Create database and tables if they don't exist.

    Args:
        db_path: Path to SQLite database file (default: db/weather_data.db)
        schema_path: Path to SQL schema file (default: weather_schema.sql)
    """
    # Ensure database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        with open(schema_path, "r", encoding="utf-8") as schema_file:
            schema_sql = schema_file.read()
            cursor.executescript(schema_sql)
        conn.commit()


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
    setup_database(args.db_path, args.schema_path)
    print(f"Database schema created at {args.db_path}") 