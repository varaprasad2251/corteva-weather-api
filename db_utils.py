#!/usr/bin/env python3
"""
Database utility functions for weather data project.

Provides setup_database to create tables from weather_schema.sql.
"""
import argparse
import sqlite3


def setup_database(db_path: str, schema_path: str = "weather_schema.sql") -> None:
    """
    Set up the SQLite database with required tables from schema file.
    Args:
        db_path: Path to the SQLite database file
        schema_path: Path to the SQL schema file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Read and execute the schema file
    with open(schema_path, 'r') as schema_file:
        schema_sql = schema_file.read()
        cursor.executescript(schema_sql)
    
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
    setup_database(args.db_path, args.schema_path)
    print(f"Database schema created at {args.db_path}")
