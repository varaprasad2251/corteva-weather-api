#!/usr/bin/env python3
"""
Problem 3: Data Analysis Script

- This script calculates annual weather statistics from the weather data.
- Calculate average (max_temp, min_temp) and total_precipitation for all stations,
  years, skipping data containing null values (-9999).

Usage:
    python data_analysis.py
"""

import sqlite3
from logging_utils import setup_logging


class WeatherDataAnalysis:
    def __init__(self, db_path: str = "db/weather_data.db", logger=None):
        self.db_path = db_path
        self.logger = logger or setup_logging()
        self.conn = sqlite3.connect(self.db_path)
        self.logger.info(f"Connected to database at {self.db_path}")

    def calculate_annual_stats(self):
        """
        Calculate annual weather statistics for all stations.
        Returns:
            List of tuples with (station_id, year, avg_max_temp, avg_min_temp,
            total_precipitation)
        """
        self.logger.info("Calculating annual weather statistics...")
        query = """
            SELECT station_id,
                   strftime('%Y', date) AS year,
                   ROUND(AVG(CASE WHEN max_temp != -9999 THEN max_temp / 10.0 END), 2)
                       AS avg_max_temp,
                   ROUND(AVG(CASE WHEN min_temp != -9999 THEN min_temp / 10.0 END), 2)
                       AS avg_min_temp,
                   ROUND(SUM(CASE WHEN precipitation != -9999 THEN
                        precipitation / 100.0 END), 2) AS total_precipitation
            FROM weather_records
            GROUP BY station_id, year
            ORDER BY station_id, year
            """
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        self.logger.info(f"Calculated stats for {len(results)} station-year pairs.")
        return results

    def store_annual_stats(self, stats):
        """
        Store annual weather statistics in the database.
        Args:
            stats: List of tuples with annual statistics
        """
        self.logger.info("Storing annual statistics in the database...")
        cursor = self.conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO annual_weather_stats
            (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)
            VALUES (?, ?, ?, ?, ?)
            """,
            stats,
        )
        self.conn.commit()
        self.logger.info("Annual statistics stored successfully.")

    def run(self):
        self.logger.info("Starting data analysis workflow...")
        stats = self.calculate_annual_stats()
        self.store_annual_stats(stats)
        self.logger.info("Data analysis workflow completed.")

    def close(self):
        self.conn.close()
        self.logger.info("Database connection closed.")


if __name__ == "__main__":
    analysis = WeatherDataAnalysis()
    analysis.run()
    analysis.close()
