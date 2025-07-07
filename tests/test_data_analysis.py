#!/usr/bin/env python3
"""
Tests for data_analysis.py

Tests the simplified data analysis functionality including:
- WeatherDataAnalysis class initialization
- Database setup
- Annual statistics calculation
- Summary generation
- Main workflow
"""

import os
import sqlite3
import sys
import tempfile
import unittest

from data_analysis import WeatherDataAnalysis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestDataAnalysis(unittest.TestCase):
    """Test cases for data analysis functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary database for testing
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test_weather_data.db")

        # Create a minimal schema for testing
        self.test_schema = """
        CREATE TABLE IF NOT EXISTS weather_records (
            station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            max_temp INTEGER,
            min_temp INTEGER,
            precipitation INTEGER,
            PRIMARY KEY (station_id, date)
        );

        CREATE TABLE IF NOT EXISTS annual_weather_stats (
            station_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            avg_max_temp REAL,
            avg_min_temp REAL,
            total_precipitation REAL,
            PRIMARY KEY (station_id, year)
        );
        """

        # Actually create the database with schema
        with sqlite3.connect(self.test_db_path) as conn:
            conn.executescript(self.test_schema)

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary files
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_calculate_and_store_annual_stats_empty_db(self):
        """Test annual stats calculation with empty database."""
        analysis = WeatherDataAnalysis(self.test_db_path)
        stats = analysis.calculate_annual_stats()
        self.assertEqual(stats, [])
        analysis.store_annual_stats(stats)
        cursor = analysis.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM annual_weather_stats")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)
        analysis.close()

    def test_calculate_and_store_annual_stats_with_data(self):
        """Test annual stats calculation with sample data."""
        analysis = WeatherDataAnalysis(self.test_db_path)
        cursor = analysis.conn.cursor()
        test_data = [
            ("USC00110072", "1990-01-01", 250, 100, 50),
            ("USC00110072", "1990-01-02", 260, 110, 60),
            ("USC00110072", "1990-01-03", 240, 90, 40),
            ("USC00110072", "1991-01-01", 270, 120, 70),
            ("USC00257715", "1990-01-01", 280, 130, 80),
            ("USC00110072", "1990-01-04", -9999, 100, 50),
            ("USC00110072", "1990-01-05", 250, -9999, 50),
            ("USC00110072", "1990-01-06", 250, 100, -9999),
        ]
        cursor.executemany(
            """INSERT INTO weather_records (station_id, date, max_temp, min_temp,
            precipitation) VALUES (?, ?, ?, ?, ?)""",
            test_data,
        )
        analysis.conn.commit()
        stats = analysis.calculate_annual_stats()
        analysis.store_annual_stats(stats)
        cursor.execute(
            """SELECT * FROM annual_weather_stats
                            ORDER BY station_id, year"""
        )
        results = cursor.fetchall()
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][0], "USC00110072")
        self.assertEqual(results[0][1], 1990)
        self.assertEqual(results[0][2], 25.0)
        self.assertEqual(results[0][3], 10.0)
        self.assertEqual(results[0][4], 2.5)
        self.assertEqual(results[1][0], "USC00110072")
        self.assertEqual(results[1][1], 1991)
        self.assertEqual(results[1][2], 27.0)
        self.assertEqual(results[1][3], 12.0)
        self.assertEqual(results[1][4], 0.7)
        self.assertEqual(results[2][0], "USC00257715")
        self.assertEqual(results[2][1], 1990)
        self.assertEqual(results[2][2], 28.0)
        self.assertEqual(results[2][3], 13.0)
        self.assertEqual(results[2][4], 0.8)
        analysis.close()


if __name__ == "__main__":
    unittest.main()
