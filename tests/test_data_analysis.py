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
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data_analysis import WeatherDataAnalysis, main


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

    def test_weather_data_analysis_init(self):
        """Test WeatherDataAnalysis class initialization."""
        analysis = WeatherDataAnalysis(self.test_db_path, setup_db=False)
        
        self.assertEqual(analysis.db_path, self.test_db_path)
        self.assertIsNotNone(analysis.logger)

    def test_calculate_and_store_annual_stats_empty_db(self):
        """Test annual stats calculation with empty database."""
        analysis = WeatherDataAnalysis(self.test_db_path, setup_db=False)
        
        # Calculate stats (should return 0 for empty db)
        records_stored = analysis.calculate_and_store_annual_stats()
        self.assertEqual(records_stored, 0)

    def test_calculate_and_store_annual_stats_with_data(self):
        """Test annual stats calculation with sample data."""
        analysis = WeatherDataAnalysis(self.test_db_path, setup_db=False)
        
        # Insert test data
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            test_data = [
                ('USC00110072', '1990-01-01', 250, 100, 50),   # 25.0°C, 10.0°C, 0.5cm
                ('USC00110072', '1990-01-02', 260, 110, 60),   # 26.0°C, 11.0°C, 0.6cm
                ('USC00110072', '1990-01-03', 240, 90, 40),    # 24.0°C, 9.0°C, 0.4cm
                ('USC00110072', '1991-01-01', 270, 120, 70),   # 27.0°C, 12.0°C, 0.7cm
                ('USC00257715', '1990-01-01', 280, 130, 80),   # 28.0°C, 13.0°C, 0.8cm
                # Null values that should be skipped
                ('USC00110072', '1990-01-04', -9999, 100, 50), # Skip this row
                ('USC00110072', '1990-01-05', 250, -9999, 50), # Skip this row
                ('USC00110072', '1990-01-06', 250, 100, -9999), # Skip this row
            ]
            cursor.executemany(
                "INSERT INTO weather_records (station_id, date, max_temp, min_temp, precipitation) VALUES (?, ?, ?, ?, ?)",
                test_data
            )
            conn.commit()
        
        # Calculate stats
        records_stored = analysis.calculate_and_store_annual_stats()
        
        # Should have 3 records (2 stations x 2 years, but USC00257715 only has 1 year)
        self.assertEqual(records_stored, 3)
        
        # Verify the calculated values
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM annual_weather_stats ORDER BY station_id, year")
            results = cursor.fetchall()
            
            # Check USC00110072 1990: avg_max=25.0, avg_min=10.0, total_precip=1.5cm
            self.assertEqual(results[0][0], 'USC00110072')  # station_id
            self.assertEqual(results[0][1], 1990)           # year
            self.assertEqual(results[0][2], 25.0)           # avg_max_temp
            self.assertEqual(results[0][3], 10.0)           # avg_min_temp
            self.assertEqual(results[0][4], 1.5)            # total_precipitation
            
            # Check USC00110072 1991: avg_max=27.0, avg_min=12.0, total_precip=0.7cm
            self.assertEqual(results[1][0], 'USC00110072')  # station_id
            self.assertEqual(results[1][1], 1991)           # year
            self.assertEqual(results[1][2], 27.0)           # avg_max_temp
            self.assertEqual(results[1][3], 12.0)           # avg_min_temp
            self.assertEqual(results[1][4], 0.7)            # total_precipitation
            
            # Check USC00257715 1990: avg_max=28.0, avg_min=13.0, total_precip=0.8cm
            self.assertEqual(results[2][0], 'USC00257715')  # station_id
            self.assertEqual(results[2][1], 1990)           # year
            self.assertEqual(results[2][2], 28.0)           # avg_max_temp
            self.assertEqual(results[2][3], 13.0)           # avg_min_temp
            self.assertEqual(results[2][4], 0.8)            # total_precipitation

    def test_get_analysis_summary(self):
        """Test analysis summary generation."""
        analysis = WeatherDataAnalysis(self.test_db_path, setup_db=False)
        
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO annual_weather_stats 
                (station_id, year, avg_max_temp, avg_min_temp, total_precipitation) 
                VALUES 
                ('USC00110072', 1990, 25.0, 10.0, 1.5),
                ('USC00110072', 1991, 27.0, 12.0, 0.7),
                ('USC00257715', 1990, 28.0, 13.0, 0.8)
            """)
            conn.commit()
        
        # Get summary
        summary = analysis.get_analysis_summary()
        
        # Verify summary
        self.assertEqual(summary['total_records'], 3)
        self.assertEqual(summary['stations'], 2)
        self.assertEqual(summary['year_range'], (1990, 1991))

    def test_get_analysis_summary_empty_db(self):
        """Test analysis summary with empty database."""
        analysis = WeatherDataAnalysis(self.test_db_path, setup_db=False)
        
        # Get summary
        summary = analysis.get_analysis_summary()
        
        # Verify summary for empty db
        self.assertEqual(summary['total_records'], 0)
        self.assertEqual(summary['stations'], 0)
        self.assertIsNone(summary['year_range'][0])
        self.assertIsNone(summary['year_range'][1])

    @patch('data_analysis.WeatherDataAnalysis')
    def test_main_success(self, mock_analysis_class):
        """Test main function success path."""
        # Mock the analysis class
        mock_analysis = MagicMock()
        mock_analysis_class.return_value = mock_analysis
        mock_analysis.calculate_and_store_annual_stats.return_value = 100
        mock_analysis.get_analysis_summary.return_value = {
            'total_records': 100,
            'stations': 10,
            'year_range': (1990, 2000)
        }
        
        # Test main function
        result = main()
        
        # Verify function calls
        mock_analysis_class.assert_called_once()
        mock_analysis.calculate_and_store_annual_stats.assert_called_once()
        mock_analysis.get_analysis_summary.assert_called_once()
        
        # Should return 0 for success
        self.assertEqual(result, 0)

    @patch('data_analysis.WeatherDataAnalysis')
    def test_main_database_error(self, mock_analysis_class):
        """Test main function with database error."""
        # Mock the analysis class to raise exception
        mock_analysis = MagicMock()
        mock_analysis_class.return_value = mock_analysis
        mock_analysis.calculate_and_store_annual_stats.side_effect = Exception("Database error")
        
        # Test main function
        result = main()
        
        # Should return 1 for error
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main() 