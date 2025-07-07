#!/usr/bin/env python3
"""
Tests for data_ingestion.py

Tests the data ingestion functionality including:
- WeatherDataIngestion class initialization
- Database setup
- Data parsing and validation
- File processing
- Main workflow
"""

import os
import sqlite3
import tempfile
import unittest
import shutil
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from data_ingestion import WeatherDataIngestion, main


class TestDataIngestion(unittest.TestCase):
    """Test cases for data ingestion functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for testing
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
        """
        
        # Actually create the database with schema
        with sqlite3.connect(self.test_db_path) as conn:
            conn.executescript(self.test_schema)
        
        # Sample weather data lines (tab-separated format)
        self.sample_data_lines = [
            "19900101\t250\t100\t50",
            "19900102\t260\t110\t60",
            "19900103\t240\t90\t40",
        ]

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary files
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_weather_data_ingestion_init(self):
        """Test WeatherDataIngestion class initialization."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        self.assertEqual(ingestion.db_path, self.test_db_path)
        self.assertIsNotNone(ingestion.logger)

    def test_convert_date_format_valid(self):
        """Test date format conversion with valid dates."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        test_cases = [
            ("19900101", "1990-01-01"),
            ("20001231", "2000-12-31"),
            ("20240229", "2024-02-29"),  # Leap year
        ]
        
        for input_date, expected in test_cases:
            with self.subTest(input_date=input_date):
                result = ingestion.convert_date_format(input_date)
                self.assertEqual(result, expected)

    def test_convert_date_format_invalid(self):
        """Test date format conversion with invalid dates."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        invalid_dates = [
            "19901301",  # Invalid month
            "19900001",  # Invalid month
            "19900132",  # Invalid day
            "19900100",  # Invalid day
            "19900230",  # Invalid day for February
            "20230229",  # Invalid leap day in non-leap year
            "invalid",   # Completely invalid
        ]
        
        for date_str in invalid_dates:
            with self.subTest(date=date_str):
                result = ingestion.convert_date_format(date_str)
                self.assertIsNone(result)

    def test_parse_weather_line_valid(self):
        """Test parsing valid weather data lines."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        test_cases = [
            # (line, station_id, expected_result)
            ("19900101\t250\t100\t50", "USC00110072", 
             ("USC00110072", "1990-01-01", 250, 100, 50)),
            ("19900102\t260\t110\t60", "USC00110072", 
             ("USC00110072", "1990-01-02", 260, 110, 60)),
            ("19900103\t-100\t-200\t0", "USC00110072", 
             ("USC00110072", "1990-01-03", -100, -200, 0)),
        ]
        
        for line, station_id, expected in test_cases:
            with self.subTest(line=line):
                result = ingestion.parse_weather_line(line, station_id)
                self.assertEqual(result, expected)

    def test_parse_weather_line_invalid(self):
        """Test parsing invalid weather data lines."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        invalid_cases = [
            ("", "USC00110072"),  # Empty line
            ("19900101\t250\t100", "USC00110072"),  # Too few fields
            ("19900101\t250\t100\t50\textra", "USC00110072"),  # Too many fields
            ("19900101\tinvalid\t100\t50", "USC00110072"),  # Invalid numeric
            ("invalid\t250\t100\t50", "USC00110072"),  # Invalid date
        ]
        
        for line, station_id in invalid_cases:
            with self.subTest(line=line):
                result = ingestion.parse_weather_line(line, station_id)
                self.assertIsNone(result)

    def test_get_station_id_from_filename(self):
        """Test station ID extraction from filename."""
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        test_cases = [
            ("USC00110072.txt", "USC00110072"),
            ("USC00257715.txt", "USC00257715"),
            ("station123.txt", "station123"),
        ]
        
        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = ingestion.get_station_id_from_filename(filename)
                self.assertEqual(result, expected)

    def test_ingest_weather_file_success(self):
        """Test successful weather file ingestion."""
        # Create test data file
        test_file_path = os.path.join(self.temp_dir, "USC00110072.txt")
        with open(test_file_path, 'w') as f:
            f.write('\n'.join(self.sample_data_lines))
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest file
        stats = ingestion.ingest_weather_file(test_file_path)
        
        # Verify statistics
        self.assertEqual(stats['station_id'], 'USC00110072')
        self.assertEqual(stats['file_path'], test_file_path)
        self.assertEqual(stats['records_processed'], 3)
        self.assertEqual(stats['records_ingested'], 3)
        self.assertEqual(stats['records_skipped'], 0)
        self.assertEqual(stats['errors'], 0)
        self.assertGreater(stats['duration_seconds'], 0)
        
        # Verify data was stored correctly
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM weather_records")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 3)
            
            # Check specific values
            cursor.execute("""
                SELECT station_id, date, max_temp, min_temp, precipitation 
                FROM weather_records 
                WHERE station_id = 'USC00110072' AND date = '1990-01-01'
            """)
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], 'USC00110072')  # station_id
            self.assertEqual(result[1], '1990-01-01')   # date
            self.assertEqual(result[2], 250)            # max_temp
            self.assertEqual(result[3], 100)            # min_temp
            self.assertEqual(result[4], 50)             # precipitation

    def test_ingest_weather_file_with_invalid_records(self):
        """Test weather file ingestion with some invalid records."""
        # Create test data file with some invalid records
        test_data = [
            "19900101\t250\t100\t50",    # Valid
            "19900102\t260\t110\t60",    # Valid
            "invalid\tline",             # Invalid
            "19900103\t240\t90\t40",     # Valid
            "",                          # Empty line
            "19900104\tinvalid\t90\t40", # Invalid numeric
        ]
        
        test_file_path = os.path.join(self.temp_dir, "USC00110072.txt")
        with open(test_file_path, 'w') as f:
            f.write('\n'.join(test_data))
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest file
        stats = ingestion.ingest_weather_file(test_file_path)
        
        # Should process 5 records (invalid\tline is skipped before counting), 
        # ingest 3 valid ones, skip 0, have 2 errors
        self.assertEqual(stats['records_processed'], 5)
        self.assertEqual(stats['records_ingested'], 3)
        self.assertEqual(stats['records_skipped'], 0)
        self.assertEqual(stats['errors'], 2)
        
        # Verify only valid data was stored
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM weather_records")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 3)

    def test_ingest_weather_file_nonexistent(self):
        """Test ingestion of nonexistent file."""
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Try to ingest nonexistent file
        with self.assertRaises(FileNotFoundError):
            ingestion.ingest_weather_file("nonexistent.txt")

    def test_ingest_weather_file_empty(self):
        """Test ingestion of empty file."""
        # Create empty test file
        test_file_path = os.path.join(self.temp_dir, "empty.txt")
        with open(test_file_path, 'w') as f:
            pass  # Empty file
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest empty file
        stats = ingestion.ingest_weather_file(test_file_path)
        
        # Should process 0 records
        self.assertEqual(stats['records_processed'], 0)
        self.assertEqual(stats['records_ingested'], 0)
        self.assertEqual(stats['records_skipped'], 0)
        self.assertEqual(stats['errors'], 0)

    def test_ingest_weather_data_single_file(self):
        """Test ingesting weather data from a single file."""
        # Create test data file
        test_file_path = os.path.join(self.temp_dir, "USC00110072.txt")
        with open(test_file_path, 'w') as f:
            f.write('\n'.join(self.sample_data_lines))
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest single file
        stats = ingestion.ingest_weather_data(test_file_path)
        
        # Verify statistics
        self.assertEqual(stats['files_processed'], 1)
        self.assertEqual(stats['files_successful'], 1)
        self.assertEqual(stats['files_failed'], 0)
        self.assertEqual(stats['total_records_processed'], 3)
        self.assertEqual(stats['total_records_ingested'], 3)

    def test_ingest_weather_data_directory(self):
        """Test ingesting weather data from a directory."""
        # Create test directory with multiple files
        test_dir = os.path.join(self.temp_dir, "wx_data")
        os.makedirs(test_dir, exist_ok=True)
        
        # Create multiple test files
        files_data = {
            "USC00110072.txt": ["19900101\t250\t100\t50", "19900102\t260\t110\t60"],
            "USC00257715.txt": ["19900101\t270\t120\t70", "19900102\t280\t130\t80"],
        }
        
        for filename, data in files_data.items():
            file_path = os.path.join(test_dir, filename)
            with open(file_path, 'w') as f:
                f.write('\n'.join(data))
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest directory
        stats = ingestion.ingest_weather_data(test_dir)
        
        # Verify statistics
        self.assertEqual(stats['files_processed'], 2)
        self.assertEqual(stats['files_successful'], 2)
        self.assertEqual(stats['files_failed'], 0)
        self.assertEqual(stats['total_records_processed'], 4)
        self.assertEqual(stats['total_records_ingested'], 4)

    @patch('data_ingestion.WeatherDataIngestion')
    @patch('sys.argv', ['data_ingestion.py'])
    def test_main_success(self, mock_ingestion_class):
        """Test main function success path."""
        # Mock the ingestion class
        mock_ingestion = MagicMock()
        mock_ingestion_class.return_value = mock_ingestion
        mock_ingestion.ingest_weather_data.return_value = {
            'files_processed': 1,
            'total_records_ingested': 100
        }
        
        # Test main function
        result = main()
        
        # Verify function calls
        mock_ingestion_class.assert_called_once()
        mock_ingestion.ingest_weather_data.assert_called_once()
        
        # Should return 0 for success
        self.assertEqual(result, 0)

    @patch('data_ingestion.WeatherDataIngestion')
    @patch('sys.argv', ['data_ingestion.py'])
    def test_main_error(self, mock_ingestion_class):
        """Test main function with error."""
        # Mock the ingestion class to raise exception
        mock_ingestion = MagicMock()
        mock_ingestion_class.return_value = mock_ingestion
        mock_ingestion.ingest_weather_data.side_effect = Exception("Test error")
        
        # Test main function
        result = main()
        
        # Should return 1 for error
        self.assertEqual(result, 1)

    def test_duplicate_record_handling(self):
        """Test handling of duplicate records."""
        # Create test data file
        test_file_path = os.path.join(self.temp_dir, "USC00110072.txt")
        with open(test_file_path, 'w') as f:
            f.write('\n'.join(self.sample_data_lines))
        
        # Setup ingestion
        ingestion = WeatherDataIngestion(self.test_db_path, setup_db=False)
        
        # Ingest file twice
        stats1 = ingestion.ingest_weather_file(test_file_path)
        stats2 = ingestion.ingest_weather_file(test_file_path)
        
        # First ingestion should succeed
        self.assertEqual(stats1['records_ingested'], 3)
        self.assertEqual(stats1['records_skipped'], 0)
        
        # Second ingestion should skip duplicates
        self.assertEqual(stats2['records_ingested'], 0)
        self.assertEqual(stats2['records_skipped'], 3)
        
        # Total records in database should still be 3
        with sqlite3.connect(self.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM weather_records")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 3)


if __name__ == '__main__':
    unittest.main() 