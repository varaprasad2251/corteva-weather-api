#!/usr/bin/env python3
"""
Problem 2: Data Ingestion Script

This script ingests weather data from text files into a SQLite database.
Supports both single file and directory processing with flexible input options.

Usage:
    python data_ingestion.py                    # Process all files in data/wx_data/
    python data_ingestion.py /path/to/file.txt  # Process single file
    python data_ingestion.py /path/to/directory # Process all .txt files in directory
"""

import argparse
import os
import sqlite3
from datetime import datetime
from typing import Dict, Optional, Tuple

from db_utils import setup_database
from logging_utils import setup_logging


class WeatherDataIngestion:
    """Weather data ingestion class"""

    def __init__(self, db_path: str = "db/weather_data.db", setup_db: bool = True):
        """
        Initialize the ingestion process.

        Args:
            db_path: Path to SQLite database file (default: db/weather_data.db)
            setup_db: Whether to set up the database schema (default: True)
        """
        self.db_path = db_path
        self.logger = setup_logging("logs/weather_ingestion.log", __name__)
        if setup_db:
            setup_database(self.db_path)

    def convert_date_format(self, date_str: str) -> Optional[str]:
        """Convert date from YYYYMMDD format to ISO 8601 format (YYYY-MM-DD)"""
        try:
            # Parse YYYYMMDD format using datetime
            date_obj = datetime.strptime(date_str, "%Y%m%d")

            # Return ISO format
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def parse_weather_line(
        self, line: str, station_id: str
    ) -> Optional[Tuple[str, str, int, int, int]]:
        """
        Parse a single line of weather data.

        Args:
            line: Tab-separated line from weather file
            station_id: Weather station identifier

        Returns:
            Tuple of (
                station_id,
                date_iso,
                max_temp,
                min_temp,
                precipitation
            ) or None if invalid
        """
        try:
            parts = [part.strip() for part in line.split("\t")]
            if len(parts) != 4:
                self.logger.warning(
                    f"Invalid line format for station {station_id}: {line}"
                )
                return None

            date_str, max_temp_str, min_temp_str, precip_str = parts

            # Validate date format (YYYYMMDD)
            date_iso = self.convert_date_format(date_str)
            if not date_iso:
                self.logger.warning(
                    "Invalid date format for station %s: %s", station_id, date_str
                )
                return None

            # Parse numeric values
            try:
                max_temp = int(max_temp_str)
                min_temp = int(min_temp_str)
                precipitation = int(precip_str)
            except ValueError:
                self.logger.warning(
                    "Invalid numeric values for station %s: %s", station_id, line
                )
                return None

            return (station_id, date_iso, max_temp, min_temp, precipitation)
        except Exception as e:
            self.logger.error(
                "Error parsing line for station %s: %s. Error: %s", station_id, line, e
            )
            return None

    def get_station_id_from_filename(self, filename: str) -> str:
        """Extract station ID from weather data filename"""
        return filename.replace(".txt", "")

    def _insert_weather_record(self, cursor, parsed_data, station_id):
        """
        Insert a single weather record into the database.

        Args:
            cursor: SQLite cursor object
            parsed_data: Tuple of (station_id, date, max_temp, min_temp, precipitation)
            station_id: Weather station identifier for logging

        Returns:
            Tuple of (success, skipped, error):
                - success: Boolean indicating if record was inserted
                - skipped: Boolean indicating if record was skipped (duplicate)
                - error: Error object if insertion failed, None otherwise
        """
        try:
            cursor.execute(
                """
                INSERT INTO weather_records
                (station_id, date, max_temp, min_temp, precipitation)
                VALUES (?, ?, ?, ?, ?)
                """,
                parsed_data,
            )
            return True, False, None
        except sqlite3.IntegrityError:
            self.logger.debug(
                "Duplicate record skipped: %s_%s", station_id, parsed_data[1]
            )
            return False, True, None
        except sqlite3.DatabaseError as e:
            self.logger.error("Database error inserting record: %s", e)
            return False, False, e
        except Exception as e:
            self.logger.error("Error inserting record: %s", e)
            return False, False, e

    def ingest_weather_file(self, file_path: str) -> Dict:
        """
        Ingest weather data from a single file.

        Args:
            file_path: Path to the weather data file to ingest

        Returns:
            Dictionary containing ingestion statistics:
                - station_id: Weather station identifier
                - file_path: Path to the ingested file
                - start_time: Start time of ingestion
                - end_time: End time of ingestion
                - duration_seconds: Total duration in seconds
                - records_processed: Total records processed
                - records_ingested: Records successfully inserted
                - records_skipped: Records skipped (duplicates)
                - errors: Number of errors encountered
        """
        start_time = datetime.now()
        station_id = os.path.splitext(os.path.basename(file_path))[0]
        self.logger.info(
            "Starting ingestion for station %s from %s", station_id, file_path
        )

        records_processed = 0
        records_ingested = 0
        records_skipped = 0
        errors = 0

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                with open(file_path, "r", encoding="utf-8") as file:
                    for line in file:
                        line = line.strip()
                        if not line:
                            continue

                        records_processed += 1

                        # Parse the line
                        parsed_data = self.parse_weather_line(line, station_id)
                        if not parsed_data:
                            errors += 1
                            continue

                        # Insert record
                        success, skipped, err = self._insert_weather_record(
                            cursor, parsed_data, station_id
                        )
                        if success:
                            records_ingested += 1
                        elif skipped:
                            records_skipped += 1
                        elif err:
                            errors += 1

                conn.commit()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            stats = {
                "station_id": station_id,
                "file_path": file_path,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "records_processed": records_processed,
                "records_ingested": records_ingested,
                "records_skipped": records_skipped,
                "errors": errors,
            }

            self.logger.info(
                "Completed ingestion for station %s: %d ingested, %d skipped, "
                "%d errors in %.2f seconds",
                station_id,
                records_ingested,
                records_skipped,
                errors,
                duration,
            )

            return stats

        except sqlite3.DatabaseError as e:
            self.logger.error("Database error ingesting file %s: %s", file_path, e)
            raise
        except Exception as e:
            self.logger.error("Error ingesting file %s: %s", file_path, e)
            raise

    def ingest_weather_data(self, input_path: str = "data/wx_data") -> Dict:
        """
        Ingest weather data from files or directory.

        Args:
            input_path: Path to weather data file or directory (default: data/wx_data)

        Returns:
            Dictionary containing overall ingestion statistics:
                - start_time: Start time of ingestion process
                - end_time: End time of ingestion process
                - duration_seconds: Total duration in seconds
                - files_processed: Total files processed
                - files_successful: Files successfully processed
                - files_failed: Files that failed to process
                - total_records_processed: Total records processed across all files
                - total_records_ingested: Total records successfully inserted
                - total_records_skipped: Total records skipped (duplicates)
                - total_errors: Total errors encountered
                - file_stats: List of individual file statistics

        Raises:
            FileNotFoundError: If input path does not exist
            ValueError: If input path is invalid or no files found
        """
        start_time = datetime.now()
        self.logger.info("Starting weather data ingestion from %s", input_path)

        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input path not found: {input_path}")

        # Determine if input is a file or directory
        if os.path.isfile(input_path):
            # Single file processing
            if not input_path.endswith(".txt"):
                raise ValueError(f"Input file must be a .txt file: {input_path}")
            weather_files = [input_path]
        else:
            # Directory processing
            weather_files = [
                os.path.join(input_path, f)
                for f in os.listdir(input_path)
                if f.endswith(".txt")
            ]

        if not weather_files:
            raise ValueError(f"No weather data files found in {input_path}")

        total_stats = {
            "start_time": start_time,
            "files_processed": 0,
            "files_successful": 0,
            "files_failed": 0,
            "total_records_processed": 0,
            "total_records_ingested": 0,
            "total_records_skipped": 0,
            "total_errors": 0,
            "file_stats": [],
        }

        for file_path in weather_files:
            try:
                file_stats = self.ingest_weather_file(file_path)
                total_stats["file_stats"].append(file_stats)
                total_stats["files_processed"] += 1
                total_stats["files_successful"] += 1
                total_stats["total_records_processed"] += file_stats[
                    "records_processed"
                ]
                total_stats["total_records_ingested"] += file_stats["records_ingested"]
                total_stats["total_records_skipped"] += file_stats["records_skipped"]
                total_stats["total_errors"] += file_stats["errors"]
            except sqlite3.DatabaseError as e:
                total_stats["files_processed"] += 1
                total_stats["files_failed"] += 1
                self.logger.error("Database error ingesting file %s: %s", file_path, e)
            except Exception as e:
                total_stats["files_processed"] += 1
                total_stats["files_failed"] += 1
                self.logger.error("Failed to ingest file %s: %s", file_path, e)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        total_stats["end_time"] = end_time
        total_stats["duration_seconds"] = duration

        self.logger.info("=" * 80)
        self.logger.info("WEATHER DATA INGESTION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info("Start Time: %s", total_stats["start_time"])
        self.logger.info("End Time: %s", total_stats["end_time"])
        self.logger.info("Total Duration: %.2f seconds", duration)
        self.logger.info("Files Processed: %d", total_stats["files_processed"])
        self.logger.info("Files Successful: %d", total_stats["files_successful"])
        self.logger.info("Files Failed: %d", total_stats["files_failed"])
        self.logger.info(
            "Total Records Processed: %d", total_stats["total_records_processed"]
        )
        self.logger.info(
            "Total Records Ingested: %d", total_stats["total_records_ingested"]
        )
        self.logger.info(
            "Total Records Skipped: %d", total_stats["total_records_skipped"]
        )
        self.logger.info("Total Errors: %d", total_stats["total_errors"])

        return total_stats


def main():
    """Main function to run the weather data ingestion."""
    parser = argparse.ArgumentParser(
        description="Weather Data Ingestion Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_ingestion.py                    # Process all files in data/wx_data/
  python data_ingestion.py /path/to/file.txt  # Process single file
  python data_ingestion.py /path/to/directory # Process all .txt files in directory
        """,
    )

    parser.add_argument(
        "input_path",
        nargs="?",
        default="data/wx_data",
        help="Path to weather data file or directory (default: data/wx_data)",
    )

    args = parser.parse_args()

    try:
        # Initialize the weather data ingestion class
        ingestion = WeatherDataIngestion()

        # Ingest weather data
        ingestion.ingest_weather_data(args.input_path)
    except Exception as e:
        print("Error: %s", e)
        return 1

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
