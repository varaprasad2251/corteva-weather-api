#!/usr/bin/env python3
"""
Problem 3: Data Analysis Script

- This script calculates annual weather statistics from the weather data.
- Calculate average (max_temp, min_temp) and total_precipitation for all stations, years, 
  skipping data containing null values (-9999).

Usage:
    python data_analysis.py
"""

import logging
import os
import sqlite3
from datetime import datetime
from db_utils import setup_database
from logging_utils import setup_logging


class WeatherDataAnalysis:
    """Weather data analysis class"""

    def __init__(self, db_path: str = "db/weather_data.db", setup_db: bool = True):
        """
        Initialize the analysis process.

        Args:
            db_path: Path to SQLite database file (default: db/weather_data.db)
            setup_db: Whether to set up the database schema (default: True)
        """
        self.db_path = db_path
        self.logger = setup_logging("logs/weather_analysis.log", __name__)
        if setup_db:
            setup_database(self.db_path)

    def calculate_and_store_annual_stats(self):
        """
        Calculate annual weather statistics for all stations and store in database.
        
        Skips null values (-9999) and calculates:
        - Average max temperature (in degrees Celsius)
        - Average min temperature (in degrees Celsius) 
        - Total precipitation (in centimeters)
        
        Returns:
            Number of records stored
        """
        start_time = datetime.now()
        self.logger.info("Starting annual weather statistics calculation")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Clear existing stats
                cursor.execute("DELETE FROM annual_weather_stats")
                self.logger.info("Cleared existing annual statistics")
                
                # Query to calculate annual statistics for all stations
                # Skip null values (-9999) using WHERE clauses
                query = """
                    INSERT INTO annual_weather_stats
                    (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)
                    SELECT 
                        station_id,
                        CAST(substr(date, 1, 4) AS INTEGER) as year,
                        ROUND(AVG(max_temp) / 10.0, 2) as avg_max_temp,
                        ROUND(AVG(min_temp) / 10.0, 2) as avg_min_temp,
                        ROUND(SUM(precipitation) / 100.0, 2) as total_precipitation
                    FROM weather_records
                    WHERE max_temp != -9999 
                      AND min_temp != -9999 
                      AND precipitation != -9999
                    GROUP BY station_id, year
                    ORDER BY station_id, year
                """
                
                cursor.execute(query)
                records_stored = cursor.rowcount
                
                conn.commit()
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                self.logger.info(
                    "Analysis completed: %d records stored in %.2fs",
                    records_stored, processing_time
                )
                
                return records_stored
                
        except sqlite3.DatabaseError as e:
            self.logger.error("Database error during analysis: %s", e)
            raise
        except Exception as e:
            self.logger.error("Error during analysis: %s", e)
            raise

    def get_analysis_summary(self):
        """
        Get summary of analysis results.
        
        Returns:
            Dictionary with analysis summary
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get total records
                cursor.execute("SELECT COUNT(*) FROM annual_weather_stats")
                total_records = cursor.fetchone()[0]
                
                # Get unique stations
                cursor.execute("SELECT COUNT(DISTINCT station_id) FROM annual_weather_stats")
                stations = cursor.fetchone()[0]
                
                # Get year range
                cursor.execute("SELECT MIN(year), MAX(year) FROM annual_weather_stats")
                year_range = cursor.fetchone()
                
                return {
                    "total_records": total_records,
                    "stations": stations,
                    "year_range": year_range,
                }
                
        except sqlite3.DatabaseError as e:
            self.logger.error("Database error getting analysis summary: %s", e)
            return {}
        except Exception as e:
            self.logger.error("Error getting analysis summary: %s", e)
            return {}


def main():
    """Main function to run the weather data analysis."""
    try:
        # Initialize the weather data analysis class
        analysis = WeatherDataAnalysis()
        
        # Calculate and store annual statistics
        records_stored = analysis.calculate_and_store_annual_stats()
        
        print("Analysis completed successfully!")
        print("   Records stored: %d" % records_stored)
        
        # Show summary
        summary = analysis.get_analysis_summary()
        if summary:
            print("\nAnalysis Summary:")
            print("   Total records: %d" % summary["total_records"])
            print("   Number of stations: %d" % summary["stations"])
            if summary["year_range"][0]:
                print(
                    "   Year range: %d - %d" % (
                        summary["year_range"][0], summary["year_range"][1]
                    )
                )
        
    except Exception as e:
        print("Analysis failed: %s" % e)
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
 