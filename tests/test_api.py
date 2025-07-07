#!/usr/bin/env python3
"""
Comprehensive unit tests for the Weather Data API.

This module contains unit tests for all API endpoints, including:
- Weather records endpoint (/api/weather)
- Weather statistics endpoint (/api/weather/stats)
- Health check endpoint (/health)
- Input validation
- Error handling
- Pagination logic

Test Categories:
- Unit tests: Test individual functions and components
- Integration tests: Test API endpoints with database
- Edge cases: Test boundary conditions and error scenarios

Author: Varaprasad Korlapati
Date: 2024
"""

import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from flask.testing import FlaskClient

from api.app import app


class TestWeatherAPI:
    """Test suite for Weather Data API endpoints."""

    @pytest.fixture
    def client(self) -> FlaskClient:
        """
        Create a test client for the Flask application.

        Returns:
            FlaskClient: Test client instance
        """
        app.config["TESTING"] = True
        app.config["WTF_CSRF_ENABLED"] = False
        with app.test_client() as client:
            yield client

    @pytest.fixture
    def test_db(self) -> str:
        """
        Create a temporary test database with sample data.

        Returns:
            str: Path to temporary database file
        """
        # Create a temporary database file
        db_fd, db_path = tempfile.mkstemp()

        # Create tables and insert test data
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Create weather_records table
            cursor.execute(
                """
                CREATE TABLE weather_records (
                    station_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    max_temp INTEGER,
                    min_temp INTEGER,
                    precipitation INTEGER,
                    PRIMARY KEY (station_id, date)
                )
            """
            )

            # Create annual_weather_stats table
            cursor.execute(
                """
                CREATE TABLE annual_weather_stats (
                    station_id TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    avg_max_temp REAL,
                    avg_min_temp REAL,
                    total_precipitation REAL,
                    PRIMARY KEY (station_id, year)
                )
            """
            )

            # Insert test weather records
            test_weather_data = [
                ("USC00110072", "2020-01-01", 250, 100, 50),
                ("USC00110072", "2020-01-02", 260, 110, 0),
                ("USC00110073", "2020-01-01", 240, 90, 75),
                ("USC00110073", "2020-01-02", 245, 95, 25),
            ]
            cursor.executemany(
                "INSERT INTO weather_records VALUES (?, ?, ?, ?, ?)", test_weather_data
            )

            # Insert test annual statistics
            test_stats_data = [
                ("USC00110072", 2020, 25.5, 10.5, 2.5),
                ("USC00110073", 2020, 24.25, 9.25, 5.0),
            ]
            cursor.executemany(
                "INSERT INTO annual_weather_stats VALUES (?, ?, ?, ?, ?)",
                test_stats_data,
            )

            conn.commit()

        yield db_path

        # Cleanup
        os.close(db_fd)
        os.unlink(db_path)

    @pytest.fixture
    def sample_weather_data(self) -> List[Dict[str, Any]]:
        """
        Sample weather data for testing.

        Returns:
            List[Dict[str, Any]]: Sample weather records
        """
        return [
            {
                "station_id": "USC00110072",
                "date": "2020-01-01",
                "max_temp": 250,
                "min_temp": 100,
                "precipitation": 50,
            },
            {
                "station_id": "USC00110072",
                "date": "2020-01-02",
                "max_temp": 260,
                "min_temp": 110,
                "precipitation": 0,
            },
            {
                "station_id": "USC00110073",
                "date": "2020-01-01",
                "max_temp": 240,
                "min_temp": 90,
                "precipitation": 75,
            },
        ]

    @pytest.fixture
    def sample_stats_data(self) -> List[Dict[str, Any]]:
        """
        Sample weather statistics data for testing.

        Returns:
            List[Dict[str, Any]]: Sample weather statistics
        """
        return [
            {
                "station_id": "USC00110072",
                "year": 2020,
                "avg_max_temp": 25.5,
                "avg_min_temp": 10.5,
                "total_precipitation": 2.5,
            },
            {
                "station_id": "USC00110073",
                "year": 2020,
                "avg_max_temp": 24.25,
                "avg_min_temp": 9.25,
                "total_precipitation": 5.0,
            },
        ]

    def test_health_check_endpoint(self, client: FlaskClient) -> None:
        """
        Test the health check endpoint.

        Args:
            client: Flask test client
        """
        response = client.get("/health")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert data["service"] == "weather-data-api"
        assert data["version"] == "1.0.0"
        assert "timestamp" in data

    @pytest.mark.unit
    def test_validate_date_format_valid(self) -> None:
        """Test date format validation with valid dates."""
        from api.app import validate_date_format

        assert validate_date_format("2020-01-01") is True
        assert validate_date_format("2020-12-31") is True
        assert validate_date_format("1999-06-15") is True

    @pytest.mark.unit
    def test_validate_date_format_invalid(self) -> None:
        """Test date format validation with invalid dates."""
        from api.app import validate_date_format

        # Wrong format (no dashes)
        assert validate_date_format("20200101") is False
        assert validate_date_format("2020-13-01") is False  # Invalid month
        assert validate_date_format("2020-01-32") is False  # Invalid day
        assert validate_date_format("invalid") is False
        assert validate_date_format("") is False
        assert validate_date_format("2020-01") is False  # Missing day
        assert validate_date_format(None) is False  # None value

        # These should now be valid (flexible format)
        assert validate_date_format("2020-1-1") is True  # Flexible format
        assert validate_date_format("2020-01-01") is True  # Standard format

    @pytest.mark.unit
    def test_apply_pagination(self) -> None:
        """Test pagination query building."""
        from api.app import apply_pagination

        query = "SELECT * FROM weather_records"

        # Test first page
        result = apply_pagination(query, 1, 10)
        assert result == "SELECT * FROM weather_records LIMIT 10 OFFSET 0"

        # Test second page
        result = apply_pagination(query, 2, 10)
        assert result == "SELECT * FROM weather_records LIMIT 10 OFFSET 10"

        # Test custom page size
        result = apply_pagination(query, 3, 25)
        assert result == "SELECT * FROM weather_records LIMIT 25 OFFSET 50"

    @pytest.mark.unit
    def test_build_where_clause(self) -> None:
        """Test WHERE clause building."""
        from api.app import build_where_clause

        # Test with conditions
        conditions = ["station_id = ?", "date = ?"]
        params = ["USC00110072", "20200101"]
        where_clause, final_params = build_where_clause(conditions, params)

        assert where_clause == " WHERE station_id = ? AND date = ?"
        assert final_params == ["USC00110072", "20200101"]

        # Test without conditions
        where_clause, final_params = build_where_clause([], [])
        assert where_clause == ""
        assert final_params == []

    @pytest.mark.unit
    def test_get_db_connection(self) -> None:
        """Test database connection function."""
        from api.app import get_db_connection

        # Test successful connection
        conn = get_db_connection()
        assert conn is not None
        conn.close()

        # Test connection error
        with patch("sqlite3.connect") as mock_connect:
            mock_connect.side_effect = sqlite3.Error("Connection failed")
            with pytest.raises(sqlite3.Error):
                get_db_connection()

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_weather_endpoint_basic(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test basic weather endpoint functionality.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "data" in data
        assert "pagination" in data
        assert "query_time" in data
        assert isinstance(data["data"], list)
        assert isinstance(data["pagination"], dict)
        assert len(data["data"]) == 4  # All test records

        # Check pagination structure
        pagination = data["pagination"]
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_weather_endpoint_with_filters(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather endpoint with filtering.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test station_id filter
        response = client.get("/api/weather/?station_id=USC00110072")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 2  # Only USC00110072 records
        for record in data["data"]:
            assert record["station_id"] == "USC00110072"

        # Test date filter
        response = client.get("/api/weather/?date=2020-01-01")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 2  # Only records from 2020-01-01
        for record in data["data"]:
            assert record["date"] == "2020-01-01"

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_weather_endpoint_pagination(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather endpoint pagination.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test first page
        response = client.get("/api/weather/?page=1&pageSize=2")
        assert response.status_code == 200

        data = json.loads(response.data)
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["pageSize"] == 2
        assert len(data["data"]) == 2
        assert pagination["totalRecords"] == 4
        assert pagination["totalPages"] == 2
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

        # Test second page
        response = client.get("/api/weather/?page=2&pageSize=2")
        assert response.status_code == 200

        data = json.loads(response.data)
        pagination = data["pagination"]
        assert pagination["page"] == 2
        assert len(data["data"]) == 2
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_weather_endpoint_invalid_date(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather endpoint with invalid date format.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/?date=20200101")
        assert response.status_code == 400

        data = json.loads(response.data)
        assert "message" in data
        assert "Invalid date format" in data["message"]

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_weather_endpoint_invalid_pagination(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather endpoint with invalid pagination parameters.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test negative page
        response = client.get("/api/weather/?page=-1")
        assert response.status_code == 200  # Should default to page 1

        # Test zero page
        response = client.get("/api/weather/?page=0")
        assert response.status_code == 200  # Should default to page 1

        # Test very large pageSize
        response = client.get("/api/weather/?pageSize=10000")
        assert response.status_code == 200  # Should cap at 1000

        data = json.loads(response.data)
        pagination = data["pagination"]
        assert pagination["pageSize"] == 1000
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalRecords",
            "totalPages",
        ]
        for field in required_pagination_fields:
            assert field in pagination

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_stats_endpoint_basic(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test basic weather statistics endpoint functionality.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/stats")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "data" in data
        assert "pagination" in data
        assert "query_time" in data
        assert isinstance(data["data"], list)
        assert isinstance(data["pagination"], dict)
        assert len(data["data"]) == 2  # All test stats records

        # Check pagination structure
        pagination = data["pagination"]
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_stats_endpoint_with_filters(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather statistics endpoint with filtering.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test station_id filter
        response = client.get("/api/weather/stats?station_id=USC00110072")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 1  # Only USC00110072 stats
        assert data["data"][0]["station_id"] == "USC00110072"

        # Test year filter
        response = client.get("/api/weather/stats?year=2020")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 2  # Both stations for 2020
        for record in data["data"]:
            assert record["year"] == 2020

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_stats_endpoint_invalid_year(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather statistics endpoint with invalid year.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test year too early
        response = client.get("/api/weather/stats?year=1700")
        assert response.status_code == 400

        data = json.loads(response.data)
        assert "message" in data
        assert "Invalid year" in data["message"]

        # Test year too late
        response = client.get("/api/weather/stats?year=2200")
        assert response.status_code == 400

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_stats_endpoint_pagination(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather statistics endpoint pagination.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/stats?page=1&pageSize=1")
        assert response.status_code == 200

        data = json.loads(response.data)
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["pageSize"] == 1
        assert len(data["data"]) == 1
        assert pagination["totalRecords"] == 2
        assert pagination["totalPages"] == 2
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalRecords",
            "totalPages",
        ]
        for field in required_pagination_fields:
            assert field in pagination

    @pytest.mark.unit
    @patch("api.app.get_db_connection")
    def test_database_connection_error(
        self, mock_db_conn: MagicMock, client: FlaskClient
    ) -> None:
        """
        Test handling of database connection errors.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
        """
        # Mock database connection to raise an error
        mock_db_conn.side_effect = sqlite3.Error("Database connection failed")

        response = client.get("/api/weather")
        assert response.status_code == 500

        data = json.loads(response.data)
        assert "message" in data
        assert "Database error" in data["message"]

    @pytest.mark.unit
    @patch("api.app.get_db_connection")
    def test_response_structure_weather(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather endpoint response structure.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather")
        assert response.status_code == 200

        data = json.loads(response.data)

        # Check required fields
        assert "data" in data
        assert "pagination" in data
        assert "query_time" in data

        # Check pagination structure
        pagination = data["pagination"]
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

        # Check data structure if records exist
        if data["data"]:
            record = data["data"][0]
            required_record_fields = [
                "station_id",
                "date",
                "max_temp",
                "min_temp",
                "precipitation",
            ]
            for field in required_record_fields:
                assert field in record

    @pytest.mark.unit
    @patch("api.app.get_db_connection")
    def test_response_structure_stats(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test weather statistics endpoint response structure.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/stats")
        assert response.status_code == 200

        data = json.loads(response.data)

        # Check required fields
        assert "data" in data
        assert "pagination" in data
        assert "query_time" in data

        # Check pagination structure
        pagination = data["pagination"]
        required_pagination_fields = [
            "page",
            "pageSize",
            "totalPages",
            "totalRecords",
        ]
        for field in required_pagination_fields:
            assert field in pagination

        # Check data structure if records exist
        if data["data"]:
            record = data["data"][0]
            required_record_fields = [
                "station_id",
                "year",
                "avg_max_temp",
                "avg_min_temp",
                "total_precipitation",
            ]
            for field in required_record_fields:
                assert field in record

    @pytest.mark.slow
    @patch("api.app.get_db_connection")
    def test_large_dataset_performance(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test API performance with large datasets.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        import time

        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test weather endpoint performance
        start_time = time.time()
        response = client.get("/api/weather/?pageSize=1000")
        end_time = time.time()

        assert response.status_code == 200
        assert end_time - start_time < 5.0  # Should respond within 5 seconds

        # Test stats endpoint performance
        start_time = time.time()
        response = client.get("/api/weather/stats?pageSize=1000")
        end_time = time.time()

        assert response.status_code == 200
        assert end_time - start_time < 3.0  # Should respond within 3 seconds

    @pytest.mark.unit
    @patch("api.app.get_db_connection")
    def test_cors_headers(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test CORS headers for cross-origin requests.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/")
        assert response.status_code == 200

        # Check for CORS headers (if implemented)
        # This test can be expanded when CORS is added
        assert (
            "Access-Control-Allow-Origin" not in response.headers
        )  # Not implemented yet

    @pytest.mark.unit
    @patch("api.app.get_db_connection")
    def test_content_type_headers(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test content type headers.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/")
        assert response.status_code == 200
        assert response.content_type == "application/json"

        response = client.get("/api/weather/stats")
        assert response.status_code == 200
        assert response.content_type == "application/json"

        response = client.get("/health")
        assert response.status_code == 200
        assert response.content_type == "application/json"

    @pytest.mark.unit
    def test_pagination_metadata_calculation(self) -> None:
        """Test pagination metadata calculation logic."""
        # Test edge cases for pagination calculation
        total = 100
        per_page = 10

        # Test first page
        page = 1
        total_pages = (total + per_page - 1) // per_page
        has_next = page < total_pages
        has_prev = page > 1

        assert total_pages == 10
        assert has_next is True
        assert has_prev is False

        # Test last page
        page = 10
        has_next = page < total_pages
        has_prev = page > 1

        assert has_next is False
        assert has_prev is True

        # Test middle page
        page = 5
        has_next = page < total_pages
        has_prev = page > 1

        assert has_next is True
        assert has_prev is True

    @pytest.mark.unit
    def test_error_handling_edge_cases(self) -> None:
        """Test error handling for edge cases."""
        from api.app import apply_pagination, build_where_clause, validate_date_format

        # Test empty date validation
        assert validate_date_format("") is False
        assert validate_date_format(None) is False  # type: ignore

        # Test pagination with edge values
        query = "SELECT * FROM table"
        assert apply_pagination(query, 1, 1) == "SELECT * FROM table LIMIT 1 OFFSET 0"
        assert apply_pagination(query, 1, 0) == "SELECT * FROM table LIMIT 0 OFFSET 0"

        # Test WHERE clause with empty conditions
        where_clause, params = build_where_clause([], [])
        assert where_clause == ""
        assert params == []


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def client(self) -> FlaskClient:
        """Create a test client."""
        app.config["TESTING"] = True
        with app.test_client() as client:
            yield client

    @pytest.fixture
    def test_db(self) -> str:
        """Create a temporary test database."""
        db_fd, db_path = tempfile.mkstemp()

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Create tables
            cursor.execute(
                """
                CREATE TABLE weather_records (
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
                CREATE TABLE annual_weather_stats (
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

        yield db_path

        os.close(db_fd)
        os.unlink(db_path)

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_empty_database_response(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test API response when database is empty.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/?station_id=NONEXISTENT")
        assert response.status_code == 200

        data = json.loads(response.data)
        pagination = data["pagination"]
        assert pagination["totalRecords"] == 0
        assert pagination["page"] == 1
        assert pagination["pageSize"] >= 1
        assert pagination["totalPages"] == 0 or pagination["totalPages"] == 1
        assert len(data["data"]) == 0

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_special_characters_in_parameters(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test API with special characters in parameters.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test with special characters in station_id
        response = client.get("/api/weather/?station_id=USC00110072'")
        assert response.status_code == 400  # Should reject invalid station_id format
        data = json.loads(response.data)
        assert "message" in data
        assert "Invalid station_id format" in data["message"]

        # Test with special characters in date
        response = client.get("/api/weather/?date=2020-01-01'")
        assert response.status_code == 400  # Should reject invalid date
        data = json.loads(response.data)
        assert "message" in data
        assert "Invalid date format" in data["message"]

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_very_large_page_numbers(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test API with very large page numbers.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        response = client.get("/api/weather/?page=999999")
        assert response.status_code == 200

        data = json.loads(response.data)
        # Should return empty data for non-existent page
        assert len(data["data"]) == 0

    @pytest.mark.integration
    @patch("api.app.get_db_connection")
    def test_malformed_query_parameters(
        self, mock_db_conn: MagicMock, client: FlaskClient, test_db: str
    ) -> None:
        """
        Test API with malformed query parameters.

        Args:
            mock_db_conn: Mock database connection
            client: Flask test client
            test_db: Path to test database
        """
        mock_db_conn.return_value = sqlite3.connect(test_db)

        # Test with non-numeric page
        response = client.get("/api/weather/?page=abc")
        assert (
            response.status_code == 400
        )  # Flask-RESTX returns 400 for invalid query params

        # Test with non-numeric pageSize
        response = client.get("/api/weather/?pageSize=xyz")
        assert (
            response.status_code == 400
        )  # Flask-RESTX returns 400 for invalid query params

    @pytest.mark.unit
    def test_sql_injection_prevention(self) -> None:
        """Test SQL injection prevention."""
        from api.app import build_where_clause

        # Test that parameters are properly escaped
        conditions = ["station_id = ?", "date = ?"]
        params = ["'; DROP TABLE weather_records; --", "20200101"]
        where_clause, final_params = build_where_clause(conditions, params)

        # Should use parameterized queries
        assert "?" in where_clause
        assert final_params == ["'; DROP TABLE weather_records; --", "20200101"]

    @pytest.mark.unit
    def test_missing_database_file(self) -> None:
        """Test behavior when database file doesn't exist."""
        from api.app import get_db_connection

        # This should create a new database file
        conn = get_db_connection()
        assert conn is not None
        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
