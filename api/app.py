#!/usr/bin/env python3
"""
Problem 4: Flask REST API

This module provides a comprehensive REST API for weather data with the following
features:
- GET /api/weather - Query weather records with filtering and pagination
- GET /api/weather/stats - Query annual weather statistics with filtering and pagination
- OpenAPI/Swagger documentation at /api/docs
- Comprehensive error handling and logging
- Type hints for better code maintainability

The API supports:
- Filtering by station_id, date, and year
- Pagination with configurable page size
- Detailed response metadata
- Proper HTTP status codes
- Input validation
"""

import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

from flask import Flask
from flask_restx import Api, Resource, fields

# Configure logging with detailed formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Initialize Flask app with configuration
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False  # Preserve field order in JSON responses
# Pretty print JSON in development
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

# Initialize Flask-RESTX API with comprehensive OpenAPI documentation
api = Api(
    app,
    version="1.0",
    title="Weather Data API",
    description=(
        "REST API for querying weather records and annual statistics. "
        "Supports filtering, pagination, and comprehensive data analysis."
    ),
    doc="/api/docs",
    default="api",
    default_label="Weather Data API Endpoints",
)

# Define namespace with correct path
weather_ns = api.namespace("api/weather", description="Weather data operations")

# Define comprehensive models for OpenAPI documentation
weather_model = api.model(
    "WeatherRecord",
    {
        "station_id": fields.String(
            required=True, description="Weather station ID (e.g., 'USC00110072')"
        ),
        "date": fields.String(
            required=True, description="Date in ISO 8601 format (e.g., '2020-01-01')"
        ),
        "max_temp": fields.Integer(
            description=(
                "Maximum temperature in tenths of Celsius (e.g., 250 = 25.0°C)"
            )
        ),
        "min_temp": fields.Integer(
            description=(
                "Minimum temperature in tenths of Celsius (e.g., 100 = 10.0°C)"
            )
        ),
        "precipitation": fields.Integer(
            description=("Precipitation in tenths of millimeters " "(e.g., 50 = 5.0mm)")
        ),
    },
)

weather_stats_model = api.model(
    "WeatherStats",
    {
        "station_id": fields.String(
            required=True, description="Weather station ID (e.g., 'USC00110072')"
        ),
        "year": fields.Integer(required=True, description="Year (e.g., 2020)"),
        "avg_max_temp": fields.Float(
            description="Average maximum temperature in degrees Celsius"
        ),
        "avg_min_temp": fields.Float(
            description="Average minimum temperature in degrees Celsius"
        ),
        "total_precipitation": fields.Float(
            description="Total precipitation in centimeters"
        ),
    },
)

pagination_model = api.model(
    "Pagination",
    {
        "page": fields.Integer(description="Current page number (1-based)"),
        "pageSize": fields.Integer(description="Number of records per page"),
        "totalPages": fields.Integer(description="Total number of pages"),
        "totalRecords": fields.Integer(
            description="Total number of records matching query"
        ),
    },
)

weather_response_model = api.model(
    "WeatherResponse",
    {
        "data": fields.List(fields.Nested(weather_model)),
        "pagination": fields.Nested(pagination_model),
        "query_time": fields.String(description="Timestamp of the query"),
    },
)

stats_response_model = api.model(
    "StatsResponse",
    {
        "data": fields.List(fields.Nested(weather_stats_model)),
        "pagination": fields.Nested(pagination_model),
        "query_time": fields.String(description="Timestamp of the query"),
    },
)

# Define query parameter models for input validation
weather_query_params = api.parser()
weather_query_params.add_argument(
    "station_id", type=str, help="Filter by station ID (e.g., 'USC00110072')"
)
weather_query_params.add_argument(
    "date", type=str, help="Filter by date in ISO 8601 format (e.g., '2020-01-01')"
)
weather_query_params.add_argument(
    "page", type=int, default=1, help="Page number (default: 1, min: 1)"
)
weather_query_params.add_argument(
    "pageSize",
    type=int,
    default=100,
    help="Records per page (default: 100, min: 1, max: 1000)",
)

stats_query_params = api.parser()
stats_query_params.add_argument(
    "station_id", type=str, help="Filter by station ID (e.g., 'USC00110072')"
)
stats_query_params.add_argument("year", type=int, help="Filter by year (e.g., 2020)")
stats_query_params.add_argument(
    "page", type=int, default=1, help="Page number (default: 1, min: 1)"
)
stats_query_params.add_argument(
    "pageSize",
    type=int,
    default=100,
    help="Records per page (default: 100, min: 1, max: 1000)",
)


def get_db_connection() -> sqlite3.Connection:
    """
    Get SQLite database connection.

    Returns:
        sqlite3.Connection: Database connection object

    Raises:
        sqlite3.Error: If database connection fails
    """
    try:
        db_path = os.path.join(os.path.dirname(__file__), "..", "db", "weather_data.db")
        db_path = os.path.abspath(db_path)
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        logger.error("Database connection failed: %s", e)
        raise


def apply_pagination(query: str, page: int, page_size: int) -> str:
    """
    Apply pagination to SQL query.

    Args:
        query: Base SQL query string
        page: Page number (1-based)
        page_size: Number of records per page

    Returns:
        str: SQL query with LIMIT and OFFSET clauses

    Example:
        >>> apply_pagination("SELECT * FROM table", 2, 10)
        "SELECT * FROM table LIMIT 10 OFFSET 10"
    """
    offset = (page - 1) * page_size
    return f"{query} LIMIT {page_size} OFFSET {offset}"


def validate_date_format(date_str: str) -> bool:
    """Validate date string format (ISO 8601: YYYY-MM-DD) with flexible input"""
    if date_str is None or not isinstance(date_str, str) or not date_str:
        return False

    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Additional year range validation
        if parsed_date.year < 1800 or parsed_date.year > 2100:
            return False
        return True
    except ValueError:
        return False


def build_where_clause(
    conditions: List[str], params: List[Any]
) -> Tuple[str, List[Any]]:
    """
    Build SQL WHERE clause from conditions and parameters.

    Args:
        conditions: List of SQL condition strings
        params: List of parameter values

    Returns:
        Tuple[str, List[Any]]: WHERE clause string and final parameters

    Example:
        >>> build_where_clause(["station_id = ?", "date = ?"],
        ...                    ["USC00110072", "20200101"])
        (" WHERE station_id = ? AND date = ?", ["USC00110072", "20200101"])
    """
    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    return where_clause, params


def validate_station_id(station_id: str) -> bool:
    """
    Validate station ID format.

    Args:
        station_id: Station ID string to validate

    Returns:
        bool: True if valid format, False otherwise
    """
    if not station_id or not isinstance(station_id, str):
        return False
    # Station ID should be alphanumeric and reasonable length
    if len(station_id) < 3 or len(station_id) > 20:
        return False
    # Allow alphanumeric characters and common separators
    return station_id.replace("-", "").replace("_", "").isalnum()


def validate_weather_args(args):
    """Comprehensive validation for weather endpoint arguments."""
    # Validate pagination
    page = max(1, args.get("page", 1))
    page_size = min(1000, max(1, args.get("pageSize", 100)))

    # Validate station_id if provided
    station_id = args.get("station_id")
    if station_id is not None and not validate_station_id(station_id):
        logger.warning("Invalid station_id format: %s", station_id)
        api.abort(400, "Invalid station_id format. Use alphanumeric characters only.")

    # Validate date if provided
    date = args.get("date")
    if date is not None and not validate_date_format(date):
        logger.warning("Invalid date format: %s", date)
        api.abort(400, "Invalid date format. Use YYYY-MM-DD format.")

    return page, page_size, station_id, date


def validate_stats_args(args):
    """Comprehensive validation for stats endpoint arguments."""
    # Validate pagination
    page = max(1, args.get("page", 1))
    page_size = min(1000, max(1, args.get("pageSize", 100)))

    # Validate station_id if provided
    station_id = args.get("station_id")
    if station_id is not None and not validate_station_id(station_id):
        logger.warning("Invalid station_id format: %s", station_id)
        api.abort(400, "Invalid station_id format. Use alphanumeric characters only.")

    # Validate year if provided
    year = args.get("year")
    if year is not None and (year < 1800 or year > 2100):
        logger.warning("Invalid year: %d", year)
        api.abort(400, "Invalid year. Must be between 1800 and 2100.")

    return page, page_size, station_id, year


@weather_ns.route("/", endpoint="weather_list", strict_slashes=False)
class WeatherList(Resource):
    """
    Weather records endpoint.

    Provides access to individual weather records with filtering and pagination.
    Supports filtering by station_id and date, with comprehensive pagination.
    """

    def _validate_weather_args(self, args):
        """Helper to validate and extract weather query args."""
        page, page_size, station_id, date = validate_weather_args(args)
        return page, page_size, station_id, date

    def _build_weather_query(self, args):
        """Helper to build weather SQL queries and params."""
        where_conditions = []
        params = []
        if args.get("station_id"):
            where_conditions.append("station_id = ?")
            params.append(args["station_id"])
        if args.get("date"):
            where_conditions.append("DATE(date) = DATE(?)")
            params.append(args["date"])
        where_clause, params = build_where_clause(where_conditions, params)
        count_query = f"SELECT COUNT(*) FROM weather_records{where_clause}"
        data_query = f"""
            SELECT station_id, date, max_temp, min_temp, precipitation
            FROM weather_records{where_clause}
            ORDER BY station_id, date
        """
        return count_query, data_query, params

    @weather_ns.doc("get_weather_records")
    @weather_ns.expect(weather_query_params)
    @weather_ns.marshal_with(weather_response_model)
    @weather_ns.response(400, "Bad Request - Invalid parameters")
    @weather_ns.response(500, "Internal Server Error")
    def get(self) -> Union[Dict[str, Any], Tuple[Dict[str, str], int]]:
        """
        Get weather records with optional filtering and pagination.
        """
        try:
            args = weather_query_params.parse_args()
            page, page_size, station_id, date = self._validate_weather_args(args)
            count_query, data_query, params = self._build_weather_query(args)
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(count_query, params)
                total = cursor.fetchone()[0]
                paginated_query = apply_pagination(data_query, page, page_size)
                cursor.execute(paginated_query, params)
                records = [
                    {
                        "station_id": row[0],
                        "date": row[1],
                        "max_temp": row[2],
                        "min_temp": row[3],
                        "precipitation": row[4],
                    }
                    for row in cursor.fetchall()
                ]
                total_pages = (total + page_size - 1) // page_size
                logger.info(
                    "Weather records query: %d records returned "
                    "(page %d/%d, total: %d)",
                    len(records),
                    page,
                    total_pages,
                    total,
                )
                return {
                    "data": records,
                    "pagination": {
                        "page": page,
                        "pageSize": page_size,
                        "totalPages": total_pages,
                        "totalRecords": total,
                    },
                    "query_time": datetime.now().isoformat(),
                }
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            logger.error("Database error: %s", e)
            api.abort(500, "Database error")


@weather_ns.route("/stats", endpoint="weather_stats", strict_slashes=False)
class WeatherStats(Resource):
    """
    Annual weather statistics endpoint.

    Provides access to pre-calculated annual weather statistics with filtering
    and pagination. Statistics include averages and totals per station and year.
    """

    def _validate_stats_args(self, args):
        """Helper to validate and extract stats query args."""
        page, page_size, station_id, year = validate_stats_args(args)
        return page, page_size, station_id, year

    def _build_stats_query(self, args):
        """Helper to build stats SQL queries and params."""
        where_conditions = []
        params = []
        if args.get("station_id"):
            where_conditions.append("station_id = ?")
            params.append(args["station_id"])
        if args.get("year"):
            where_conditions.append("year = ?")
            params.append(args["year"])
        where_clause, params = build_where_clause(where_conditions, params)
        count_query = f"SELECT COUNT(*) FROM annual_weather_stats{where_clause}"
        data_query = f"""
            SELECT station_id, year, avg_max_temp, avg_min_temp, total_precipitation
            FROM annual_weather_stats{where_clause}
            ORDER BY station_id, year
        """
        return count_query, data_query, params

    @weather_ns.doc("get_weather_stats")
    @weather_ns.expect(stats_query_params)
    @weather_ns.marshal_with(stats_response_model)
    @weather_ns.response(400, "Bad Request - Invalid parameters")
    @weather_ns.response(500, "Internal Server Error")
    def get(self) -> Union[Dict[str, Any], Tuple[Dict[str, str], int]]:
        """
        Get annual weather statistics with optional filtering and pagination.
        """
        try:
            args = stats_query_params.parse_args()
            page, page_size, station_id, year = self._validate_stats_args(args)
            count_query, data_query, params = self._build_stats_query(args)
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(count_query, params)
                total = cursor.fetchone()[0]
                paginated_query = apply_pagination(data_query, page, page_size)
                cursor.execute(paginated_query, params)
                records = [
                    {
                        "station_id": row[0],
                        "year": row[1],
                        "avg_max_temp": row[2],
                        "avg_min_temp": row[3],
                        "total_precipitation": row[4],
                    }
                    for row in cursor.fetchall()
                ]
                total_pages = (total + page_size - 1) // page_size
                logger.info(
                    "Weather stats query: %d records returned "
                    "(page %d/%d, total: %d)",
                    len(records),
                    page,
                    total_pages,
                    total,
                )
                return {
                    "data": records,
                    "pagination": {
                        "page": page,
                        "pageSize": page_size,
                        "totalPages": total_pages,
                        "totalRecords": total,
                    },
                    "query_time": datetime.now().isoformat(),
                }
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            logger.error("Database error: %s", e)
            api.abort(500, "Database error")


@app.route("/health", methods=["GET"])
def health_check() -> Dict[str, str]:
    """
    Health check endpoint.

    Returns:
        Dict[str, str]: Health status information
    """
    return {
        "status": "healthy",
        "service": "weather-data-api",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
    }


# Add error handlers for consistent JSON error responses
@app.errorhandler(sqlite3.Error)
def handle_database_error(error):
    """Handle database errors and return JSON response."""
    logger.error("Database error: %s", error)
    return {"message": "Database error"}, 500


@app.errorhandler(Exception)
def handle_generic_error(error):
    """Handle generic errors and return JSON response."""
    logger.error("Unexpected error: %s", error)
    return {"message": "Internal server error"}, 500


if __name__ == "__main__":
    logger.info("Starting Weather Data API server...")
    app.run(host="0.0.0.0", port=5001, debug=True)
