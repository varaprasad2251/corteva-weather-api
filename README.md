# Weather Data API

A comprehensive weather data processing and API system that ingests weather data from text files, performs statistical analysis, and exposes the data through a REST API with filtering and pagination capabilities.

## Project Description

This project processes weather data records from weather stations across Nebraska, Iowa, Illinois, Indiana, and Ohio, covering the period from 1985-01-01 to 2014-12-31. Each weather record contains:

- Date (YYYYMMDD format)
- Maximum temperature (in tenths of a degree Celsius)
- Minimum temperature (in tenths of a degree Celsius) 
- Precipitation (in tenths of a millimeter)

The system provides:
- Data ingestion with duplicate detection
- Annual statistical analysis
- REST API with filtering and pagination
- Comprehensive test coverage
- Swagger documentation


### Project Structure
```
corteva-weather-api/
├── api/                    # Flask API application
│   ├── app.py             # Main API application
│   └── __init__.py
├── data_ingestion.py      # Weather data ingestion
├── data_analysis.py       # Stats calculation
├── tests/                 # Test suite
├── db/                    # Database files
├── data/                  # Raw data
├── Makefile               # Build and run commands
├── requirements.txt       # Dependencies file
├── db_utils.py
├── logging_utils.py
└── README.md              
```

### Dependencies
- **Flask-RESTX**: API framework with Swagger support
- **SQLite**: Database (can be easily migrated to PostgreSQL)
- **pytest**: Testing framework
- **flake8**: Code quality tools

## Installation

### Prerequisites
- Python 3.8+
- pipenv

### Getting Started
```bash
# Clone the repository
git clone https://github.com/varaprasad2251/corteva-weather-api.git
cd corteva-weather-api

# Install pipenv
pip install pipenv

#Initialize Pipenv and create virtual environment
pipenv --python 3.10  # or  preferred version

#Install dependencies
pipenv install -r requirements.txt


#Activate the virtual environment
pipenv shell

#Change to project directory
cd corteva-weather-api
```

### Makefile Commands

To make development easier, I’ve added a Makefile with the most common commands:

| Command        | Description                                 |
|----------------|---------------------------------------------|
| `make lint`    | Run code linting with flake8                |
| `make test`    | Run test suite using pytest                 |
| `make format`  | Auto-format code using black                |
| `make ingest`  | Set up DB and run full data ingestion       |
| `make analyze` | Run data analysis after ingestion           |
| `make run`     | Run the Flask application locally           |
| `make clean`   | Remove temporary files or caches            |
| `make clean-db`| Remove db files                             |

### Downloading Weather Data

To run the ingestion locally, you must download the real weather data from the provided code challenge repository.

```bash
#Clone the Source Data Repository
git clone https://github.com/corteva/code-challenge-template.git

#Ensure you're inside your main project directory
mkdir -p data/wx_data
cp -r ../code-challenge-template/data/wx_data/* data/wx_data/
```

### Running the App
```bash
# Start the API server
make run
```

The API will be available at:
- API Documentation: http://localhost:5001/api/docs
- Weather Records: http://localhost:5001/api/weather
- Annual Statistics: http://localhost:5001/api/stats

---

## Problem 1 - Data Modeling

### Solution
I chose SQLite for this project due to its simplicity and suitability for this scale of data. The database schema includes two main tables:

**weather_records table:**
```sql
CREATE TABLE IF NOT EXISTS weather_records (
    station_id TEXT NOT NULL,
    date TEXT NOT NULL CHECK (
        date LIKE '____-__-__' 
        AND date(date) IS NOT NULL
        AND CAST(substr(date, 1, 4) AS INTEGER) BETWEEN 1800 AND 2100
    ), -- ISO 8601 format (YYYY-MM-DD)
    max_temp INTEGER,   -- tenths of a degree Celsius
    min_temp INTEGER,   -- tenths of a degree Celsius
    precipitation INTEGER, -- tenths of a millimeter
    PRIMARY KEY (station_id, date) -- To ensure unique entry
);
```

**annual_weather_stats table:**
```sql
CREATE TABLE IF NOT EXISTS annual_weather_stats (
    station_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    avg_max_temp REAL,         -- Average max temperature in degrees Celsius
    avg_min_temp REAL,         -- Average min temperature in degrees Celsius
    total_precipitation REAL,  -- Total precipitation in centimeters
    PRIMARY KEY (station_id, year)
);
```

### Key Design Decisions:
- **Date format**: Stored as ISO 8601 (YYYY-MM-DD) for consistency and validation
- **Temperature units**: Stored in tenths of degrees Celsius as provided
- **Precipitation units**: Stored in tenths of millimeters as provided
- **Constraints**: Added CHECK constraint for date validation and PRIMARY KEY constraint to prevent duplicates
- **Indexing**: Primary key is a combination of station_id and date (or year), which enables efficient indexing on these columns.

### How to Run
The schema is automatically created when you run the ingestion or analysis scripts. You can also create it manually:

```bash
# Create schema manually
make setup-db
```

---

## Problem 2 - Ingestion

### Solution
The ingestion process (`data_ingestion.py`) provides:

- **File processing**: Handles both single files and directories
- **Duplicate detection**: Uses database constraints to prevent duplicate records
- **Data validation**: Validates date formats and numeric values
- **Logging**: logging of ingestion progress and summary of the ingestion process

### Features:
- Converts dates from YYYYMMDD to ISO 8601 format
- Skips invalid records and logs them
- Provides detailed statistics (records processed, inserted, skipped)
- Handles missing values (-9999) appropriately

### How to Run
```bash
# Ingest data from all files in the directory data/wx_data
make ingest

# Ingest all data from a specified directory
make ingest-dir DIR=data/wx_data

# Ingest a specific file
make ingest-file FILE=data/wx_data/USC00110072.txt
```

### Example Output:
```
Starting weather data ingestion from: data/wx_data
Database: db/weather_data.db
================================================================================
2025-07-07 07:09:16 - INFO - WEATHER DATA INGESTION SUMMARY
2025-07-07 07:09:16 - INFO - ================================================================================
2025-07-07 07:09:16 - INFO - Start Time: 2025-07-07 07:09:06.766482
2025-07-07 07:09:16 - INFO - End Time: 2025-07-07 07:09:16.766278
2025-07-07 07:09:16 - INFO - Total Duration: 10.00 seconds
2025-07-07 07:09:16 - INFO - Files Processed: 167
2025-07-07 07:09:16 - INFO - Files Successful: 167
2025-07-07 07:09:16 - INFO - Files Failed: 0
2025-07-07 07:09:16 - INFO - Total Records Processed: 1729957
2025-07-07 07:09:16 - INFO - Total Records Ingested: 1729957
2025-07-07 07:09:16 - INFO - Total Records Skipped: 0
2025-07-07 07:09:16 - INFO - Total Errors: 0
```

---

## Problem 3 - Data Analysis

### Solution
The analysis process (`data_analysis.py`) calculates annual statistics for each weather station and for each year:

- **Average maximum temperature** (converted to degrees Celsius)
- **Average minimum temperature** (converted to degrees Celsius)  
- **Total accumulated precipitation** (converted to centimeters)


Ignores missing data (-9999) in calculations

### SQL Query:
```sql
SELECT
    station_id,
    CAST(SUBSTR(date, 1, 4) AS INTEGER) as year,
    ROUND(AVG(CASE WHEN max_temp != -9999 THEN max_temp / 10.0 END), 2) as avg_max_temp,
    ROUND(AVG(CASE WHEN min_temp != -9999 THEN min_temp / 10.0 END), 2) as avg_min_temp,
    ROUND(SUM(CASE WHEN max_temp != -9999 AND min_temp != -9999 AND precipitation != -9999 
        THEN precipitation / 100.0 END), 2) as total_precipitation
FROM weather_records
GROUP BY station_id, year
ORDER BY station_id, year
```

### How to Run
```bash
# Run analysis on default database
make analyze
```

### Example Output:
```
Running data analysis...
2025-07-07 06:09:18 - INFO - Connected to database at db/weather_data.db
2025-07-07 06:09:18 - INFO - Starting data analysis workflow...
2025-07-07 06:09:18 - INFO - Calculating annual weather statistics...
2025-07-07 06:09:20 - INFO - Calculated stats for 4820 station-year pairs.
2025-07-07 06:09:20 - INFO - Storing annual statistics in the database...
2025-07-07 06:09:20 - INFO - Annual statistics stored successfully.
2025-07-07 06:09:20 - INFO - Data analysis workflow completed.
2025-07-07 06:09:20 - INFO - Database connection closed.
```

---

## Problem 4 - REST API

### Solution
Built with Flask-RESTX, the API provides:

- **Weather records endpoint**: `/api/weather` with filtering by date, station ID
- **Annual statistics endpoint**: `/api/stats` with filtering by year, station ID
- **Pagination**: can configure pageSize and supports filtering by page
- **Swagger documentation**: Automatic API documentation

### API Endpoints

#### GET /api/weather
Returns weather records

**Query Parameters:**
- `station_id`: Filter by weather station ID
- `date`: Filter by specific date (YYYY-MM-DD)
- `page`: Page number (default: 1)
- `pageSize`: Records per page (default: 100, max: 1000)

**Example Response:**
```json
{
  "data": [
    {
      "station_id": "USC00110072",
      "date": "2004-01-01",
      "max_temp": 28,
      "min_temp": -33,
      "precipitation": 0
    }
  ],
  "pagination": {
    "page": 1,
    "pageSize": 100,
    "totalPages": 1,
    "totalRecords": 1
  },
  "query_time": "2025-07-07T05:48:53.304862"
}
```

#### GET /api/stats
Returns annual weather statistics

**Query Parameters:**
- `station_id`: Filter by weather station ID
- `year`: Filter by specific year
- `page`: Page number (default: 1)
- `pageSize`: Records per page (default: 100, max: 1000)

**Example Response:**
```json
{
  "data": [
    {
      "station_id": "USC00110072",
      "year": 2014,
      "avg_max_temp": 13.8,
      "avg_min_temp": 2.37,
      "total_precipitation": 98.63
    }
  ],
  "pagination": {
    "page": 1,
    "pageSize": 100,
    "totalPages": 1,
    "totalRecords": 1
  },
  "query_time": "2025-07-07T05:49:29.757599"
}
```

### How to Run
```bash
# Start the API server
make run
```

Endpoints:
- **Weather Records**: http://localhost:5001/api/weather
- **Stats**: http://localhost:5001/api/stats

### Testing
```bash
# Run all tests
make test

# Run specific test categories
pipenv run pytest tests/test_api.py -v
pipenv run pytest tests/test_data_ingestion.py -v
pipenv run pytest tests/test_data_analysis.py -v
```

---

### Assumptions

Following are assumptions made while working on the project:
- **File Format**: All input weather data files are expected to be `.txt` files. During ingestion, only `.txt` files from given directory are processed.
- **Missing Values**: The value `-9999` is used to indicate missing data for temperature or precipitation. These are treated as `NULL` in the database to allow proper statistical calculations.
- **Date Range**: Only dates from the year **1800** up to but not including **2100** are considered valid. Records outside this range are skipped or rejected.
- **Date Format**: Dates in the source files are assumed to be in `YYYYMMDD` format and are converted to ISO 8601 format (`YYYY-MM-DD`) before storing in the database.
- **Station ID**: Each file represents a unique weather station, and the filename without extension is the station ID.
- **Idempotency**: Ingestion is designed to be idempotent. If the same files are ingested multiple times, duplicates are avoided using the `(station_id, date)` primary key constraint.
- **Units**:
  - Temperatures are provided in **tenths of degrees Celsius**, precipitation is in **tenths of millimeters**.
  - Conversion to degrees Celsius and centimeters happens only for stats analysis, not during ingestion.


## Extra Credit - Deployment

### AWS Deployment Strategy

The current implementation uses SQLite and local scripts, which works well for development and small-scale usage. However, for deploying this system to production in the cloud (AWS), I would adopt a more scalable and managed architecture. Here's how I would do it:


To make the deployment repeatable and maintainable:
- I would use **Terraform** to define infrastructure components like compute, networking, and databases.
- **GitHub Actions** can be used to create CI/CD workflow, which runs tests, builds Docker containers, and deploys to the cloud.

---

#### Hosting the Application

Instead of running the Flask app manually, I would containerize it using **Docker** and deploy it using **AWS ECS Fargate** which allow running containers without managing servers.
An **Application Load Balancer (ALB)** can be used to distribute incoming traffic and perform health checks, and also enable **Auto Scaling** based on CPU or memory to handle varying load.

This setup supports higher traffic and allows multiple instances of the API to run concurrently.

---

#### Database

While SQLite works locally, it is not suitable for concurrent access in production. So I would use **Amazon RDS** which postgreSQL for a scalable and fully managed relational database, **Amazon ElastiCache** to cache frequent API queries and reduce load on the database. I would also enable **automated backups** to ensure disaster recovery.

---

#### Data Storage & Processing

The current implementation reads weather files from local directory, but in the cloud, we can upload **weather data** to **Amazon S3** which offers durable object storage, use **AWS Lambda** to ingest data from S3 into RDS. **Amazon EventBridge** can be used to schedule daily or weekly ingestion jobs, making it easy to automate the data pipeline.

This removes the need to run ingestion manually and makes the system more reliable.

---

#### Security
Current implementation does not have any authentication enabled for accessing api endpoints, but when deployed on cloud we can use **AWS Secrets Manager** to store database credentials securely, assign **least privilege IAM roles** to services like Lambda, S3, and deploy everything within a private **VPC** to isolate internal resources.

---

#### Monitoring

For production visibility, we can use **Amazon CloudWatch** to collect logs and metrics from ECS, Lambda, and the database, set up **alarms** for ingestion failures, high latency, or API downtime.

---

#### CI/CD Pipeline

To automate deployment process, I would configure GitHub Actions to:
1. Run unit and integration tests on every push.
2. Build and tag Docker images.
3. Push images to **Amazon ECR**.
4. Deploy new versions to **AWS ECS**
5. Use **blue-green deployment** to ensure zero-downtime upgrades.


By moving to the cloud, we can ensure the following:
- **Scalability**: To easily handle large volumes of weather data and concurrent API requests.
- **Reliability**: Use managed services with automated failover, backups, and monitoring.
- **Automation**: Fully automated ingestion and deployment pipelines.
- **Security**: Isolated network, secrets management, and protection from web threats.

This architecture ensures the system is production-ready, easy to maintain, and future-proof for larger datasets and broader usage.

