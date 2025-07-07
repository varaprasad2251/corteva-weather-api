# Makefile for Weather Data API

# Installation
install:
	pip install -r requirements.txt

# Code formatting
format:
	@echo "Formatting code with black..."
	black api/ tests/ *.py
	@echo "Sorting imports with isort..."
	isort api/ tests/ *.py
	@echo "Code formatting complete!"

lint:
	@echo "Checking flake8..."
	@flake8 api/ tests/ *.py --max-line-length=88 --extend-ignore=E203,W503 > .flake8.log 2>&1; \
	if [ $$? -eq 0 ]; then \
		echo "flake8: PASSED"; \
	else \
		echo "flake8: FAILED"; \
		cat .flake8.log; \
	fi; \
	rm -f .flake8.log

# Testing
test:
	@echo "Running all tests..."
	pytest tests/ -v

test-cov:
	@echo "Running tests with coverage..."
	pytest tests/ -v --cov=api --cov-report=term-missing --cov-report=html

test-unit:
	@echo "Running unit tests..."
	pytest tests/ -v -m "unit"

test-integration:
	@echo "Running integration tests..."
	pytest tests/ -v -m "integration"

# Database operations
ingest: setup-db
	@echo "Running data ingestion..."
	python data_ingestion.py

ingest-file:
	@echo "Usage: make ingest-file FILE=/path/to/file.txt"
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE parameter is required"; \
		echo "Example: make ingest-file FILE=data/wx_data/USC00110072.txt"; \
		exit 1; \
	fi
	@echo "Running data ingestion for file: $(FILE)"
	python data_ingestion.py $(FILE)

ingest-dir:
	@echo "Usage: make ingest-dir DIR=/path/to/directory"
	@if [ -z "$(DIR)" ]; then \
		echo "Error: DIR parameter is required"; \
		echo "Example: make ingest-dir DIR=data/wx_data"; \
		exit 1; \
	fi
	@echo "Running data ingestion for directory: $(DIR)"
	python data_ingestion.py $(DIR)

analyze: setup-db
	@echo "Running data analysis..."
	python data_analysis.py

# API server
run:
	@echo "Starting Flask development server..."
	cd api && python app.py

# Cleanup
clean:
	@echo "Cleaning generated files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf docs/_build/
	@echo "Cleanup complete!"

clean-db:
	@echo "Removing database files..."
	rm -f db/weather_data.db
	rm -f logs/*.log
	@echo "Database cleanup complete!"

# Quality checks
quality: format lint type-check test
	@echo "All quality checks passed!"

# Pre-commit setup
pre-commit:
	@echo "Installing pre-commit hooks..."
	pre-commit install
	@echo "Pre-commit hooks installed!"

# Database setup
setup-db:
	@echo "Setting up the weather database schema..."
	python db_utils.py --db-path db/weather_data.db --schema-path weather_schema.sql
	@echo "Database setup complete!"