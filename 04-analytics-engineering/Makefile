.PHONY: build download shell duckdb dbt-debug dbt-run dbt-test clean reset-load help

# Setup commands
download:
	@echo "📥 Downloading and converting data locally..."
	python download_data.py

build:
	@echo "🔨 Building Docker image..."
	docker-compose build

# Interactive shells
shell:
	@echo "💻 Opening bash shell in container..."
	docker-compose run --rm dbt bash

duckdb:
	@echo "🦆 Opening DuckDB CLI..."
	docker-compose run --rm dbt duckdb taxi_rides_ny.duckdb

# dbt commands
dbt-debug:
	@echo "🔍 Running dbt debug..."
	docker-compose run --rm dbt dbt debug

dbt-run:
	@echo "▶️  Running dbt models..."
	docker-compose run --rm dbt dbt run

dbt-test:
	@echo "🧪 Running dbt tests..."
	docker-compose run --rm dbt dbt test

dbt-docs:
	@echo "📚 Generating dbt documentation..."
	docker-compose run --rm dbt dbt docs generate

# Utility commands
reset-load:
	@echo "🔄 Resetting data load marker..."
	rm -f taxi_rides_ny/.data_loaded
	@echo "✅ Next container start will re-load data"

clean:
	@echo "🧹 Cleaning up Docker resources..."
	docker-compose down
	@echo "✅ Docker cleanup complete"

clean-all:
	@echo "🧹 Cleaning up everything..."
	docker-compose down
	rm -rf data/
	rm -f taxi_rides_ny/taxi_rides_ny.duckdb
	rm -f taxi_rides_ny/.data_loaded
	@echo "✅ Full cleanup complete"

# Help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Setup:"
	@echo "  make download    - Download and convert data locally (run FIRST)"
	@echo "  make build       - Build Docker image"
	@echo ""
	@echo "Development:"
	@echo "  make shell       - Open bash shell in container"
	@echo "  make duckdb      - Open DuckDB CLI"
	@echo "  make dbt-debug   - Test dbt connection"
	@echo "  make dbt-run     - Run dbt models"
	@echo "  make dbt-test    - Run dbt tests"
	@echo "  make dbt-docs    - Generate dbt documentation"
	@echo ""
	@echo "Utilities:"
	@echo "  make reset-load  - Force data re-load on next container start"
	@echo "  make clean       - Remove Docker containers"
	@echo "  make clean-all   - Remove everything (data + Docker)"
	@echo "  make help        - Show this help message"