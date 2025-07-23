# ETL Script

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3670A0?logo=python&logoColor=ffdd54"/>
  <img alt="R" src="https://img.shields.io/badge/R-276DC3?logo=r&logoColor=white"/>
  <img alt="Docker" src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white"/>
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white"/>
</p>

A collection of scripts for performing an ETL (Extract, Transform, Load) workflow on Synthea generated data and loading the results into an OMOP Common Data Model database.

## Features

- Written in **Python** with support from **R** via `rpy2`.
- Loads data into a **PostgreSQL** database using OHDSI tools.
- Can be executed locally or inside **Docker** using `docker-compose`.
- Includes utilities to generate CSVs, DDL statements and mapping files.

## Prerequisites

- Python 3.10+
- R (for OHDSI packages)
- Docker & Docker Compose (optional for containerised setup)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Install the required R packages:

```bash
Rscript install.R
```

## Configuration

Create a `.env` file in the project root with the following variables:

```dotenv
DB_TYPE=postgresql
DB_SERVER=localhost
DB_PORT=5432
DB_NAME=ohdsi_tutorial
DB_USER=postgres
DB_PASSWORD=postgres
DB_SCHEMA=public
DRIVER_PATH=/path/to/jdbc/driver
FILE_PATH=/path/to/synthea/csvs
MAPPER_CLASS=SyntheaETLPipeline
```

Additional environment variables are used for optional tasks such as generating CSVs or vocabulary loading (see `main.py` for details).

## Running the ETL

To run the main ETL pipeline:

```bash
python main.py
```

This will process the files defined in the selected mapper class and load them into the configured database.

### Docker

A `docker-compose.yml` file is provided to start a PostgreSQL instance preconfigured for the ETL. Run the following to start the service:

```bash
docker compose up -d
```

The database will be accessible on port `5452` by default.

## Directory Structure

```
├── mappers/          # Pipeline definitions
├── scripts/          # ETL, loader and utility scripts
├── requirements.txt  # Python dependencies
├── install.R         # R package installation script
└── docker-compose.yml
```

## License

This project is provided as-is under the MIT license.
