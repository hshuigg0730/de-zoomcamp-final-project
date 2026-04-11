# de-zoomcamp-final-project

A final project for Data Engineering Zoomcamp 2026.

This repository builds a reproducible data pipeline that:
- ingests European GDP CSV data from Kaggle into a bronze data lake,
- transforms the data with PySpark and converts USD values to EUR,
- writes polished parquet data to the silver layer,
- loads the transformed data into a PostgreSQL warehouse,
- serves an interactive Streamlit dashboard with GDP trends and country share charts.

Source dataset: https://www.kaggle.com/datasets/samuelcortinhas/gdp-of-european-countries

## Architecture

- `pipeline/` contains the ETL application and Docker image for ingestion and transformation.
- `streamlit/` contains the dashboard app and Docker image for visualization.
- `docker-compose.yaml` starts PostgreSQL and pgAdmin.
- `datalake/bronze` stores raw CSV data.
- `datalake/silber` stores transformed parquet output.

## Technologies

- Python 3.13
- PySpark
- PostgreSQL 18
- Streamlit
- Plotly
- SQLAlchemy
- KaggleHub
- Docker / Docker Compose

## Prerequisites

- Docker
- Docker Compose
- Internet access

## Setup and run

### 1. Build the pipeline image

```bash
cd pipeline
docker build -t pipeline:image1 .
cd ..
```

### 2. Build the Streamlit image

```bash
cd streamlit
docker build -t streamlit:image1 .
cd ..
```

### 3. Start PostgreSQL and pgAdmin

```bash
docker compose up -d
```

This starts:
- PostgreSQL on `localhost:5432`
- pgAdmin on `localhost:8085`

### 4. Run the ETL pipeline

```bash
docker run -it --network de-zoomcamp-final-project_default -v $(pwd)/datalake:/app/datalake pipeline:image1
```

What this does:
- downloads the Kaggle GDP dataset into `datalake/bronze`
- reads the raw CSV with PySpark
- converts values from USD to EUR using a live exchange rate
- writes parquet output to `datalake/silber`
- loads the transformed data into PostgreSQL table `eu_gdp`

### 5. Run the Streamlit dashboard

```bash
docker run -it --network de-zoomcamp-final-project_default -p 8501:8501 streamlit:image1
```

Open the dashboard at:

```text
http://127.0.0.1:8501/
```

### 6. Inspect the warehouse

Open pgAdmin at:

```text
http://127.0.0.1:8085/
```

Default login:
- Email: `admin@admin.com`
- Password: `root`

Database connection inside pgAdmin:
- Host: `pgdatabase`
- Port: `5432`
- Database: `eu_gdp`
- User: `root`
- Password: `root`

## Project details

### Pipeline behavior

The pipeline performs two main steps:
1. `download_to_datalake_bronze()` downloads the Kaggle dataset and moves the CSV files into `datalake/bronze`.
2. `convert_csv_to_parquet_silber_postgre_warehouse()` reads `datalake/bronze/GDP_table.csv`, applies a live USD-to-EUR conversion, writes parquet output to `datalake/silber/GDP_table.csv`, and writes the same transformed data to PostgreSQL table `eu_gdp`.

### Streamlit dashboard

The dashboard loads data from PostgreSQL and shows:
- a stacked bar chart of country GDP history,
- a GDP share donut chart for a selected year,
- interactive filters for countries,
- summary metrics for the selected year.

## Folder structure

- `docker-compose.yaml` — starts the warehouse services.
- `pipeline/` — ETL code, dependencies, and Docker build.
- `streamlit/` — dashboard app, dependencies, and Docker build.
- `datalake/bronze` — raw CSV ingestion layer.
- `datalake/silber` — transformed parquet layer.
- `notebook/` — analysis notebooks for bronze and silver layers.

## Notes

- The pipeline uses `https://api.frankfurter.app` to fetch the USD → EUR rate.
- If the exchange API fails, the code falls back to a default rate of `0.92`.
- The Streamlit app connects to the `pgdatabase` service by default.

## Cleanup

To stop and remove Compose services:

```bash
docker compose down
```
 