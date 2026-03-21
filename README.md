# SC Iceberg API Data Platform

Location-aware weather data platform built with:

- Open-Meteo APIs
- Apache Iceberg via PyIceberg
- Pandera validation
- Dagster orchestration
- FastAPI serving

## Current Pipeline

`Open-Meteo Geocoding API -> dim_location -> Open-Meteo Weather API -> Bronze -> Silver -> Gold -> FastAPI`

Current API endpoints:

- `GET /health`
- `GET /locations`
- `GET /weather/daily`

## Local Setup

Install dependencies:

```bash
uv sync
```

## Local Bootstrap

To rebuild the full local pipeline state:

```bash
task bootstrap-local
```

This will:

1. reset the local Iceberg catalog
2. create all tables
3. ingest locations
4. ingest Bronze weather
5. rebuild Silver weather
6. rebuild Gold facts

## Run Dagster

```bash
task run-dagster
```

Dagster loads assets from:

- [`orchestration/definitions.py`](/mnt/c/Users/71861/Own/Projects/data_engineering/showcase/sc-iceberg-api-data-platform/orchestration/definitions.py)

Current location partition:

- `Amsterdam`

## Run API

```bash
task run-api
```

## API Usage

List available locations:

```text
GET /locations
GET /locations?limit=20
```

Query daily weather:

```text
GET /weather/daily
GET /weather/daily?location_id=<location_id>
GET /weather/daily?start_date=2026-03-20&end_date=2026-03-27
GET /weather/daily?location_id=<location_id>&start_date=2026-03-20&end_date=2026-03-27&limit=7
```

## Notes

- `dim_location` and Bronze are append-oriented.
- Silver and Gold are rebuilt as canonical tables from upstream state.
- Dagster assets are partitioned by location name.
