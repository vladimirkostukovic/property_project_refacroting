# Real Estate Data Pipeline (Refactored to Gold Layer)

**Author**: [Vladimír Košťukovič](https://www.linkedin.com/in/vladimirkostukovic/)

This repository presents a fully refactored version of a real estate analytics pipeline, developed up to the Gold Layer. The focus is on clean modular architecture, automated orchestration, and scalable processing for analytics and BI integration.

The project follows the Medallion Architecture (bronze → silver → gold) to ensure clear separation of concerns:

- Raw data extraction  
- Transformation and standardization  
- Cleaning and deduplication (including AI-based logic)  
- Structured output ready for business analysis  

## What This Project Is NOT

Web scraping is externalized.  
All scraper infrastructure is deployed on a separate machine and is not included here.

## What’s Inside

- ETL Pipelines — transformation from raw to normalized, structured data  
- Geo Normalization — fuzzy address matching with AI assistance (city, region, street, etc.)  
- Image Deduplication — high-performance visual deduplication using perceptual hashing and parallelism  
- Modular Design — each stage in its own Python module (`main.py` per module)  
- Logging and Error Handling — all steps fully logged; alerts on failure  
- Superset-Ready Output — clean schema designed for use in Apache Superset dashboards  
- Scalable Architecture — built for distributed workflows and large data volumes  
- Reproducibility — modules can be run standalone or orchestrated via DAG (Airflow-ready)  

## Tech Stack

- Python: pandas, sqlalchemy, APScheduler, aiohttp, Pillow, numpy, imagehash, annoy, rapidfuzz, tqdm, unidecode  
- PostgreSQL: main analytical database (bronze/silver/gold)  
- Redis: used for batching and duplicate detection  
- APScheduler: modular scheduling and retries  
- Docker: recommended for deployment  
- Apache Superset: BI/dashboard layer (instead of Metabase)  
- Airflow: DAG orchestrator  

See `requirements.txt` for full list of packages.

## Project Structure

project-root/
│
├── etl_from_scrappers/
│   ├── sreality.py
│   ├── idnes.py
│   ├── hyper.py
│   ├── bezrealitky.py
│   └── main.py
│
├── tranfer_to_silver/
│   ├── standartize_to_sliver.py
│   └── main.py
│
├── geo_modul/
│   ├── standartize_geo_city.py
│   ├── standartize_geo_city_part.py
│   ├── standartize_geo_region.py
│   ├── standartize_geo_street.py
│   └── main.py
│
├── AI_modul/
│   ├── image_processor.py
│   ├── batch_manager.py
│   ├── db_handler.py
│   ├── main.py
│   └── config.json
│
├── DAGS/
│   └── reality_data_pipeline.py
│
├── config.py
├── requirements.txt
├── README.md
└── .gitignore


## Why Refactor?

After multiple MVP iterations and SQL-based prototypes, this version was fully restructured to:

- Improve maintainability  
- Enable orchestration and automation  
- Support growth in data volume and logic complexity  

This repo reflects the current best practices derived from real-world experimentation and implementation.

## Next Steps

- Implement Data Vault design pattern for the Gold Layer  
- Add DAX/OLAP semantic layer for business reporting  
- Publish selected dashboards publicly via Apache Superset  

## How to Run

1. Clone the repository:

    git clone
    cd reality-data-pipeline

2. Install dependencies (use virtualenv if needed):

    pip install -r requirements.txt

3. Configure database access in `config.py` (or `config.json` for AI module).

4. Run modules manually for testing:

    python etl_from_scrappers/main.py  
    python tranfer_to_silver/main.py  
    python geo_modul/main.py  
    python AI_modul/main.py

5. Or trigger full pipeline via Airflow:

    airflow dags trigger reality_data_pipeline