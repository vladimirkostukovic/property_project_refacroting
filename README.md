Autor https://www.linkedin.com/in/vladimirkostukovic/


Real Estate Data Pipeline (up to Gold Layer Refactoring)

This repository presents the refactored version of real estate analytics project, reworked up to the “gold layer” level. The main focus here is on robust ETL pipelines, data verification, and preparing the foundation for scalable analytics.

The project follows the medallion architecture (bronze, silver, gold), separating all stages:
	•	raw data collection,
	•	transformation,
	•	cleaning (with custom scripts and AI),
	•	deduplication,
	•	and structured output ready for advanced analytics and reporting.

⸻

What This Project Is NOT
	•	Web scraping is separated into its own process and runs on a dedicated server.
All scraping code and infrastructure are out of scope and not described here.

⸻

What’s Inside
	•	ETL pipelines: All transformations from raw to clean, normalized data
	•	Geo normalization: AI-enhanced scripts for fuzzy-matching addresses (city, region, street, etc.)
	•	Image deduplication: High-performance module for detecting and removing duplicate listings using perceptual hashing and parallel processing
	•	Modular structure: Each logical step in its own Python module, easy to test and scale
	•	Full logging: All critical steps are logged, failures are tracked separately
	•	Superset-ready: Output structure is optimized for further BI analysis in Apache Superset,
since Metabase is not suitable for this level of data workflow
	•	Scalability: Built for distributed processing and big data, with clear boundaries between data layers
	•	Reproducible pipeline: All modules can be run independently or as part of an automated orchestration

⸻

Tech Stack
	•	Python (Pandas, SQLAlchemy, APScheduler, aiohttp, Pillow, numpy, imagehash, annoy, rapidfuzz, tqdm, unidecode)
	•	PostgreSQL (main DB, optimized for analytics)
	•	Redis (for batching, cache, and fast duplicate search)
	•	APScheduler (for orchestration and scheduling)
	•	Docker (recommended for running isolated services)
	•	Superset (BI/dashboards)
	•	And a range of smaller utility libraries (see requirements.txt)

⸻

Project Structure

project-root/
    etl from scrappers/
        sreality.py
        idnes.py
        hyper.py
        bezrealitky.py
    tranfer to silver/
        standartize_to_sliver.py
        copy_st_silver_to_geo.py
    geo modul/
        standartize_geo_city.py
        standartize_geo_city_part.py
        standartize_geo_region.py
        standartize_geo_street.py
    AI modul/
        main.py           # Orchestrates image deduplication & processing
    logs/
    config.json          # Main configuration for the photo module
    requirements.txt
    README.md

Why Refactor? (Background)

After extensive SQL prototyping and data testing, the project reached an MVP stage.
At this point, I decided to rewrite and structure all code from scratch to achieve maintainability and clear scaling options.
This repository reflects the current best practices and lessons learned from the initial MVP.

⸻

Next Steps
	•	Data Vault design for the gold layer
	•	Full DAX/OLAP layer for business analytics (after validation)
	•	Public BI dashboards on Superset

