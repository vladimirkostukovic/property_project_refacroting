import os
import subprocess
import logging

def run_module(path, args=None):
    cmd = ["python", path] + (args if args else [])
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Script failed: {path}")

def main():
    logging.basicConfig(level=logging.INFO)
    BASE = os.path.dirname(__file__)

    # ===== ETL =====
    etl_dir = os.path.join(BASE, "etl from scrappers")
    run_module(os.path.join(etl_dir, "sreality.py"))
    run_module(os.path.join(etl_dir, "idnes.py"))
    run_module(os.path.join(etl_dir, "hyper.py"))
    run_module(os.path.join(etl_dir, "bezrealitky.py"))

    # ===== SILVER =====
    silver_dir = os.path.join(BASE, "tranfer to silver")
    run_module(os.path.join(silver_dir, "standartize_to_sliver.py"))

    # ===== GEO =====
    geo_dir = os.path.join(BASE, "geo modul")
    run_module(os.path.join(geo_dir, "standartize_geo_city.py"))
    run_module(os.path.join(geo_dir, "standartize_geo_city_part.py"))
    run_module(os.path.join(geo_dir, "standartize_geo_region.py"))
    run_module(os.path.join(geo_dir, "standartize_geo_street.py"))

    # ===== AI =====
    ai_dir = os.path.join(BASE, "AI modul")
    run_module(os.path.join(ai_dir, "main.py"), ["batch", "--batch-size", "100", "--limit", "10000"])

    logging.info("Pipeline completed successfully.")

if __name__ == '__main__':
    main()