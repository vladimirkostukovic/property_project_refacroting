from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import logging
import time

log = logging.getLogger(__name__)

# === Utility functions ===
def run_script(script_path):
    try:
        log.info(f"Running script: {script_path}")
        subprocess.run(["python3", script_path], check=True)
    except subprocess.CalledProcessError as e:
        log.error(f"Script failed: {script_path} with error: {e}")
        raise

def delay():
    time.sleep(10)

def _build_path(rel):
    return f"/home/airflowadmin/reality_data/{rel}"

def run_with_delay(script_path):
    run_script(script_path)
    delay()

def create_python_task(task_id, script):
    return PythonOperator(
        task_id=task_id,
        python_callable=run_with_delay,
        op_args=[_build_path(script)]
    )

def create_direct_task(task_id, script):
    return PythonOperator(
        task_id=task_id,
        python_callable=run_script,
        op_args=[_build_path(script)]
    )

def build_dag():
    default_args = {
        "owner": "airflow",
        "email": ["alerts@lifegoal.cz"],
        "email_on_failure": True,
        "retries": 0
    }

    with DAG(
        dag_id="reality_data_pipeline",
        default_args=default_args,
        schedule_interval="50 6 * * *",  # 06:50 UTC = 08:50 Prague
        start_date=days_ago(1),
        catchup=False,
        description="Daily data processing pipeline",
        tags=["reality", "etl"]
    ) as dag:

        # === Run modular main.py scripts ===
        etl_main = create_direct_task("etl_main", "etl_from_scrappers/main.py")
        to_silver_main = create_direct_task("transfer_to_silver", "tranfer_to_silver/main.py")
        geo_main = create_direct_task("geo_normalization", "geo_modul/main.py")
        ai_main = create_direct_task("ai_image_processing", "AI_modul/main.py")

        # === Task order ===
        etl_main >> to_silver_main >> geo_main >> ai_main

        return dag

globals()["reality_data_pipeline"] = build_dag()