from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


PROJECT_DIR = "/opt/airflow/project"


@dag(
    dag_id="retail_data_engineering_pipeline",
    description="Run the retail PySpark medallion pipeline from Bronze to Gold.",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "dhivakar",
        "retries": 1,
    },
    tags=["pyspark", "delta", "retail"],
    doc_md="""
    ### Retail Data Engineering Pipeline

    Runs the PySpark Delta Lake pipeline in medallion order:
    Bronze ingestion, incremental merge, Silver transformation, SCD Type 2,
    and Gold star schema creation.
    """,
)
def retail_data_engineering_pipeline():
    @task.bash(task_id="bronze_ingestion")
    def bronze_ingestion() -> str:
        return f"cd {PROJECT_DIR} && python spark_jobs/bronze_ingestion.py"

    @task.bash(task_id="bronze_incremental_merge")
    def bronze_incremental_merge() -> str:
        return f"cd {PROJECT_DIR} && python spark_jobs/bronze_incremental_merge.py"

    @task.bash(task_id="silver_transformation")
    def silver_transformation() -> str:
        return f"cd {PROJECT_DIR} && python spark_jobs/silver_transformation.py"

    @task.bash(task_id="scd2_tracker")
    def scd2_tracker() -> str:
        return f"cd {PROJECT_DIR} && python spark_jobs/scd2_tracker.py"

    @task.bash(task_id="gold_star_schema")
    def gold_star_schema() -> str:
        return f"cd {PROJECT_DIR} && python spark_jobs/gold_star_schema.py"

    (
        bronze_ingestion()
        >> bronze_incremental_merge()
        >> silver_transformation()
        >> scd2_tracker()
        >> gold_star_schema()
    )


retail_data_engineering_pipeline()
