import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from datetime import datetime
from tasks.categorizer import Categorizer
from tasks.converter import Converter

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 9),
    "retries": 1,
}

mysql_driver_path = "/usr/share/java/mysql-connector-java-9.2.0.jar"


@dag(
    dag_id="process_html",
    default_args=default_args,
    schedule=None,  # TODO: run every 10 mins schedule_interval = '*/10 * * * *'
    catchup=False
)
def spark_jobs():
    """
       DAG to process HTML files with Spark in parallel using @task.pyspark.
       """
    html_files_folder = "../WikipediaCrawler/html_pages"

    # Task 1: Categorizer
    @task.pyspark(config_kwargs={"spark.jars": mysql_driver_path})
    def run_categorizer(spark: SparkSession, sc: SparkContext):
        """
        Runs the Categorizer to extract categories from HTML files.
        """
        categorizer = Categorizer(spark, html_files_folder)  # Initialize Categorizer
        categorizer.save_to_sql()  # Process files

    # Task 2: Converter
    @task.pyspark()
    def run_converter(spark: SparkSession, sc: SparkContext):
        """
        Runs the Converter to extract text and save it in HDFS.
        """
        converter = Converter(spark, html_files_folder)  # Initialize Converter
        converter.save_to_hdfs()  # Process files

    # Run both tasks in parallel

    run_categorizer()
    run_converter()


# Register DAG
spark_jobs()
