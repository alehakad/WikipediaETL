import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from datetime import datetime
from tasks.categorizer import Categorizer
from tasks.converter import Converter
import shutil

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
    catchup=False,  # prevents DAG from running jobs for missed intervals
    max_active_runs=1  # max number of dag active runs
)
def spark_jobs():
    """
       DAG to process HTML files with Spark in parallel using @task.pyspark.
       """
    html_files_folder = "../WikipediaCrawler/html_pages"
    html_processed_files_folder = "../WikipediaCrawler/html_pages_processed"

    # Task 1: Categorizer
    @task.pyspark(config_kwargs={"spark.jars": mysql_driver_path})
    def run_categorizer(spark: SparkSession, sc: SparkContext):
        """
        Runs the Categorizer to extract categories from HTML files.
        """
        categorizer = Categorizer(spark, html_files_folder)  # Initialize Categorizer
        processed_files = categorizer.save_to_sql()  # Process files

        return processed_files

    # Task 2: Converter
    @task.pyspark()
    def run_converter(spark: SparkSession, sc: SparkContext):
        """
        Runs the Converter to extract text and save it in HDFS.
        """
        converter = Converter(spark, html_files_folder)  # Initialize Converter
        converter.save_to_hdfs()  # Process files

        return []

    # task group to run both tasks in parallel
    @task_group
    def transform_htmls():
        categorizer_files = run_categorizer()
        converter_files = run_converter()

        return categorizer_files, converter_files

    # Task 3: move processed files to processed folder
    @task
    def move_files(categorizer_files: list[str], converter_files: list[str]):
        """Moves all files processed to another folder"""
        all_files_to_move = categorizer_files
        for file_path in all_files_to_move:
            try:
                file_path = file_path.removeprefix("file://")  # remove spark prefix
                if os.path.exists(file_path):
                    file_name = os.path.basename(file_path)
                    destination = os.path.join(html_processed_files_folder, file_name)
                    shutil.move(file_path, destination)
                    print(f"Moved {file_path} to {destination}")
                else:
                    print(f"File {file_path} does not exist!")
            except Exception as e:
                # print(f"Error moving file {file_path}: {str(e)}")
                raise AirflowException(f"Error moving file {file_path}: {str(e)}")

    task1_files, task2_files = transform_htmls()
    move_files(task1_files, task2_files)


# Register DAG
spark_jobs()
