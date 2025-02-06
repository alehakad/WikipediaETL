import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.categorizer import Categorizer
from tasks.converter import Converter

html_files_folder = "../WikipediaCrawler/html_pages"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_unprocessed_files():
    """Gets list of all unprocessed files from folder """

    unprocessed_files = []
    for file_name in os.listdir(html_files_folder):
        file_path = os.path.join(html_files_folder, file_name)
        unprocessed_files.append(file_path)

    return unprocessed_files


with DAG(
        dag_id="html_processing_dag",
        default_args=default_args,
        description='A simple DAG to process HTML pages for categorization and text conversion',
        schedule_interval='*/10 * * * *',  # Runs every 10 minutes
        catchup=False,
) as dag:
    @task
    def process_and_save_categories(file_path):
        """Extract categories from HTML and save to MySQL."""
        try:
            print(f"process_and_save_categories for {file_path}")
            categorizer = Categorizer(file_path)
            categories = categorizer.extract_categories()
            categorizer.load_to_sql(categories)
            return categories
        except Exception as e:
            print(f"Error in processing and saving categories: {e}")
            raise


    @task
    def process_and_save_text(file_path):
        """Extract clean text from HTML and save to HDFS."""
        try:
            print(f"process_and_save_text for {file_path}")
            converter = Converter(file_path)
            text = converter.extract_text()
            converter.save_to_hdfs(text)
            return text
        except Exception as e:
            print(f"Error in processing and saving text: {e}")
            raise


    html_files = get_unprocessed_files()

    categories_tasks = process_and_save_categories.expand(file_path=html_files)
    text_tasks = process_and_save_text.expand(file_path=html_files)

    # Set dependencies: process categories before processing text
    # categories_tasks >> text_tasks
