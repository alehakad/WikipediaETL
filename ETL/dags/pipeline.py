import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.categorizer import Categorizer
from tasks.converter import Converter

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def process_and_save_categories(file_path):
    """Extract categories from HTML and save to MySQL."""
    try:
        categorizer = Categorizer(file_path)  # Initialize Categorizer with the file path
        categories = categorizer.extract_categories()  # Extract categories
        print(f"Categories extracted: {categories}")

        # Write the categories to MySQL (assuming a method for that in Categorizer)
        categorizer.load_to_sql(categories)

        return categories
    except Exception as e:
        print(f"Error in processing and saving categories: {e}")
        raise


def process_and_save_text(file_path):
    """Extract clean text from HTML and save to HDFS."""
    try:
        converter = Converter(file_path)  # Initialize Converter with the file path
        text = converter.extract_text()  # Extract clean text
        print(f"Text extracted: {text}")

        # Write the text to HDFS (assuming a method for that in Converter)
        converter.save_to_hdfs(text)

        return text
    except Exception as e:
        print(f"Error in processing and saving text: {e}")
        raise


with DAG(
        dag_id="html_processing_dag",
        default_args=default_args,
        description='A simple DAG to process HTML pages for categorization and text conversion',
        schedule_interval='*/10 * * * *',  # Runs every 10 minutes
        catchup=False,
) as dag:
    # Define the tasks using PythonOperator
    process_categories_task = PythonOperator(
        task_id='process_and_save_categories',
        python_callable=process_and_save_categories,
        op_args=["../../WikipediaCrawler/html_pages/en.wikipedia.org_wiki_Agriculture.html"]
    )

    process_text_task = PythonOperator(
        task_id='process_and_save_text',
        python_callable=process_and_save_text,
        op_args=["../../WikipediaCrawler/html_pages/en.wikipedia.org_wiki_Agriculture.html"]
    )

    # Set task dependencies
    process_categories_task >> process_text_task  # Task 2 runs after Task 1
