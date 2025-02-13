import logging
import os

from airflow.exceptions import AirflowException
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyspark.sql.functions import input_file_name, udf
from pyspark.sql.types import StringType

from utils import sanitize_filename

# Configure the logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

load_dotenv()

HADOOP_HOST = os.getenv("HADOOP_HOST", "localhost")
HADOOP_PORT = os.getenv("HADOOP_PORT", 9000)


class Converter:
    def __init__(self, spark, html_dir):
        """Initialize with the file path of the HTML."""
        self.html_dir = html_dir
        self.spark = spark  # SparkSession.builder \
        #     .appName("ExtractHtmlText") \
        #     .getOrCreate()

    @staticmethod
    def extract_text(html):
        """Extract clean text from the HTML."""
        soup = BeautifulSoup(html, "html.parser")
        # Remove unwanted tags
        for element in soup(["script", "style", "meta", "head", "title", "noscript"]):
            element.extract()

        # Return clean text
        return soup.get_text(separator=" ", strip=True)

    def save_to_hdfs(self):
        """Saves the given text to a file in HDFS."""
        try:
            extract_text_udf = udf(self.extract_text, StringType())
            clean_name_udf = udf(sanitize_filename, StringType())

            html_df = self.spark.read.text(self.html_dir, wholetext=True).withColumn("file_path", input_file_name())

            # clean file_name
            html_df = html_df.withColumn("file_name", clean_name_udf("file_path"))

            html_df = html_df.withColumn("extracted_text", extract_text_udf(html_df["value"])).select("file_name",
                                                                                                      "extracted_text")

            output_path = f"hdfs://{HADOOP_HOST}:{HADOOP_PORT}/user/html_texts/"
            html_df.write.mode("overwrite").parquet(output_path)

            logging.info(f"Text saved to HDFS")
        except Exception as e:
            logging.error("Error extracting text", e)
            raise AirflowException(f"Error extracting text: {str(e)}")


if __name__ == "__main__":
    html_dir = "../../WikipediaCrawler/html_pages"

    converter = Converter(html_dir)  # Pass the BeautifulSoup object
    converter.save_to_hdfs()
