import logging
import os

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, to_json, udf
from pyspark.sql.types import ArrayType, StringType
from sqlalchemy import Column, Integer, JSON, String, create_engine
from sqlalchemy.orm import declarative_base

from utils.utils import sanitize_filename

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = os.getenv("MYSQL_PORT")

# Configure the logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ])

DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
# Create engine and session
engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()


# Define table
class PageCategory(Base):
    __tablename__ = 'pages_categories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(500), nullable=False, unique=True)  # VARCHAR(1000)
    categories = Column(JSON, nullable=False)  # JSON format


def create_tables():
    Base.metadata.create_all(engine)
    logging.info("Table 'pages_categories' created or already exists.")


mysql_driver_path = "/usr/share/java/mysql-connector-java-9.2.0.jar"


# Run on startup
class Categorizer:
    def __init__(self, html_dir):
        """Initialize with the file path of the HTML."""
        self.spark = SparkSession.builder.appName("HTMLCategoryExtraction").config("spark.jars",
                                                                                   mysql_driver_path).getOrCreate()
        self.html_dir = html_dir
        self.mysql_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        self.mysql_properties = {
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    @staticmethod
    def extract_categories(html):
        """Extract categories from the first <ul> inside mw-normal-catlinks."""
        soup = BeautifulSoup(html, "html.parser")
        cat_links_div = soup.find("div", {"id": "mw-normal-catlinks"})

        if not cat_links_div:
            return []  # Return empty categories

        ul = cat_links_div.find("ul")
        categories = [a.get_text(strip=True) for a in ul.find_all("a")] if ul else []

        return categories

    def process_html_files(self):
        """Reads all HTML files, extracts categories, and returns a DataFrame."""
        clean_name_udf = udf(sanitize_filename, StringType())
        extract_categories_udf = udf(self.extract_categories, ArrayType(StringType()))

        # read htmls, add name of file
        categories_df = self.spark.read.text(html_dir, wholetext=True).withColumn("file_path", input_file_name())
        # clean file_name
        categories_df = categories_df.withColumn("file_name", clean_name_udf("file_path"))
        # add categories
        categories_df = categories_df.withColumn("categories", extract_categories_udf(categories_df["value"]))

        return categories_df

    def save_to_sql(self):
        """Saves extracted data to MySQL."""
        # process HTML files and extract categories
        categories_df = self.process_html_files()

        # convert categories to JSON and clean page_path
        mysql_df = categories_df.withColumn("categories", to_json(col("categories")))

        # write the DataFrame to MySQL
        mysql_df.select("file_name", "categories").write \
            .jdbc(url=self.mysql_url,
                  table="pages_categories",
                  mode="append",
                  properties=self.mysql_properties)

        logging.info("Html pages with categories saved successfully to MySQL.")


if __name__ == "__main__":
    create_tables()
    html_dir = "../../WikipediaCrawler/html_pages"

    categorizer = Categorizer(html_dir)  # Pass the file path
    categorizer.save_to_sql()
