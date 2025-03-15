import logging
import os
from datetime import datetime

from airflow.exceptions import AirflowException
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, size, split, udf
from pyspark.sql.types import ArrayType, DateType, StringType
from sqlalchemy import Column, Date, ForeignKey, Integer, String, Table, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from .utils import sanitize_filename

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

# Define tables
# pages and categories pairs
page_category_association = Table(
    'page_categories',
    Base.metadata,
    Column('page_id', Integer, ForeignKey('pages.id'), primary_key=True),
    Column('category_id', Integer, ForeignKey('categories.id'), primary_key=True)
)


# pages table
class Page(Base):
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(500), nullable=False, unique=True)  # File name
    word_count = Column(Integer, nullable=False, default=0)  # Word count
    last_edited_date = Column(Date, nullable=True)  # Last edited date

    # Many-to-Many relationship with categories
    categories = relationship('Category', secondary=page_category_association, back_populates='pages')


# category table
class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)  # Category name

    # Many-to-Many relationship with pages
    pages = relationship('Page', secondary=page_category_association, back_populates='categories')


def create_tables():
    Base.metadata.create_all(engine)
    logging.info("Tables created or already exist.")


# Run on startup
class Categorizer:
    def __init__(self, spark, html_dir):
        """Initialize with the file path of the HTML."""
        self.spark = spark
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

    @staticmethod
    def extract_last_edited_date(html):
        """Find last edited date"""
        soup = BeautifulSoup(html, 'html.parser')
        last_edited = soup.find('li', id='footer-info-lastmod')
        last_edited_date = None

        if last_edited:
            last_edited_text = last_edited.get_text(strip=True)
            last_edited_text = last_edited_text.replace(" (UTC)", "")
            date_str = last_edited_text.replace("This page was last edited on ", "").split(",")[0]

            try:
                last_edited_date = datetime.strptime(date_str, '%d %B %Y').date()
                logging.debug(f"Last time edited (date only): {last_edited_date}")
            except ValueError as e:
                logging.error(f"Error parsing date: {e}")
        else:
            logging.debug("Couldn't find last time edited.")
        return last_edited_date

    def process_html_files(self):
        """Reads all HTML files, extracts categories, and returns a DataFrame."""
        clean_name_udf = udf(sanitize_filename, StringType())
        extract_categories_udf = udf(self.extract_categories, ArrayType(StringType()))
        extract_dates_udf = udf(self.extract_last_edited_date, DateType())

        # read htmls, add name of file
        categories_df = self.spark.read.text(f"{self.html_dir}/*.html", wholetext=True).withColumn("file_path", input_file_name())
        # clean file_name
        categories_df = categories_df.withColumn("file_name", clean_name_udf("file_path"))
        # add categories
        categories_df = categories_df.withColumn("categories", extract_categories_udf(categories_df["value"]))
        # add word count
        categories_df = categories_df.withColumn("word_count", size(split(categories_df["value"], " ")))
        # add last edited date
        categories_df = categories_df.withColumn("last_edited_date", extract_dates_udf(categories_df["value"]))

        return categories_df

    def save_to_sql(self):
        """Saves extracted data to MySQL, returns processed html."""
        # process HTML files and extract categories
        try:
            categories_df = self.process_html_files()

            # insert rows with sqlalchemy
            data = categories_df.select("file_name", "categories", "word_count", "last_edited_date").collect()

            Session = sessionmaker(bind=engine)
            session = Session()

            # insert pages
            for row in data:
                file_name = row["file_name"]
                word_count = row["word_count"]
                last_edited_date = row["last_edited_date"]
                page = session.query(Page).filter_by(file_name=file_name).first()
                if not page:
                    page = Page(file_name=file_name, word_count=word_count, last_edited_date=last_edited_date)
                    session.add(page)
            session.commit()

            # insert categories
            for row in data:
                categories = row["categories"]

                for category_name in categories:
                    category = session.query(Category).filter_by(name=category_name).first()
                    if not category:
                        category = Category(name=category_name)
                        session.add(category)
            session.commit()

            # insert pages, categories pairs
            for row in data:
                file_name = row["file_name"]
                categories = row["categories"]
                page = session.query(Page).filter_by(file_name=file_name).first()
                if page:
                    for category_name in categories:
                        category = session.query(Category).filter_by(name=category_name).first()
                        if category:
                            page.categories.append(category)
            session.commit()
            session.close()
            logging.info("HTML pages with categories saved successfully to MySQL.")

            return [row["file_name"] for row in data]

        except Exception as e:
            logging.error("Error processing categories", e)
            raise AirflowException(f"Error processing categories {e}")


if __name__ == "__main__":
    create_tables()
    html_dir = "../../WikipediaCrawler/html_pages"
    mysql_driver_path = "/usr/share/java/mysql-connector-java-9.2.0.jar"
    spark_session = SparkSession.builder.appName("HTMLCategoryExtraction").config("spark.jars",
                                                                                  mysql_driver_path).getOrCreate()
    categorizer = Categorizer(spark_session, html_dir)  # Pass the file path
    categorizer.save_to_sql()
