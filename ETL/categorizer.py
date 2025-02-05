import os

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Load environment variables from .env file
load_dotenv()

# Get the DATABASE_URL from the .env file
DATABASE_URL = os.getenv("DATABASE_URL")

# Check if the DATABASE_URL is loaded properly
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in the environment variables!")
# Create the engine
engine = create_engine(DATABASE_URL, echo=True)

Base = declarative_base()


class Page(Base):
    __tablename__ = 'pages_table'

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_path = Column(String(255), nullable=False, unique=True)

    categories = relationship("Category", back_populates="page")

    def __init__(self, page_path):
        self.page_path = page_path


class Category(Base):
    __tablename__ = 'categories_table'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(255), nullable=False)
    page_id = Column(Integer, ForeignKey('pages_table.id'), nullable=False)

    # Relationship to the Page table
    page = relationship("Page", back_populates="categories")


# Define metadata and table structure
def create_tables_if_not_exists():
    # Create the table in the database if it doesn't already exist
    Base.metadata.create_all(engine)
    print(f"Table 'categories_table' created or already exists.")


class Categorizer:
    def __init__(self, file_path):
        """Initialize with the file path of the HTML."""
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Create a BeautifulSoup object from the HTML content
        self.soup = BeautifulSoup(html_content, "html.parser")
        self.file_path = file_path

    def extract_categories(self):
        """Extract categories from the first <ul> inside mw-normal-catlinks."""
        # Find the mw-normal-catlinks div
        cat_links_div = self.soup.find("div", {"id": "mw-normal-catlinks"})

        if not cat_links_div:
            return []  # Return empty list if div is not found

        # Find the first <ul> inside this div
        ul = cat_links_div.find("ul")
        if not ul:
            return []  # Return empty list if no <ul> found

        # Extract category names from <a> tags within <li> elements inside the <ul>
        categories = [a.get_text(strip=True) for a in ul.find_all("a")]

        return categories

    def load_to_sql(self, categories):
        """Write the categories to MySQL using SQLAlchemy."""
        print(f"Categories loaded to MySQL: {categories}")

        # Create an engine and session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Check if the page exists in the pages table or create it
        page = session.query(Page).filter_by(page_path=self.file_path).first()
        if not page:
            page = Page(page_path=self.file_path)
            session.add(page)  # Add the page if it doesn't exist
            session.commit()  # Commit to get the page ID

        # Insert categories into the table using ORM
        for category in categories:
            category_instance = Category(category=category, page=page)  # Reference the page instance
            session.add(category_instance)  # Add the instance to the session

        # Commit the transaction
        session.commit()

        # Close the session
        session.close()


if __name__ == "__main__":
    create_tables_if_not_exists()
    # Example usage with a BeautifulSoup object
    file_path = "../WikipediaCrawler/html_pages/en.wikipedia.org_wiki_Agriculture.html"

    categorizer = Categorizer(file_path)  # Pass the file path
    categories_list = categorizer.extract_categories()
    categorizer.load_to_sql(categories_list)
