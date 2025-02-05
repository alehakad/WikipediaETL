import os

from bs4 import BeautifulSoup
from hdfs import InsecureClient


class Converter:
    def __init__(self, file_path):
        """Initialize with the file path of the HTML."""
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Create a BeautifulSoup object from the HTML content
        self.soup = BeautifulSoup(html_content, "html.parser")
        self.file_name = os.path.splitext(os.path.basename(file_path))[0]

    def extract_text(self):
        """Extract clean text from the HTML."""
        # Remove unwanted tags
        for element in self.soup(["script", "style", "meta", "head", "title", "noscript"]):
            element.extract()

        # Return clean text
        return self.soup.get_text(separator=" ", strip=True)

    def save_to_hdfs(self, text):
        """Saves the given text to a file in HDFS."""
        # Connect to HDFS
        client = InsecureClient('http://localhost:50070', user='hadoop')  # Replace with your HDFS URL

        hdfs_path = f'/user/hadoop/{self.file_name}.txt'

        # Save text to a file in HDFS
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(text)

        print(f"Text saved to HDFS at {hdfs_path}")


if __name__ == "__main__":
    # Example usage with a BeautifulSoup object
    html_path = "../WikipediaCrawler/html_pages/en.wikipedia.org_wiki_Agriculture.html"

    converter = Converter(html_path)  # Pass the BeautifulSoup object
    clean_text = converter.extract_text()
    converter.save_to_hdfs(clean_text)

    print(clean_text)
