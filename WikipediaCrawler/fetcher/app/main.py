import os
from datetime import datetime
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pika.spec import Basic, BasicProperties

from db_api import MongoConnector
from logger_config import logger
from queue_api import QueueConnector


class Fetcher:
    html_storage_path = "/app/html_pages"
    out_queue = os.getenv("OUT_QUEUE", "to_check_links")
    in_queue = os.getenv("IN_QUEUE", "valid_links")

    def __init__(self, queue_connector: QueueConnector, mongo_connector: MongoConnector):
        logger.info(f"Connect to queue")
        self.queue_connector = queue_connector
        self.mongo_connector = mongo_connector
        os.makedirs(Fetcher.html_storage_path, exist_ok=True)

    @staticmethod
    def fetch_html(url: str) -> tuple[str, dict] | tuple[None, None]:
        """
        Fetches the HTML content of a given URL.
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.text, response.headers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None, None

    @staticmethod
    def save_html_to_local(url: str, html_content: str):
        """
        Saves the fetched HTML content to a local file.
        """
        sanitized_name = url.replace("http://", "").replace("https://", "").replace("/", "_")
        file_path = os.path.join(Fetcher.html_storage_path, f"{sanitized_name}.html")

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(html_content)
        logger.info(f"Page {url} saved to local storage")
        return file_path

    @staticmethod
    def find_last_modified_date(html_headers: dict) -> datetime | None:
        """
        Finds the last modified date from the HTTP headers of the given URL.
        """
        last_modified_str = html_headers.get('Last-Modified')
        if not last_modified_str:
            logger.warning("Last-Modified header not found in the response.")
            return None

        try:
            return datetime.strptime(last_modified_str, "%a, %d %b %Y %H:%M:%S %Z")
        except ValueError as e:
            logger.error(f"Error parsing Last-Modified header: {e}")
            return None

    def process_message(self, channel, method: Basic.Deliver, properties: BasicProperties, body):
        """
        Processes a message by fetching HTML from the URL, saving it locally,
        extracting links from the HTML content, and publishing them to the out_queue.
        """
        delivery_tag = method.delivery_tag
        url = body.decode()
        retry_count = int(properties.headers.get("x-retry-count", 0)) if properties.headers else 0
        logger.debug(f"Processing URL: {url}, Retry Count: {retry_count}")

        try:
            html_content, html_headers = self.fetch_html(url)
            if not html_content:
                logger.error(f"Failed to fetch HTML from {url}")
                # send to retry queue with delay
                self.queue_connector.nack(delivery_tag)
                return
            # save html to localhost
            page_file_path = self.save_html_to_local(url, html_content)

            last_modified_date = self.find_last_modified_date(html_headers)
            # add url with metadata to mongo
            self.mongo_connector.save_url_to_db(url, page_file_path, last_modified_date)
            # find links
            links = self.find_html_links(url, html_content)

            # add to next message queues
            for link in links:
                self.queue_connector.publish(link, Fetcher.out_queue)

            self.queue_connector.ack(delivery_tag)

        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}, moving to dlx")
            # TODO: handle retries
            self.queue_connector.nack(delivery_tag, requeue=False)

    @staticmethod
    def find_html_links(start_url: str, html_content: str) -> List[str]:
        """
        Extracts all the hyperlinks from the HTML content.
        """
        html_soup = BeautifulSoup(html_content, "html.parser")
        links = {urljoin(start_url, a_tag['href']) for a_tag in html_soup.find_all("a", href=True)}
        return list(links)

    def start(self):
        """
        Starts the queue consumption.
        """
        logger.info("Starting new fetcher")
        self.queue_connector.consume(Fetcher.in_queue, self.process_message, False)

    def close(self):
        """
        Clean up resources.
        """
        logger.info("Closing resources...")
        self.mongo_connector.close()


if __name__ == "__main__":
    queue_handler = QueueConnector()
    mongo_handler = MongoConnector()
    fetcher = Fetcher(queue_handler, mongo_handler)
    try:
        fetcher.start()
    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down")
    finally:
        fetcher.close()
