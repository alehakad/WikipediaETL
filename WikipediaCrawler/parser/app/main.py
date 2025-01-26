import os
from urllib.parse import urlparse, urlunparse

from logger_config import logger
from queue_api import QueueConnector


class Parser:
    out_queue = os.getenv("OUT_QUEUE", "check_dups_links")
    in_queue = os.getenv("IN_QUEUE", "to_check_links")

    def __init__(self, queue_connector: QueueConnector):
        self.queue_connector = queue_connector

    @staticmethod
    def parse_wikipedia_link(url: str):
        """
        Checks if a given URL is a Wikipedia link and normalizes it.
        """
        try:
            parsed_url = urlparse(url)

            # Check if the domain belongs to Wikipedia
            if parsed_url.netloc.endswith("wikipedia.org"):
                # Normalize the URL
                normalized_path = parsed_url.path.rstrip("/")  # Remove trailing slash
                normalized_url = urlunparse((
                    parsed_url.scheme,  # Keep the original scheme (http/https)
                    parsed_url.netloc.lower(),  # Use lowercase for the domain
                    normalized_path,  # Normalized path
                    "",  # Remove params
                    "",  # Remove query
                    ""  # Remove fragment
                ))
                return normalized_url
            else:
                return None  # Not a Wikipedia link
        except Exception as e:
            logger.error(f"Error processing URL: {e}")
            return None

    def process_message(self, channel, method, properties, body):
        """
        Processes links adds valid ones to filter queue.
        """
        url = body.decode()
        logger.debug(f"Checking with link for {url}")
        normalized_link = self.parse_wikipedia_link(url)
        if normalized_link:
            logger.debug(f"Added link {normalized_link} to filter queue")
            self.queue_connector.publish(normalized_link, Parser.out_queue)

    def start(self):
        """
        Starts the queue consumption.
        """
        logger.info("Start parser")
        self.queue_connector.consume(Parser.in_queue, self.process_message)


if __name__ == "__main__":
    queue_handler = QueueConnector()
    parser = Parser(queue_handler)
    parser.start()
