import os

from logger_config import logger
from queue_api import QueueConnector
from redis_api import RedisURLHandler


class Filter:
    in_queue = os.getenv("IN_QUEUE", "check_dups_links")
    out_queue = os.getenv("OUT_QUEUE", "valid_links")

    def __init__(self, queue_connector: QueueConnector, redis_connector: RedisURLHandler):
        self.queue_connector = queue_connector
        self.redis_connector = redis_connector

    def check_duplicate(self, url: str) -> bool:
        """
        Checks if a given URL was already handled - exists in redis.
        """
        try:
            return self.redis_connector.add_url_if_not_exists(url)
        except Exception as e:
            logger.error(f"Error checking duplicate for URL {url}: {e}")
            return False

    def process_message(self, channel, method, properties, body: bytes):
        """
        Processes links adds valid ones to fetcher queue.
        """
        try:
            url = body.decode()
            logger.debug(f"Checking duplicate url for {url}")
            if self.check_duplicate(url):
                logger.debug(f"Added link {url} to fetcher queue")
                self.queue_connector.publish(url, Filter.out_queue)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def start(self):
        """
        Starts the queue consumption.
        """
        logger.info("Start filter")
        self.queue_connector.consume(Filter.in_queue, self.process_message)


if __name__ == "__main__":
    queue_handler = QueueConnector()
    redis_handler = RedisURLHandler()
    filterer = Filter(queue_handler, redis_handler)
    filterer.start()
