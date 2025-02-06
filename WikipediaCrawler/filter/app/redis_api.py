import os

import redis

from logger_config import logger


class RedisURLHandler:
    url_set_name = "links"

    def __init__(self):
        """
        Initialize the Redis connection.
        """
        self.redis_client = redis.StrictRedis(host=os.getenv("REDIS_HOST", "localhost"),
                                              port=os.getenv("REDIS_PORT", "localhost"), decode_responses=True)

    def add_url_if_not_exists(self, url: str):
        """
        Check if a URL exists in a Redis set, and add it if it doesn't.
        """
        if self.redis_client.sismember(RedisURLHandler.url_set_name, url):
            logger.debug(f"URL already exists in the set: {url}")
            return False
        else:
            self.redis_client.sadd(RedisURLHandler.url_set_name, url)
            logger.debug(f"URL added to the set: {url}")
            return True
