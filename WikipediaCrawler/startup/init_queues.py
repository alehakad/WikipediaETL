import os

import pika


def setup_rabbitmq():
    start_url = "https://en.wikipedia.org/wiki/Main_Page"
    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(os.getenv("RABBITMQ_DEFAULT_USER"), os.getenv("RABBITMQ_DEFAULT_PASS"))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST"), credentials=credentials),
    )

    channel = connection.channel()

    # Declare queues

    channel.queue_declare(queue=os.getenv("FETCHER_QUEUE"), durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "dead_letter_queue"
    })

    channel.queue_declare(queue="dead_letter_queue", durable=True, arguments={'x-message-ttl': 30000,
                                                                              'x-dead-letter-exchange': '',
                                                                              'x-dead-letter-routing-key': os.getenv(
                                                                                  "FETCHER_QUEUE")})

    channel.queue_declare(queue=os.getenv("PARSER_QUEUE"), durable=True)
    channel.queue_declare(queue=os.getenv("FILTER_QUEUE"), durable=True)

    # Add the initial URL to the valid_links queue
    channel.basic_publish(
        exchange="",
        routing_key="valid_links",
        body=start_url,
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent message
    )

    print(f"Initialized RabbitMQ with initial URL: {start_url}")

    # Close the connection
    connection.close()


if __name__ == "__main__":
    setup_rabbitmq()
