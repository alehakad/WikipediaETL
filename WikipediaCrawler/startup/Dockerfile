# Use a lightweight Python image
FROM python:3.12-slim

# Install dependencies
RUN pip install pika

# Copy the initialization script
COPY init_queues.py /app/init_queues.py

# Set the working directory
WORKDIR /app

# Run the script
CMD ["python", "init_queues.py"]
