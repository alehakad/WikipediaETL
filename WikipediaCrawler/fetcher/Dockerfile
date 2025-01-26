# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file from the main folder to the container
COPY requirements.txt /app/requirements.txt

# Disable output buffering
#ENV PYTHONUNBUFFERED=1

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the contents of the app folder into the container
COPY app /app/

# Define the default command to run your fetcher
CMD ["python", "main.py"]
