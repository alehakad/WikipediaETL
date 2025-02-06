#!/bin/bash

# Set local instance of Airflow
export AIRFLOW_HOME=$(pwd)

# Initialize the Airflow database
airflow db upgrade

# Start the webserver and scheduler in the background
airflow webserver --port 8080 &
airflow scheduler &

# Create admin user (only if it doesn't already exist)
airflow users create \
    --username admin \
    --password admin123 \
    --firstname YourFirstName \
    --lastname YourLastName \
    --role Admin \
    --email your@email.com
