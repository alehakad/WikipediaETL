#!/bin/bash

# start hadoop
start-all.sh

# create folder for pages text
hadoop fs -mkdir /user
hadoop fs -mkdir /user/html_texts/

# Set local instance of Airflow
export AIRFLOW_HOME=$(pwd)

# Initialize the Airflow database
airflow standalone