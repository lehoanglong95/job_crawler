#!/bin/bash

#set -e

#cd /home/ec2-user/job_crawler
files=$(ls)

# Echo the captured output
echo "List of files in the current directory:"
echo "$files"
docker build . --tag my_custom_airflow_with_requirements
docker-compose up
