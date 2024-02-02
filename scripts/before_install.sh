#!/bin/bash

#set -e

cd /home/ec2-user/job_crawler_v1

docker build . --tag my_custom_airflow_with_requirements
docker-compose up
