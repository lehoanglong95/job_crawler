#!/bin/bash

cd /home/ec2-user/job_crawler

docker build . --tag my_custom_airflow_with_requirements
docker-compose up -d
