#!/bin/bash

cd /home/ec2-user/job_crawler

docker build . --build-arg SMTP_USER="$SMTP_USER" --build-arg SMTP_PW="$SMTP_PW" --tag my_custom_airflow_with_requirements
docker-compose up -d
