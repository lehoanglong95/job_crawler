#!/bin/bash

set -e

if [ "$(git diff master..HEAD --name-only | grep requirements.txt)" ]; then
  echo "Requirements.txt has changed. Building Docker image and starting services..."
  cd /home/ubuntu/my_custom_airflow
  docker build . --tag my_custom_airflow_with_requirements
  docker-compose up
else
  echo "No changes in requirements.txt. Skipping Docker build and service restart."
fi
