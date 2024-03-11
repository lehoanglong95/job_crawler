# Job Analytics Project

![Job Analytic Website](https://iili.io/JW2EYHg.png)

## Overview

This project crawls over 10,000 job descriptions for popular roles such as data engineer, AI engineer, data analyst, etc., from various popular job search platforms in Australia between December 2023 and February 2024. After collecting the data, it conducts analytics to identify skill demand for each job role, average salary by title, job postings per day, and more. The aim is to provide job seekers with an overview of the job market in Australia. You can find the Job Analytics platform [here](http://jobanainau-lb-1729811373.ap-southeast-2.elb.amazonaws.com/superset/dashboard/analytic/?native_filters_key=OlT_rxHXGjG9mZkvvve6Yo1aggsakiQnP2kbdVDhaMLulArEwzKvEGvM0h8wroLQ).

## Installation

To run this project locally, follow these steps:

```bash
docker build . --build-arg SMTP_USER="$SMTP_USER" --build-arg SMTP_PW="$SMTP_PW" --tag my_custom_airflow_with_requirements
docker-compose up -d
```


## Overview Architecture

1. **Crawler Setup**: The project sets up a crawler to regularly fetch job descriptions and saves them to a data lake (S3).
2. **Data Extraction**: Utilizes Langchain and LLM to extract useful data such as demand skills, job titles, salary, and location from the job descriptions.
3. **Data Warehousing**: The extracted data is stored in a data warehouse (PostgreSQL).
4. **Data Visualization**: Data is visualized using Superset to provide insightful analytics.
5. **Public Dashboard**: The analytics are presented in a public dashboard on the website.

# 🐉 About Me

## Hi, I'm Long - A Data Analyst! 💻

## Author

- [@Long H Le](https://github.com/https://github.com/lehoanglong95)


## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://github.com/https://github.com/lehoanglong95)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/hoang-long-le-713b41111/)