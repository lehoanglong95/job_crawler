from typing import List
from pendulum import datetime
import requests
from bs4 import BeautifulSoup
from base_dag import (
    DAG
)
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import (
    save_to_s3,
    chunk,
    save_job_metadata_to_postgres,
    get_crawled_urls,
)
from constant import (
    job_crawler_postgres_conn,
)
from seek_job_description_extraction import (
    extract_job_description,
)

with DAG(
    dag_id="seek_crawler",
    start_date=datetime(2024, 2, 11),
    description="a dag to crawl data engineer job Sydney in seek",
    schedule_interval="0 5 * * *",
    concurrency=8,
    max_active_tasks=3,
    tags=["crawler", "seek"],
) as dag:

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    @task
    def get_job_description_link(
        crawled_urls: List[str],
        url="https://www.seek.com.au/data-engineer-jobs/in-All-Sydney-NSW"
    ):
        out_hrefs = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        def _get_job_dfs(url, hrefs, depth, stop=1e9):
            print(f"START CRAWL WITH DEPTH: {depth}")
            if depth >= stop:
                return
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                job_results_div = soup.find_all('div', class_='_1wkzzau0 a1msqi4y a1msqi4w')
                if job_results_div:
                    for job_result_div in job_results_div:
                        a_tags = job_result_div.find_all('a')
                        if a_tags:
                            for a_tag in a_tags:
                                href = a_tag.get("href")
                                if href:
                                    hrefs.append(f"https://www.seek.com.au{href}")
                next_ele = soup.find("li", class_="_1wkzzau0 a1msqia6 a1msqi9v a1msqiw")
                if next_ele:
                    a_tags = next_ele.find_all("a")
                    if a_tags:
                        for a_tag in a_tags:
                            next_page_href = a_tag.get("href")
                            _get_job_dfs(f"https://www.seek.com.au{next_page_href}", out_hrefs, depth + 1)
        _get_job_dfs(url, out_hrefs, 0)
        out_hrefs = list(set(out_hrefs).difference(crawled_urls))
        return chunk(out_hrefs)

    @task(max_active_tis_per_dagrun=4)
    def get_job_description(urls):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        out_dict = []
        for url in urls:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    role_ele = soup.find('h1', {'data-automation': 'job-detail-title'})
                    role = role_ele.get_text() if role_ele else ""
                    company_ele = soup.find('span', {'data-automation': 'advertiser-name'})
                    company = company_ele.get_text() if company_ele else ""
                    job_info_eles = soup.find_all('span', class_='_1wkzzau0 a1msqi4y a1msqir')
                    job_info = {"role": role, "company": company}
                    job_info["other job info"] = []
                    if job_info_eles:
                        for job_info_ele in job_info_eles:
                            # job_info_ele = job_info_ele.find("span", class_="_1wkzzau0 a1msqi4y a1msqir")
                            # if job_info_ele:
                            job_info["other job info"].append(job_info_ele.get_text())
                    listed_date_divs = soup.find("div", class_="_1wkzzau0 a1msqi6y")
                    for listed_date_div in listed_date_divs:
                        listed_date_ele = listed_date_div.find("span", class_="_1wkzzau0 a1msqi4y lnocuo0 lnocuo1 lnocuo22 _1d0g9qk4 lnocuoa")
                        if listed_date_ele:
                            job_info["listed date"] = listed_date_ele.get_text()
                    job_description_ele = soup.find("div", class_="_1wkzzau0 _1pehz540")
                    job_description = job_description_ele.get_text(separator='\n',
                                                                   strip=True) if job_info_ele else ""
                    out_dict.append({"crawled_url": url,
                            "crawled_website": "seek",
                            "job_info": job_info,
                            "job_description": job_description})
            except Exception as e:
                print(f"get job description fail with error: {e}")
                out_dict.append({"crawled_url": url,
                        "crawled_website": "seek",
                        "job_info": "",
                        "job_description": ""})
        return out_dict
    crawled_urls = get_crawled_urls(
        "seek",
        pg_hook=pg_hook,
    )
    job_description_link = get_job_description_link(crawled_urls=crawled_urls)
    job_descriptions = get_job_description.expand(urls=job_description_link)
    save_to_s3.expand(list_data=job_descriptions)
    extracted_job_descriptions = extract_job_description.partial(pg_hook=pg_hook).expand(list_data=job_descriptions)
    save_job_metadata_to_postgres.partial(pg_hook=pg_hook).expand(list_data=extracted_job_descriptions)