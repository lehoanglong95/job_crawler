from pendulum import datetime
import requests
from bs4 import BeautifulSoup
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from utils import save_to_s3, chunk

with DAG(
    dag_id="seek_crawler",
    start_date=datetime(2024, 1, 31),
    description="a dag to crawl data engineer job Sydney in seek",
    schedule_interval=timedelta(hours=3),
    tags=["crawler", "seek"],
) as dag:

    @task
    def get_job_description_link(url="https://www.seek.com.au/data-engineer-jobs/in-All-Sydney-NSW"):
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
        return chunk(out_hrefs)

    @task
    def get_job_description(urls):
        print(f"START TO CRAWL JOB DESCRIPTION: {url}")
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
                    job_infos = {"role": role, "company": company}
                    if job_info_eles:
                        for job_info_ele in job_info_eles:
                            # job_info_ele = job_info_ele.find("span", class_="_1wkzzau0 a1msqi4y a1msqir")
                            # if job_info_ele:
                            job_infos["other job info"] = job_info_ele.get_text()
                    listed_date_ele = soup.find("span", class_="_1wkzzau0 a1msqi4y lnocuo0 lnocuo1 lnocuo22 _1d0g9qk4 lnocuoa")
                    if listed_date_ele:
                        job_infos["listed date"] = listed_date_ele.get_text()
                    job_description_ele = soup.find("div", class_="_1wkzzau0 _1pehz540")
                    job_description = job_description_ele.get_text(separator='\n',
                                                                   strip=True) if job_info_ele else ""
                    out_dict.append({"crawled_url": url,
                            "crawled_website": "seek",
                            "job_info": "\n".join(job_infos),
                            "job_description": job_description})
            except Exception as e:
                print(f"get job description fail with error: {e}")
                out_dict.append({"crawled_url": url,
                        "crawled_website": "seek",
                        "job_info": "",
                        "job_description": ""})
        return out_dict

    job_description_link = get_job_description_link()
    job_description = get_job_description.expand(urls=job_description_link)
    save_to_s3.expand(list_data=job_description)