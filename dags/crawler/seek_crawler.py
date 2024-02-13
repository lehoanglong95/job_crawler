from base_dag import (
    DAG
)
from pendulum import datetime


with DAG(
    dag_id="seek_crawler",
    start_date=datetime(2024, 2, 11),
    description="a dag to crawl data engineer job Sydney in seek",
    schedule_interval="0 5 * * *",
    concurrency=8,
    max_active_tasks=3,
    tags=["crawler", "seek"],
) as dag:

    import time
    from typing import List, Set
    import requests
    from bs4 import BeautifulSoup
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from utils import (
        save_to_s3,
        chunk,
        save_job_metadata_to_postgres,
        get_crawled_urls,
        hash_string,
    )
    from seek_job_description_extraction import (
        extract_job_description,
    )
    from constant import (
        job_crawler_postgres_conn,
        seek_searched_sydney,
        seek_searched_melbourne,
        seek_searched_ai_engineer,
        seek_searched_data_engineer,
        seek_searched_full_stack_developer
    )

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    @task
    def get_searched_dicts() -> List[dict]:
        return [
            {
                "searched_role": seek_searched_data_engineer(),
                "searched_location": seek_searched_sydney(),
                "normalized_searched_role": str(seek_searched_data_engineer),
                "normalized_searched_location": str(seek_searched_sydney)
            },
            # {
            #     "searched_role": seek_searched_ai_engineer,
            #     "searched_location": seek_searched_sydney,
            # },
            # {
            #     "searched_role": seek_searched_full_stack_developer,
            #     "searched_location": seek_searched_sydney,
            # },
            # {
            #     "searched_role": seek_searched_data_engineer,
            #     "searched_location": seek_searched_melbourne,
            # },
            # {
            #     "searched_role": seek_searched_ai_engineer,
            #     "searched_location": seek_searched_melbourne,
            # },
            # {
            #     "searched_role": seek_searched_full_stack_developer,
            #     "searched_location": seek_searched_melbourne,
            # }
        ]

    @task
    def get_job_description_link(
        searched_dicts: List[dict],
        crawled_urls: Set[str],
    ):
        out_hrefs = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        url_to_searched_term_dict = dict()
        def _get_job_dfs(url,
                         hrefs,
                         depth,
                         searched_location,
                         searched_role,
                         stop=1e9):
            print(f"START CRAWL WITH DEPTH: {depth}")
            if depth >= stop:
                return
            print(f"URL: {url}")
            response = requests.get(url, headers=headers)
            print(f"response: {response}")
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                job_results_div = soup.find_all('div', class_='y735df0 _1iz8dgs4y _1iz8dgs4w')
                if job_results_div:
                    for job_result_div in job_results_div:
                        a_tags = job_result_div.find_all('a')
                        if a_tags:
                            for a_tag in a_tags:
                                href = a_tag.get("href")
                                if href:
                                    hrefs.append(f"https://www.seek.com.au{href}")
                                    url_to_searched_term_dict[hash_string(f"https://www.seek.com.au{href}")] = {"searched_location": searched_location,
                                                                                   "searched_role": searched_role}
                next_ele = soup.find("li", class_="y735df0 _1iz8dgsa6 _1iz8dgs9v _1iz8dgsw")
                if next_ele:
                    a_tags = next_ele.find_all("a")
                    if a_tags:
                        for a_tag in a_tags:
                            next_page_href = a_tag.get("href")
                            time.sleep(5)
                            _get_job_dfs(f"https://www.seek.com.au{next_page_href}",
                                         out_hrefs,
                                         depth + 1,
                                         searched_location,
                                         searched_role,
                                         stop)
        for searched_dict in searched_dicts:
            s_role = searched_dict["searched_role"]
            s_location = searched_dict["searched_location"]
            normalized_s_location = searched_dict["normalized_searched_location"]
            normalized_s_role = searched_dict["normalized_searched_role"]
            raw_url = f"https://www.seek.com.au/{s_role}/{s_location}"
            _get_job_dfs(raw_url,
                         out_hrefs,
                         0,
                         normalized_s_location,
                         normalized_s_role)
        out_hrefs = list(set(out_hrefs).difference(crawled_urls))
        out = [{"url": url,
                "searched_location": url_to_searched_term_dict[hash_string(url)]["searched_location"],
                "searched_role": url_to_searched_term_dict[hash_string(url)]["searched_role"]} for url in out_hrefs]
        return chunk(out)

    @task(max_active_tis_per_dagrun=4)
    def get_job_description(list_data: List[dict]):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        out_dict = []
        for data in list_data:
            # try:
            url = data.get("url")
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                role_ele = soup.find('h1', {'data-automation': 'job-detail-title'})
                role = role_ele.get_text() if role_ele else ""
                company_ele = soup.find('span', {'data-automation': 'advertiser-name'})
                company = company_ele.get_text() if company_ele else ""
                job_info_eles = soup.find_all('span', class_='y735df0 _1akoxc50 _1akoxc56')
                job_info = {"role": role, "company": company}
                job_info["other job info"] = []
                if job_info_eles:
                    for job_info_ele in job_info_eles:
                        # job_info_ele = job_info_ele.find("span", class_="_1wkzzau0 a1msqi4y a1msqir")
                        # if job_info_ele:
                        job_info["other job info"].append(job_info_ele.get_text())
                listed_date_divs = soup.find("div", class_="y735df0 _1iz8dgs6y")
                for listed_date_div in listed_date_divs:
                    listed_date_ele = listed_date_div.find("span", class_="y735df0 _1iz8dgs4y _94v4w0 _94v4w1 _94v4w22 _1wzghjf4 _94v4wa")
                    if listed_date_ele:
                        job_info["listed date"] = listed_date_ele.get_text()
                job_description_ele = soup.find("div",  {'data-automation': 'jobAdDetails'})
                job_description = job_description_ele.get_text(separator='\n',
                                                               strip=True) if job_info_ele else ""
                out_dict.append({"crawled_url": url,
                                 "crawled_website": "seek",
                                 "job_info": job_info,
                                 "job_description": job_description,
                                 "searched_location": data["searched_location"],
                                 "searched_role": data["searched_role"]})
            # except Exception as e:
            #     print(f"get job description fail with error: {e}")
            #     out_dict.append({"crawled_url": url,
            #             "crawled_website": "seek",
            #             "job_info": "",
            #             "job_description": "",
            #             "searched_location": data.get("searched_location"),
            #             "searched_role": data.get("searched_role")})
        return out_dict
    crawled_urls = get_crawled_urls(
        "seek",
        pg_hook=pg_hook,
    )
    searched_dicts = get_searched_dicts()
    job_description_link = get_job_description_link(crawled_urls=crawled_urls,
                                                    searched_dicts=searched_dicts)
    job_descriptions = get_job_description.expand(list_data=job_description_link)
    save_to_s3.expand(list_data=job_descriptions)
    extracted_job_descriptions = extract_job_description.partial(pg_hook=pg_hook).expand(list_data=job_descriptions)
    save_job_metadata_to_postgres.partial(pg_hook=pg_hook).expand(list_data=extracted_job_descriptions)