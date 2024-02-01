import time
import os

from dags.utils import hash_string
import requests
from bs4 import BeautifulSoup
from typing import List

def get_job_description_link(url, hrefs, depth, stop=1e9):
    print(f"START CRAWL WITH DEPTH: {depth}")
    if depth >= stop:
        return
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)


    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        job_results_div = soup.find('div', class_='jobresults')

        if job_results_div:
            # Find all <a> tags within the job results div
            a_tags = job_results_div.find_all('a')

            # Extract and print the href attributes
            for a_tag in a_tags:
                hrefs.append(f"https://au.jora.com/job{a_tag.get('href')}")

                # if href:
                #     response = requests.get(f"https://au.jora.com/job{href}", headers=headers)
                #     if response.status_code == 200:
                #         print(response.content)
                #     break
            # print(hrefs)
        div_next_page = soup.find('div', class_='multi-pages-pagination pagination-container')
        if div_next_page:
            next_page_buttons = div_next_page.find_all('a', class_='next-page-button')
            if next_page_buttons:
                for next_page_button in next_page_buttons:
                    time.sleep(3)
                    # print(f"https://au.jora.com{next_page_button.get('href')}")
                    get_job_description_link(f"https://au.jora.com{next_page_button.get('href')}", hrefs, depth + 1, stop)
    else:
        print(f"Failed to fetch the page. Status code: {response.status_code}")

def get_job_description(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            job_info_container = soup.find("div", id="job-info-container")
            job_info = job_info_container.get_text() if job_info_container else ""
            # role_span = job_info_container.find("h1", class_="job-title heading-xxlarge")
            # role = role_span.get_text() if role_span else ""
            # company_name_span = job_info_container.find("span", class_="company")
            # company_name = company_name_span.get_text() if company_name_span else ""
            # location_span = job_info_container.find("span", class_="location")
            # location = location_span.get_text() if location_span else ""
        # job_meta = soup.find("div", id="job-meta")
        # if job_meta:
        #     listed_date_span = job_meta.find("span", class_="listed-date")
        #     listed_date = listed_date_span.get_text() if listed_date_span else ""
        # else:
        #     listed_date = ""

        job_description_div = soup.find('div', id='job-description-container')
        job_description = job_description_div.get_text(separator='\n',
                                                       strip=True) if job_description_div else ""
        return {"crawled_url": url,
                "crawled_website": "jora",
                "job_info": job_info,
                "job_description": job_description}
    except Exception as e:
        print(f"get job description fail with error: {e}")
        return {"crawled_url": url,
                "crawled_website": "jora",
                "job_info": "",
                "job_description": ""}





# @task
# def save_crawled_urls(job_metadata_list: List[dict]):
#     pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id', schema='Jobs')
#
#     # Iterate over the list and insert each job metadata into the job_metadata table
#     for job_data in job_metadata_list:
#         sql_statement = """
#             INSERT INTO job_metadata (role, company, location, listed_date, url, crawled_website, saved_file)
#             VALUES (%s, %s, %s, %s, %s, %s, %s)
#         """
#         values = (
#             job_data["url"],
#             job_data["city"],
#             job_data["role"],
#             job_data["company"],
#             job_data["listed_date"],
#             job_data["state"],
#             job_data["raw_content_file"],
#             job_data["crawled_website_id"],
#         )
#
#         pg_hook.run(sql_statement, parameters=values)
#         print(f"Inserted job metadata into PostgreSQL: {values}")
#
#
# if __name__ == '__main__':
#     # Define the URL
#     url = "https://au.jora.com/j?sp=homepage&trigger_source=homepage&q=Data+Engineer&l=sydney"
#     hrefs = []
#     # get_job_description_link(url, hrefs, 0, stop=1)
#     # print(len(hrefs))
#     # print(hrefs[0])
#     get_job_description("https://au.jora.com/job/job/Data-Engineer-e70fdda1a762629f67e27261a3085650?abstract_type=short_llm&sl=sydney&sp=sserp_top&sponsored=true&sq=Data+Engineer&sr=1&tk=UWE132jhE7hpCikSHeQS-bFwkXBDhk5eguA6cEoAQ&trigger_source=serp")