import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from bs4 import BeautifulSoup
import requests
from utils import save_to_s3, chunk


# Define the DAG using the with statement
with DAG(
    dag_id="jora_crawler",
    start_date=datetime(2024, 1, 31),
    description="a dag to crawl data engineer job Sydney in jora",
    schedule_interval=timedelta(days=1),
    tags=["crawler", "jora"],
) as dag:

    @task
    def get_job_description_link():
        raw_url="https://au.jora.com/j?sp=homepage&trigger_source=homepage&q=Data+Engineer&l=sydney"
        out_hrefs = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        def _get_job_dfs(url, hrefs, depth, stop=1e9):
            response = requests.get(url, headers=headers)
            print(f"START CRAWL WITH DEPTH: {depth}")
            if depth >= stop:
                return

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
                div_next_page = soup.find('div', class_='multi-pages-pagination pagination-container')
                if div_next_page:
                    next_page_buttons = div_next_page.find_all('a', class_='next-page-button')
                    if next_page_buttons:
                        for next_page_button in next_page_buttons:
                            time.sleep(3)
                            _get_job_dfs(f"https://au.jora.com{next_page_button.get('href')}", hrefs, depth + 1, stop)
            else:
                print(f"Failed to fetch the page. Status code: {response.status_code}")

        _get_job_dfs(raw_url, out_hrefs, 0, 3)
        print(f"len out hrefs: {len(out_hrefs)}")
        return chunk(out_hrefs)

    @task
    def get_job_description(urls):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        out_dict = []
        for url in urls:
            try:
                job_info = dict()
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    job_info_container = soup.find("div", id="job-info-container")
                    job_info = job_info_container.get_text() if job_info_container else ""
                    role_ele = job_info_container.find("h1", class_="job-title heading-xxlarge")
                    company_ele = job_info_container.find("span", class_="company")
                    location_ele = job_info_container.find("span", class_="location")
                    contract_type_ele = job_info_container.find("div", class_="badge -default-badge")
                    listed_date_ele = job_info_container.find("span", class_="listed-date")
                    job_info["role"] = role_ele.get_text() if role_ele else ""
                    job_info["company"] = company_ele.get_text() if company_ele else ""
                    job_info["location"] = location_ele.get_text() if location_ele else ""
                    job_info["contract_type"] = contract_type_ele.get_text() if contract_type_ele else ""
                    job_info["listed_date"] = listed_date_ele.get_text() if listed_date_ele else ""
                job_description_div = soup.find('div', id='job-description-container')
                job_description = job_description_div.get_text(separator='\n',
                                                               strip=True) if job_description_div else ""
                out_dict.append({"crawled_url": url,
                        "crawled_website": "jora",
                        "job_info": str(job_info),
                        "job_description": job_description})
            except Exception as e:
                print(f"get job description fail with error: {e}")
                out_dict.append({"crawled_url": url,
                        "crawled_website": "jora",
                        "job_info": "",
                        "job_description": ""})
        return out_dict

    job_description_link = get_job_description_link()
    job_description = get_job_description.expand(urls=job_description_link)
    save_to_s3.expand(list_data=job_description)