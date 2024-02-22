from datetime import datetime
from base_dag import (
    DAG
)

# Define the DAG using the with statement
with DAG(
        dag_id="jora_crawler",
        start_date=datetime(2024, 2, 11),
        description="a dag to crawl data engineer job Sydney in jora",
        schedule_interval="0 */6 * * *",
        concurrency=8,
        max_active_tasks=2,
        tags=["crawler", "jora"],
) as dag:
    from typing import List, Set
    from random import randint
    import time
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from bs4 import BeautifulSoup
    import requests
    from utils import (
        save_to_s3,
        chunk,
        save_job_metadata_to_postgres,
        get_crawled_urls,
        hash_string,
    )
    from constant import (
        job_crawler_postgres_conn,
        jora_searched_sydney,
        jora_searched_melbourne,
        jora_searched_ai_engineer,
        jora_searched_data_engineer,
        jora_searched_full_stack_developer,
        jora_searched_backend_engineer,
        jora_searched_frontend_engineer,
        jora_searched_devops_engineer,
        jora_searched_cybersecurity_engineer
    )
    from jora_job_description_extraction import (
        extract_job_description,
    )

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    locations = [
        jora_searched_sydney,
        jora_searched_melbourne
    ]

    roles = [
        [
            jora_searched_data_engineer,
            jora_searched_ai_engineer,
            jora_searched_full_stack_developer,
            jora_searched_backend_engineer,
        ],
        [
            jora_searched_frontend_engineer,
            jora_searched_devops_engineer,
            jora_searched_cybersecurity_engineer,
        ]
    ]
    @task
    def get_searched_dicts(**context) -> List[dict]:
        execution_date = context['execution_date']
        print(f"DAY: {execution_date.day}")
        print(f"HOUR: {execution_date.hour}")
        if int(execution_date.day) % 4 == 0 or int(execution_date.day) % 4 == 1:
            location = locations[0]
            if int(execution_date.hour) == 0:
                role = roles[0][0]
            elif int(execution_date.hour) == 6:
                role = roles[0][1]
            elif int(execution_date.hour) == 12:
                role = roles[0][2]
            elif int(execution_date.hour) == 18:
                role = roles[0][3]
            else:
                rd = randint(0, 3)
                role = roles[0][rd]
        else:
            location = locations[1]
            if int(execution_date.hour) == 0:
                role = roles[0][0]
            elif int(execution_date.hour) == 6:
                role = roles[0][1]
            elif int(execution_date.hour) == 12:
                role = roles[0][2]
            elif int(execution_date.hour) == 18:
                role = roles[0][0]
            else:
                rd = randint(0, 3)
                role = roles[0][rd]
        print(f"LOCATION: {str(location)}")
        print(f"ROLE: {str(role)}")
        return [
            {
                "searched_role": role(),
                "searched_location": location(),
                "normalized_searched_role": str(role),
                "normalized_searched_location": str(location)
            }
        ]
        # return [
        #     {
        #         "searched_role": jora_searched_backend_engineer(),
        #         "searched_location": jora_searched_sydney(),
        #         "normalized_searched_role": str(jora_searched_backend_engineer),
        #         "normalized_searched_location": str(jora_searched_sydney)
        #     },
        #     {
        #         "searched_role": jora_searched_frontend_engineer(),
        #         "searched_location": jora_searched_sydney(),
        #         "normalized_searched_role": str(jora_searched_frontend_engineer),
        #         "normalized_searched_location": str(jora_searched_sydney)
        #     },
        #     {
        #         "searched_role": jora_searched_devops_engineer(),
        #         "searched_location": jora_searched_sydney(),
        #         "normalized_searched_role": str(jora_searched_devops_engineer),
        #         "normalized_searched_location": str(jora_searched_sydney)
        #     },
        #     {
        #         "searched_role": jora_searched_cybersecurity_engineer(),
        #         "searched_location": jora_searched_sydney(),
        #         "normalized_searched_role": str(jora_searched_cybersecurity_engineer),
        #         "normalized_searched_location": str(jora_searched_sydney)
        #     },
        #     {
        #         "searched_role": jora_searched_backend_engineer(),
        #         "searched_location": jora_searched_melbourne(),
        #         "normalized_searched_role": str(jora_searched_backend_engineer),
        #         "normalized_searched_location": str(jora_searched_melbourne)
        #     },
        #     {
        #         "searched_role": jora_searched_frontend_engineer(),
        #         "searched_location": jora_searched_melbourne(),
        #         "normalized_searched_role": str(jora_searched_frontend_engineer),
        #         "normalized_searched_location": str(jora_searched_melbourne)
        #     },
        #     {
        #         "searched_role": jora_searched_devops_engineer(),
        #         "searched_location": jora_searched_melbourne(),
        #         "normalized_searched_role": str(jora_searched_devops_engineer),
        #         "normalized_searched_location": str(jora_searched_melbourne)
        #     },
        #     {
        #         "searched_role": jora_searched_cybersecurity_engineer(),
        #         "searched_location": jora_searched_melbourne(),
        #         "normalized_searched_role": str(jora_searched_cybersecurity_engineer),
        #         "normalized_searched_location": str(jora_searched_melbourne)
        #     },
            # {
            #     "searched_role": jora_searched_data_engineer(),
            #     "searched_location": jora_searched_sydney(),
            #     "normalized_searched_role": str(jora_searched_data_engineer),
            #     "normalized_searched_location": str(jora_searched_sydney)
            # },
            # {
            #     "searched_role": jora_searched_ai_engineer(),
            #     "searched_location": jora_searched_sydney(),
            #     "normalized_searched_role": str(jora_searched_ai_engineer),
            #     "normalized_searched_location": str(jora_searched_sydney)
            # },
            # {
            #     "searched_role": jora_searched_full_stack_developer(),
            #     "searched_location": jora_searched_sydney(),
            #     "normalized_searched_role": str(jora_searched_full_stack_developer),
            #     "normalized_searched_location": str(jora_searched_sydney)
            # },
            # {
            #     "searched_role": jora_searched_data_engineer(),
            #     "searched_location": jora_searched_melbourne(),
            #     "normalized_searched_role": str(jora_searched_data_engineer),
            #     "normalized_searched_location": str(jora_searched_melbourne)
            # },
            # {
            #     "searched_role": jora_searched_ai_engineer(),
            #     "searched_location": jora_searched_melbourne(),
            #     "normalized_searched_role": str(jora_searched_ai_engineer),
            #     "normalized_searched_location": str(jora_searched_melbourne)
            # },
            # {
            #     "searched_role": jora_searched_full_stack_developer(),
            #     "searched_location": jora_searched_melbourne(),
            #     "normalized_searched_role": str(jora_searched_full_stack_developer),
            #     "normalized_searched_location": str(jora_searched_melbourne)
            # }
        # ]


    @task
    def get_job_description_link(crawled_urls: Set[str],
                                 searched_dicts: List[dict]):
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
                        url_to_searched_term_dict[hash_string(f"https://au.jora.com/job{a_tag.get('href')}")] = {"searched_location": searched_location,
                                                                       "searched_role": searched_role}
                div_next_page = soup.find('div', class_='multi-pages-pagination pagination-container')
                if div_next_page:
                    next_page_buttons = div_next_page.find_all('a', class_='next-page-button')
                    if next_page_buttons:
                        for next_page_button in next_page_buttons:
                            time.sleep(3)
                            _get_job_dfs(url=f"https://au.jora.com{next_page_button.get('href')}",
                                         hrefs=hrefs,
                                         depth=depth + 1,
                                         searched_location=searched_location,
                                         searched_role=searched_role,
                                         stop=stop)
            else:
                print(f"Failed to fetch the page. Status code: {response.status_code}")

        for searched_dict in searched_dicts:
            s_role = searched_dict["searched_role"]
            s_location = searched_dict["searched_location"]
            normalized_s_role = searched_dict["normalized_searched_role"]
            normalized_s_location = searched_dict["normalized_searched_location"]
            raw_url = f"https://au.jora.com/j?sp=homepage&trigger_source=homepage&q={s_role}&l={s_location}"
            _get_job_dfs(url=raw_url,
                         hrefs=out_hrefs,
                         depth=0,
                         searched_location=normalized_s_location,
                         searched_role=normalized_s_role)
        out_hrefs = set(out_hrefs).difference(set(crawled_urls))
        out_hrefs = list(out_hrefs)
        print(f"len out hrefs: {len(out_hrefs)}")
        out = [{"url": url,
                "searched_location": url_to_searched_term_dict[hash_string(url)]["searched_location"],
                "searched_role": url_to_searched_term_dict[hash_string(url)]["searched_role"]} for url in out_hrefs]
        return chunk(out, number_of_chunks=50)


    @task
    def get_job_description(list_data: List[dict]):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        out_dict = []
        print(f"LEN IN: {len(list_data)}")
        for data in list_data:
            url = data["url"]
            time.sleep(10)
            job_info = dict()
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                job_info_container = soup.find("div", id="job-info-container")
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
                                 "job_info": job_info,
                                 "job_description": job_description,
                                 "searched_location": data["searched_location"],
                                 "searched_role": data["searched_role"]})
        print(f"LEN OUT: {len(out_dict)}")
        return out_dict


    crawled_urls = get_crawled_urls(
        crawled_website_name="jora",
        pg_hook=pg_hook,
    )
    searched_dicts = get_searched_dicts()
    job_description_link = get_job_description_link(crawled_urls=crawled_urls,
                                                    searched_dicts=searched_dicts)
    job_descriptions = get_job_description.expand(list_data=job_description_link)
    job_descriptions_from_s3 = save_to_s3.expand(list_data=job_descriptions)
    extracted_job_descriptions = extract_job_description.partial(pg_hook=pg_hook) \
        .expand(list_data=job_descriptions_from_s3)
    save_job_metadata_to_postgres.partial(pg_hook=pg_hook) \
        .expand(list_data=extracted_job_descriptions)
