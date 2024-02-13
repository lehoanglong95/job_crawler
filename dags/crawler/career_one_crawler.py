import time

from pendulum import datetime
from base_dag import (
    DAG
)

with DAG(
    dag_id="careerone_crawler",
    start_date=datetime(2024, 2, 11),
    description="a dag to crawl data engineer job Sydney in careerone",
    schedule_interval="0 0 * * *",
    concurrency=8,
    max_active_tasks=3,
    tags=["crawler", "careerone"],
) as dag:

    from typing import List
    from pendulum import now
    import requests
    import re
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from jora_job_description_extraction import (
        JobInfoForDB
    )
    from utils import (
        save_job_metadata_to_postgres,
        get_crawled_website_id,
    )
    from constant import (
        job_crawler_postgres_conn,
    )

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    @task
    def get_payload() -> List[dict]:
        def de_syd_payload(page):
            return {
                'search_keywords': 'data engineer',
                'search': 'data engineer',
                'sort_by': '',
                'job_type': [],
                'categories': [],
                'skills': [],
                'source_code': [],
                'equal_opportunity_tags': [],
                'hiring_site': [],
                'hiring_platform': [],
                'ad_type': [],
                'posted_within_days': {
                    'days': 0,
                    'value': 'Any time',
                },
                'keywords': [],
                'sector': [],
                'job_title': [],
                'industry': [],
                'company_size': [],
                'job_mode': [],
                'contract_type': [],
                'career_level': [],
                'perks': [],
                'work_authorisation': [],
                'education_level': [],
                'languages': [],
                'licenses': [],
                'certifications': [],
                'pay_max': '',
                'pay_min': '',
                'brands': [],
                'employer_name': '',
                'location': {
                    'id': 15279,
                    'type': 'REGION',
                    'label': 'All Sydney NSW',
                    'display_label': 'Sydney NSW',
                    'region_name': 'Sydney NSW',
                    'area_name': '',
                    'state_name': 'New South Wales',
                    'suburb_name': '',
                    'suburb_location_id': 0,
                    'area_location_id': 0,
                    'region_location_id': 15279,
                    'state_location_id': 15295,
                    'country_location_id': 15299,
                    'state_code': 'NSW',
                    'country_name': 'Australia',
                    'country_code': 'AU',
                    'post_code': '',
                    'slug': 'sydney-nsw',
                    'meta_robots': 'index',
                },
                'include_surrounding_location': True,
                'page': page,
                'resultsPerPage': 20,
                'parsed_filter': '1',
                'parsed': {
                    'job_title': [
                        {
                            'id': '4166',
                            'title': 'Data Engineer',
                        },
                    ],
                    'search_phrase': '',
                },
                'locale': 'AU',
                'bucket_code': 'ORGANIC,PRIORITISE',
            }

        def ai_eng_syd_payload(page):
            return {"search_keywords": "ai engineer", "search": "ai engineer", "sort_by": "", "job_type": [],
                    "categories": [], "skills": [], "source_code": [], "equal_opportunity_tags": [], "hiring_site": [],
                    "hiring_platform": [], "ad_type": [], "posted_within_days": {"days": 0, "value": "Any time"},
                    "keywords": [], "sector": [], "job_title": [], "industry": [], "company_size": [], "job_mode": [],
                    "contract_type": [], "career_level": [], "perks": [], "work_authorisation": [],
                    "education_level": [], "languages": [], "licenses": [], "certifications": [], "pay_max": "",
                    "pay_min": "", "brands": [], "employer_name": "",
                    "location": {"id": 15279, "type": "REGION", "label": "All Sydney NSW",
                                 "display_label": "Sydney NSW", "region_name": "Sydney NSW", "area_name": "",
                                 "state_name": "New South Wales", "suburb_name": "", "suburb_location_id": 0,
                                 "area_location_id": 0, "region_location_id": 15279, "state_location_id": 15295,
                                 "country_location_id": 15299, "state_code": "NSW", "country_name": "Australia",
                                 "country_code": "AU", "post_code": "", "slug": "sydney-nsw", "meta_robots": "index"},
                    "include_surrounding_location": True, "page": page, "resultsPerPage": 20, "parsed_filter": "1",
                    "parsed": {"job_title": [{"id": "85520", "title": "AI Engineer"},
                                             {"id": "85520", "title": "Artificial Intelligence Engineer"},
                                             {"id": "85520", "title": "Artificial Inteligence Engineer"},
                                             {"id": "85520", "title": "Artificial Intelligence (AI) Engineer"},
                                             {"id": "85520", "title": "Artificial Inteligence (AI) Engineer"}],
                               "search_phrase": ""}, "locale": "AU", "bucket_code": "ORGANIC,PRIORITISE"}
        return [
            {
                "payload": de_syd_payload,
                "searched_location": "Sydney",
                "searched_role": "data engineer"

            },
            # {
            #     "payload": ai_eng_syd_payload,
            #     "searched_location": "Sydney",
            #     "searched_role": "AI Engineer"
            # }
        ]

    @task
    def get_job_descriptions(
        payloads: List[dict],
        url="https://seeker-api.careerone.com.au/api/v1/search-job",
    ):
        job_descriptions = []
        headers = {
            'authority': 'seeker-api.careerone.com.au',
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
            'content-type': 'application/json',
            'origin': 'https://www.careerone.com.au',
            'platform-code': 'careerone',
            'referer': 'https://www.careerone.com.au/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'site-code': 'careerone',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
        }

        def _calculate_number_of_pages(payload: dict) -> int:
            response = requests.post('https://seeker-api.careerone.com.au/api/v1/search-job', headers=headers, json=payload)
            no_of_pages = 0
            if response.status_code == 200:
                data = response.json()
                result_per_page = data.get("search_filters", {}).get("resultsPerPage", 0)
                job_count = data.get("search_results", {}).get("job_count", 0)
                if result_per_page and job_count:
                    no_of_pages = job_count // result_per_page + 1
            return no_of_pages

        for payload in payloads:
            # get job description links per search page
            number_of_pages = _calculate_number_of_pages()
            for page_number in range(1, number_of_pages):
                time.sleep(5)
                res = requests.post(f"https://seeker-api.careerone.com.au/api/v1/search-job",
                                    headers=headers,
                                    json=payload["payload"](page_number))
                if res.status_code == 200:
                    data = res.json()
                    search_results = data.get("search_results", {})
                    location = data.get("search_filters", {}).get("location", {})
                    jobs = search_results.get("jobs", [])
                    for job in jobs:
                        job_descriptions.append({"url": url,
                                                 "location": location,
                                                 "job_description": job,
                                                 "searched_location": payload["searched_location"],
                                                 "searched_role": payload["searched_role"]})

            return job_descriptions

    @task
    def extract_job_description(data: dict):

        url = data.get("url", "")
        website_id_dict = get_crawled_website_id(pg_hook)
        location = data.get("location", {})
        job_description = data.get("job_description", {})

        def get_skills():
            inner_skills = []
            skills_details = job_description.get("skills_details", [])
            for skill in skills_details:
                skill_detail = skill.get("value", "")
                if skill_detail:
                    inner_skills.append(skill_detail)
            return inner_skills

        def calculate_listed_date():
            date_label = job_description.get("date_label", "")
            if date_label:
                match = re.search(r'\d+', date_label)
                if match:
                    number = int(match.group())
                    if "week" in date_label:
                        return now().subtract(weeks=number).format("YYYY-MM-DD")
                    elif "day" in date_label:
                        return now().subtract(days=number).format("YYYY-MM-DD")
                    else:
                        return None
            return None

        city = location.get("region_name", "")
        role = job_description.get("job_title")
        company = job_description.get("company_name")
        min_salary = job_description.get("pay_min_normalised")
        max_salary = job_description.get("pay_max_normalised")
        state = location.get("state_name", "")
        crawled_website = "careerone"
        listed_date = calculate_listed_date()
        career_levels = job_description.get("career_level_label", [])
        job_type = "on-site"
        contract_type = job_description.get("contract_type_label", "permanent")
        skills = get_skills()
        crawled_website_id = website_id_dict.get(crawled_website, -1)
        job_info_for_db = JobInfoForDB(**{
                    "url": url,
                    "location": f"{city} {state}",
                    "role": role,
                    "company": company,
                    "listed_date": listed_date,
                    "min_salary": min_salary,
                    "max_salary": max_salary,
                    "contract_type": contract_type,
                    "raw_content_file": "",
                    "crawled_website": crawled_website,
                    "career_levels": career_levels,
                    "job_type": job_type,
                    "skills": skills,
               })
        json_data = job_info_for_db.dict()
        json_data["crawled_website_id"] = crawled_website_id
        json_data["searched_location"] = data.get("searched_location", "")
        json_data["searched_role"] = data.get("searched_role", "")
        return [json_data]

    job_description = get_job_descriptions()
    job_metadata = extract_job_description.expand(data=job_description)
    save_job_metadata_to_postgres.partial(pg_hook=pg_hook).expand(list_data=job_metadata)
