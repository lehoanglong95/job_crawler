import os
import hashlib

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Any
import json
import boto3

def normalize_text(input: str) -> str:
    return input.lower().strip()


def hash_string(input_string):
    # Create a new SHA-256 hash object
    sha256_hash = hashlib.sha256()

    # Update the hash object with the input string
    sha256_hash.update(input_string.encode('utf-8'))

    # Get the hexadecimal representation of the hash
    hashed_string = sha256_hash.hexdigest()

    return hashed_string


def chunk(input: List[Any], number_of_chunks=50):
    chunk_size = len(input) // number_of_chunks
    return [input[i:i+chunk_size] for i in range(0, len(input), chunk_size)]


@task
def save_to_s3(list_data: List[dict]):
    print(f"LEN IN: {len(list_data)}")
    cnt = 0
    for data in list_data:
        if not data["job_info"] and not data["job_description"]:
            print("DO NOT PROCESS")
            continue
        # try:
        crawled_url_hash = hash_string(data["crawled_url"])
        file_name = f"{crawled_url_hash}.txt"
        file_path = os.path.join(data["crawled_website"], file_name)

        # Check if the output folder exists; if not, create it
        if not os.path.exists(data["crawled_website"]):
            os.makedirs(data["crawled_website"])

        combination_text = f"url: {data['crawled_url']}\n\n{data['job_info']}\n\n{data['job_description']}"

        # Write content to the file
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(combination_text)

        print(f"file exist: {file_name in os.listdir()}")
        s3 = boto3.client('s3')
        s3.upload_file(file_name, "lhl-job-descriptions", file_path)
        print("do not remove file after upload")
        cnt += 1
    print(f"LEN OUT: {cnt}")
            # time.sleep(10)
            # os.remove(file_name)
        # except Exception as e:
        #     print(f"create file fail with error: {e}")

def merge_2_dicts(dict_a, dict_b):
    for key, value in dict_b.items():
        if key in dict_a:
            if isinstance(value, list) and isinstance(dict_a[key], list):
                dict_a[key].extend(value)
            else:
                dict_a[key] = value
        else:
            dict_a[key] = value


def get_openai_api_key_from_sm() -> str:
    openai_api_key_name = "job-crawler-openai-api-key"
    region_name = "ap-southeast-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    secrets_manager_client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    #retrieve the openai api key value
    secret_response = secrets_manager_client.get_secret_value(SecretId=openai_api_key_name)
    secret_data = secret_response['SecretString']
    secret_dict = json.loads(secret_data)
    openai_api_key = secret_dict["open_api_key"]
    return openai_api_key


def get_crawled_website_id() -> dict:
    pg_hook = PostgresHook(postgres_conn_id='postgres_job_crawler_conn_id', schema='jobs')
    query = "SELECT id, website_name FROM crawled_website"

    df = pg_hook.get_pandas_df(sql=query)
    website_names = df["website_name"].tolist()
    ids = df["id"].tolist()

    # Create a dictionary with website_name as key and id as value
    website_dict = {website_name: id for id, website_name in zip(ids, website_names)}
    return website_dict


@task(max_active_tis_per_dagrun=4)
def save_job_metadata_to_postgres(list_data: List[dict]):
    pg_hook = PostgresHook(postgres_conn_id='postgres_job_crawler_conn_id', schema='jobs')
    insert_job_metadata_sql = """
    INSERT INTO job_metadata (
        crawled_website_id, url, city, role, company, listed_date, min_salary, max_salary, state, 
        contract_type, number_of_experience, job_type, is_working_rights, raw_content_file
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """

    insert_skills_query = "INSERT INTO skills (job_id, skill) VALUES (%s, %s)"

    for data in list_data:
        job_metadata_values = (
            data["crawled_website_id"], data['url'], data['city'], data['role'],
            data['company'], data['listed_date'], data['min_salary'],
            data['max_salary'], data['state'], data['contract_type'],
            data["number_of_experience"], data['job_type'],
            data['is_working_right'], data['raw_content_file'],
        )
        job_metadata_id = pg_hook.get_first(insert_job_metadata_sql, parameters=job_metadata_values)[0]
        if data['skills']:
            skills_values = [(job_metadata_id, skill) for skill in data['skills']]
            pg_hook.run(insert_skills_query, parameters=skills_values)