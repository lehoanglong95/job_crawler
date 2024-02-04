import os
import hashlib
import time

from airflow.decorators import task
from typing import List, Any
# from airflow.hooks.S3_hook import S3Hook
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
    for data in list_data:
        if not data["job_info"] and not data["job_description"]:
            continue
        try:
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

            s3 = boto3.client('s3')
            s3.upload_file(file_name, "lhl-job-descriptions", file_path)
            time.sleep(10)
            os.remove(file_name)
        except Exception as e:
            print(f"create file fail with error: {e}")

