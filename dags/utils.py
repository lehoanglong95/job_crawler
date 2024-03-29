from airflow.decorators import task
from pendulum import now
import os
from enum import Enum


class Level(str, Enum):
    Junior = "junior"
    MidLevel = "mid-level"
    Senior = "senior"
    Staff = "staff"
    Principal = "principal"
    Lead = "lead"
    Head = "head"


def get_level_from_role(role: str):
    if not role:
        return ""
    role = normalize_text(role)
    if "junior" in role:
        return Level.Junior.value
    elif "midlevel" in role or "mid-level" in role:
        return Level.MidLevel.value
    elif "senior" in role:
        return Level.Senior.value
    elif "staff" in role:
        return Level.Staff.value
    elif "principal" in role:
        return Level.Principal.value
    elif "lead" in role:
        return Level.Lead.value
    elif "head" in role:
        return Level.Head.value
    return Level.MidLevel.value

def normalize_text(input: str) -> str:
    if not input:
        return input
    return input.lower().strip()


def hash_string(input_string):
    import hashlib
    # Create a new SHA-256 hash object
    sha256_hash = hashlib.sha256()

    # Update the hash object with the input string
    sha256_hash.update(input_string.encode('utf-8'))

    # Get the hexadecimal representation of the hash
    hashed_string = sha256_hash.hexdigest()

    return hashed_string


def chunk(input, number_of_chunks=50):
    if len(input) < number_of_chunks:
        return [input[:]]
    chunk_size = len(input) // number_of_chunks
    return [input[i:i+chunk_size] for i in range(0, len(input), chunk_size)]


def is_valid_date_format(date_string):
    import re
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    return bool(re.match(pattern, date_string))


def convert_listed_date_to_dateformat(listed_date: str):
    import re
    from pendulum import now
    if listed_date is not None:
        if is_valid_date_format(listed_date):
            return listed_date
        match = re.search(r'\d+', listed_date)
        if match:
            number = int(match.group())
            if "minute" in listed_date or "minutes" in listed_date or re.search(r"\d+m", listed_date):
                listed_date_for_db = now().subtract(minutes=number).format("YYYY-MM-DD")
            elif "hour" in listed_date or "hours" in listed_date or re.search(r"\d+h", listed_date):
                listed_date_for_db = now().subtract(hours=number).format("YYYY-MM-DD")
            elif "day" in listed_date or "days" in listed_date or re.search(r"\d+d", listed_date):
                listed_date_for_db = now().subtract(days=number).format("YYYY-MM-DD")
            elif "week" in listed_date or "weeks" in listed_date or re.search(r"\d+w", listed_date):
                listed_date_for_db = now().subtract(weeks=number).format("YYYY-MM-DD")
            elif "month" in listed_date or "months" in listed_date:
                listed_date_for_db = now().subtract(months=number).format("YYYY-MM-DD")
            elif "year" in listed_date or "years" in listed_date or re.search(r"\d+y", listed_date):
                listed_date_for_db = now().subtract(years=number).format("YYYY-MM-DD")
            else:
                listed_date_for_db = None
            return listed_date_for_db
        else:
            print(listed_date)


def create_file_path(crawled_website: str,
                     date_str: str,
                     searched_location: str,
                     searched_role: str,
                     file_name: str):
    return os.path.join(crawled_website,
                        date_str,
                        searched_location,
                        searched_role,
                        file_name)

@task
def save_to_s3(list_data):
    import os
    import boto3
    import json
    from pendulum import now

    print(f"LEN IN: {len(list_data)}")
    cnt = 0
    for data in list_data:
        if not data["job_info"] and not data["job_description"]:
            print("DO NOT PROCESS")
            continue
        # try:
        crawled_url_hash = hash_string(data["crawled_url"])
        file_name = f"{crawled_url_hash}.txt"
        searched_location = data.get("searched_location")
        searched_role = data.get("searched_role")
        file_path = create_file_path(data["crawled_website"],
                                     now().format("YYYY-MM-DD"),
                                     searched_location,
                                     searched_role,
                                     file_name)

        # Check if the output folder exists; if not, create it
        if not os.path.exists(data["crawled_website"]):
            os.makedirs(data["crawled_website"])

        combination_text = f"url: {data['crawled_url']}\n\n{json.dumps(data['job_info'])}\n\n{data['job_description']}"

        # Write content to the file
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(combination_text)

        print(f"file exist: {file_name in os.listdir()}")
        s3 = boto3.client('s3')
        s3.upload_file(file_name, "lhl-job-descriptions", file_path)
        print("do not remove file after upload")
        cnt += 1
    print(f"LEN OUT: {cnt}")
    return list_data
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
    import boto3
    import json

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


def get_crawled_website_id(pg_hook) -> dict:
    query = "SELECT id, website_name FROM crawled_website"

    df = pg_hook.get_pandas_df(sql=query)
    website_names = df["website_name"].tolist()
    ids = df["id"].tolist()

    # Create a dictionary with website_name as key and id as value
    website_dict = {website_name: id for id, website_name in zip(ids, website_names)}
    return website_dict


@task(max_active_tis_per_dagrun=4)
def save_job_metadata_to_postgres(
        pg_hook,
        list_data,
    ):

    from pendulum import now
    from uuid import uuid4

    insert_job_metadata_sql = """
    INSERT INTO job_metadata (
        id, crawled_website_id, url, location, role, company, listed_date, raw_listed_date, min_salary, max_salary, 
        contract_type, number_of_experience, job_type, is_working_right, raw_content_file, crawled_date,
        searched_location, searched_role, level
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (crawled_website_id, location, role, company, listed_date, contract_type) DO UPDATE
    SET
        url = EXCLUDED.url,
        location = EXCLUDED.location,
        role = EXCLUDED.role,
        company = EXCLUDED.company,
        listed_date = EXCLUDED.listed_date,
        raw_listed_date = EXCLUDED.raw_listed_date,
        min_salary = EXCLUDED.min_salary,
        max_salary = EXCLUDED.max_salary,
        contract_type = EXCLUDED.contract_type,
        number_of_experience = EXCLUDED.number_of_experience,
        job_type = EXCLUDED.job_type,
        is_working_right = EXCLUDED.is_working_right,
        raw_content_file = EXCLUDED.raw_content_file,
        crawled_date = EXCLUDED.crawled_date,
        searched_location = EXCLUDED.searched_location,
        searched_role = EXCLUDED.searched_role,
        level = EXCLUDED.level
    RETURNING id
    """

    insert_skills_query = "INSERT INTO skills (job_id, skill) VALUES (%s, %s)"

    for data in list_data:
        job_metadata_id = str(uuid4())
        data["listed_date_for_db"] = convert_listed_date_to_dateformat(data.get("listed_date", None))
        print(f"listed_date_for_db: {data['listed_date_for_db']}")
        job_metadata_values = (
            job_metadata_id, data["crawled_website_id"], normalize_text(data['url']),
            normalize_text(data.get('location', "")), normalize_text(data.get('role', "")),
            normalize_text(data.get('company', "")), data.get('listed_date_for_db', None),
            data.get("listed_date", None), data.get('min_salary', None),
            data.get('max_salary', None), normalize_text(data.get('contract_type', "")),
            data.get("number_of_experience", None), normalize_text(data.get('job_type', "")),
            data.get('is_working_right', True), normalize_text(data.get('raw_content_file', '')),
            now().format("YYYY-MM-DD"), normalize_text(data.get("searched_location", "")),
            normalize_text(data.get("searched_role", "")), get_level_from_role(data.get("role", ""))
        )
        job_metadata_id = pg_hook.get_first(insert_job_metadata_sql, parameters=job_metadata_values)[0]
        print(job_metadata_id)
        if data['skills']:
            skills = [normalize_text(str(e)) for e in data["skills"]]
            skills = set(skills)
            skills_values = [(job_metadata_id, skill) for skill in skills]
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_skills_query, skills_values)
                    conn.commit()

@task
def get_crawled_urls(crawled_website_name: str,
                    pg_hook) -> set:
    website_dict = get_crawled_website_id(pg_hook)
    crawled_website_id = website_dict.get(crawled_website_name)
    if crawled_website_id:
        df = pg_hook.get_pandas_df(f"SELECT * FROM job_metadata WHERE crawled_website_id = {crawled_website_id}")
        return set(df["url"].tolist())
    else:
        print(f"website dict: {website_dict}")
        print(f"crawled website name: {crawled_website_name}")


def categorize_it_role(role):
    from constant import Role
    categories = {
        Role.data_engineer: [
            ["data", "engineer"],
            ["data", "governance"],
            ["snowflake"],
            ["hadoop"]
        ],
        Role.ai_engineer: [
            ["ai"],
            ["ml"],
            ["machine", "learning"],
            ["computer vision"],
            ["computer-vision"],
            ["nlp"]
        ],
        Role.data_analyst: [
            ["data", "analyst"],
            ["finance", "data"],
            ["digital"],
            ["bi"],
            ["visualization"],
            ["business"],
            ["analytics"]
        ],
        Role.data_scientist: [
            ["scientist"],
        ],
        Role.backend_engineer: [
            ["backend"],
            ["back end"],
            ["software", "engineer"],
            ["c#"],
            ["python"],
            ["java"],
            [".net"],
            ["go lang"],
            ["golang"],
            ["api"],
            ["compiler"],
            ["c++"],
            ["back-end"],
            ["back - end"],
            ["php"]
        ],
        Role.frontend_engineer: [
            ["frontend"],
            ["angular"],
            ["front end"],
            ["react"],
            ["front", "end"]
        ],
        Role.fullstack_engineer: [
            ["fullstack"],
            ["full stack"],
            ["javascript"],
            ["web"],
            ["node"],
            ["full-stack"]
        ],
        Role.devops_engineer: [
            ["aws"],
            ["azure"],
            ["gcp"],
            ["devops"],
            ["cloud"],
            ["infrastructure"],
            ["platform"],
            ["network"],
            ["system"],
            ["integration"],
            ["dev", "ops"],
            ["sysops"],
            ["sys", "ops"],
            ["devsecops"],
            ["integrity"],
            ["kubernetes"],
            ["splunk"],
            ["site reliability engineer"]
        ],
        Role.cyber_security_engineer: [
            ["security"],
            ["fraud"],
            ["cyber"],
            ["risk"]
        ],
        Role.qa_qc_engineer: [
            ["test"],
            ["qa"],
            ["qc"]
        ],
        Role.data_architect: [
            ["architect"],
            ["architecture"]
        ],
        Role.recruiter: [
            ["recruitment"]
        ],
        Role.database_engineer: [
            ["oracle"],
            ["postgres"],
            ["mysql"],

        ],
        Role.designer: [
            ["design"],
            ["graphic"],
            ["ux/ui"],
        ],
        Role.ios_engineer: [
            ["ios"]
        ],
        Role.android_engineer: [
            ["android"]
        ],
        Role.project_manager: [
            ["project", "manager"]
        ]
    }

    for category, substrings in categories.items():
        for substring in substrings:
            if all(word.lower() in role.lower() for word in substring):
                return category
    categories_1 = {
        Role.data_engineer: [
            ["data"],
            ["etl"],
            ["elt"]
        ],
        Role.backend_engineer: [
            ["developer"],
            ["engineering"],
            ["senior engineer"],
            ["lead engineer"],
            ["principal engineer"],
            ["technology"],
            ["technical"],
            ["software", "development"],
            ["tech lead"],
            ["programmer"],
            ["endpoint"],
            ["staff engineer"],
            ["software"],
            ["programmer"]
        ],
        Role.data_analyst: [
            ["analyst"],
        ],
        Role.ai_engineer: [
            ["research engineer"],
        ],
        Role.designer: [
            ["ui"],
            ["ux"]
        ]
    }
    for category, substrings in categories_1.items():
        for substring in substrings:
            if all(word.lower() in role.lower() for word in substring):
                return category
    return Role.uncategorized