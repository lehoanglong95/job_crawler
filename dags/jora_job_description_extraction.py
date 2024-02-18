from typing import List
from airflow.decorators import task
from langchain_core.pydantic_v1 import BaseModel, Field, validator
from enum import Enum


class ContractType(str, Enum):
    Full_Time = "full time"
    Part_Time = "part time"

class CareerLevel(str, Enum):
    Internship = "internship"
    Entry_Level = "entry level"
    Associate = "associate"
    Mid_Level = "mid level"
    Senior_Level = "senior level"
    Director = "director"
    Executive = "executive"

class JobType(str, Enum):
    On_Site = "on site"
    Hybrid = "hybrid"
    Remote = "remote"

class JobInfoInput(BaseModel):
    url: str = Field(description="url of crawled website", default=None)
    location: str = Field(description="job location. Ex: Sydney", default=None)
    role: str = Field(description="role is mentioned in job info. Ex: Data Engineer, Backend Engineer", default=None)
    company: str = Field(description="company name", default=None)
    listed_date: str = Field(description="relative time compared to today this job is posted. Ex: 14 days ago, 2 weeks ago", default=None)
    salary: int = Field(description="amount of money the company can pay per year", default=None)
    min_salary: int = Field(description="minimum amount of money the company pay per year", default=None)
    max_salary: int = Field(description="maximum amount of money the company can pay per year", default=None)
    contract_type: str = Field(enum=["full time", "part time"],
                               description="contract type is mention in job description. Ex: full time, part time",
                               default=ContractType.Full_Time.value)
    number_of_experience: int = Field(description="number of experience this job requries",
                                      default=1)
    job_type: str = Field(enum=["on site", "hybrid", "remote"],
                          description="type of job. Ex: On-site, Hybrid, Remote",
                          default=JobType.On_Site.value)
    skills: List[str] = Field(description="list of skills for this role. Ex: [AWS, Airflow, Python]",
                              default=None)
    is_working_right: bool = Field(description="is working rights required for this role",
                                    default=True)

    @validator("min_salary", pre=True, always=True)
    def set_default_min_salary(cls, value, values):
        if values.get("min_salary") is None and values.get("salary") is not None:
            return values.get("salary")
        return value

    @validator("max_salary", pre=True, always=True)
    def set_default_max_salary(cls, value, values):
        if values.get("max_salary") is None:
            if values.get("salary") is not None:
                return values.get("salary")
            if values.get("min_salary") is not None:
                return values.get("min_salary")
        return value

    @validator("contract_type", pre=True, always=True)
    def set_default_contract_type(cls, value, values):
        if values.get("contract_type") not in ["full time", "part time"]:
            return "full time"
        return value

    @validator("job_type", pre=True, always=True)
    def set_default_job_type(cls, value, values):
        if values.get("job type") not in ["on site", "hybrid", "remote"]:
            return "on site"
        return value

    def post_salary_validator(self):
        if self.max_salary is not None and self.min_salary is None:
            self.min_salary = self.max_salary

class JobInfoForDB(JobInfoInput):
    listed_date_for_db: str = Field(default=None)

    @validator("listed_date_for_db", pre=True, always=True)
    def set_listed_date_for_db(cls, value, values):
        from utils import convert_listed_date_to_dateformat

        if values.get("listed_date") is not None:
            listed_date = values.get("listed_date")
            return convert_listed_date_to_dateformat(listed_date)
        return value


@task(max_active_tis_per_dagrun=5)
def extract_job_description(pg_hook, list_data: List[dict]):
    from utils import (
        get_openai_api_key_from_sm,
        get_crawled_website_id,
    )
    from langchain_openai import ChatOpenAI
    from langchain.agents import AgentExecutor, create_openai_functions_agent
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.prompts import (
        HumanMessagePromptTemplate,
        SystemMessagePromptTemplate,
        MessagesPlaceholder,
        PromptTemplate,
    )
    from langchain.tools import tool
    from pendulum import now
    from utils import (
        hash_string,
        merge_2_dicts,
        create_file_path,
    )
    import json

    openai_api_key = get_openai_api_key_from_sm()
    website_id_dict = get_crawled_website_id(pg_hook)
    llm = ChatOpenAI(
        openai_api_key=openai_api_key,
        model_name="gpt-3.5-turbo-0125",
    )

    template = ChatPromptTemplate.from_messages([
        SystemMessagePromptTemplate(prompt=PromptTemplate(input_variables=[],
                                                          template='You are AI assistant who help me extract useful data '
                                                                   'from job info and job description like location, company name, role, '
                                                                   'salary, required skills for this role')),
        HumanMessagePromptTemplate(prompt=PromptTemplate(input_variables=['input'], template='{input}')),
        MessagesPlaceholder(variable_name='agent_scratchpad')
    ])


    @tool("extract-job-info-tool", args_schema=JobInfoInput, return_direct=True)
    def extract_job_info_tool(
        url: str = None,
        location: str = None,
        role: str = None,
        company: str = None,
        listed_date: str = None,
        skills: List[str] = None,
        min_salary: int = None,
        max_salary: int = None,
        salary: int = None,
        contract_type: str = None,
        number_of_experience: int = None,
        job_type: str = None,
        is_working_right: bool = True,
    ):
        """extract job info from job description."""
        out = {
            "url": url,
            "location": location,
            "role": role,
            "company": company,
            "listed_date": listed_date,
            "min_salary": min_salary,
            "max_salary": max_salary,
            "salary": salary,
            "contract_type": contract_type,
            "job_type": job_type,
            "number_of_experience": number_of_experience,
            "skills": skills,
            "is_working_right": is_working_right
        }
        return out
    tools = [extract_job_info_tool]
    agent = create_openai_functions_agent(llm,
                                          tools,
                                          template)
    agent_executor = AgentExecutor(agent=agent,
                                   tools=tools,
                                   verbose=True)
    chunk_size = 400
    out = []
    for data in list_data:
        crawled_url_hash = hash_string(data["crawled_url"])
        file_name = f"{crawled_url_hash}.txt"
        file_path = create_file_path(data["crawled_website"],
                                     now().format("YYYY-MM-DD"),
                                     data.get("searched_location", ""),
                                     data.get("searched_role", ""),
                                     file_name)
        if not data["job_info"] and not data["job_description"]:
            print("DO NOT PROCESS")
            continue
        job_info = dict()
        job_des = f"url: {data['crawled_url']}\n\n{json.dumps(data['job_info'])}\n\n{data['job_description']}"
        print(len(job_des))
        for e in [job_des[i : i + chunk_size] for i in range(0, len(job_des), chunk_size)]:
            try:
                job_info_temp = agent_executor.invoke({"input": e})["output"]
                job_info_temp = {k: v for k, v in job_info_temp.items() if v}
                merge_2_dicts(job_info, job_info_temp)
            except Exception as e:
                print(f"call openai with error: {e}")
                continue
        job_info["role"] = data.get("job_info", {}).get("role", "") if data.get("job_info", {}).get("role", "") != "" else job_info.get("role", "")
        job_info["company"] = data.get("job_info", {}).get("company", "") if data.get("job_info", {}).get("company", "") != "" else job_info.get("company", "")
        job_info["location"] = data.get("job_info", {}).get("location", "") if data.get("job_info", {}).get("location", "") != "" else job_info.get("location", "")
        job_info["contract_type"] = data.get("job_info", {}).get("contract_type", "") if data.get("job_info", {}).get("contract_type", "") != "" else job_info.get("contract_type", "")
        job_info["listed_date"] = data.get("job_info", {}).get("listed_date", "") if data.get("job_info", {}).get("listed_date", "") != "" else job_info.get("listed_date", "")
        job_info["url"] = data["crawled_url"]
        job_info_db = JobInfoForDB(**job_info)
        job_info_db.post_salary_validator()
        job_info_db_json = job_info_db.dict()
        job_info_db_json["crawled_website_id"] = website_id_dict.get(data["crawled_website"], -1)
        job_info_db_json["raw_content_file"] = file_path
        job_info_db_json["searched_location"] = data.get("searched_location", "")
        job_info_db_json["searched_role"] = data.get("searched_role", "")
        print(f"job_info: {job_info_db_json}")
        out.append(job_info_db_json)

    return out