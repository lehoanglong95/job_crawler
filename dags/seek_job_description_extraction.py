from airflow.decorators import task
from typing import List
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from langchain_core.pydantic_v1 import BaseModel, Field, validator

from jora_job_description_extraction import (
    ContractType,
    JobType,
    JobInfoForDB,
)

class JobInfoInput(BaseModel):
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

    @validator("min_salary", pre=True, always=True)
    def set_default_min_salary(cls, value, values):
        if values.get("min_salary") is None or values.get("min_salary") >= 1000000:
            if values.get("salary") is not None:
                return values.get("salary") if values.get("salary") < 1000000 else None
        if values.get("min_salary") is not None and values.get("min_salary") >= 1000000:
            return None
        return value

    @validator("max_salary", pre=True, always=True)
    def set_default_max_salary(cls, value, values):
        if values.get("max_salary") is None or values.get("max_salary") >= 1000000:
            if values.get("salary") is not None and values.get("salary") < 1000000:
                return values.get("salary")
            if values.get("min_salary") is not None and values.get("min_salary") < 1000000:
                return values.get("min_salary")
        if values.get("max_salary") is not None and values.get("max_salary") >= 1000000:
            return None
        return value

    @validator("contract_type", pre=True, always=True)
    def set_default_contract_type(cls, value, values):
        if values.get("contract_type") not in ["full time", "part time"]:
            return "full time"
        return value

    def post_salary_validator(self):
        if self.max_salary is not None and self.min_salary is None:
            self.min_salary = self.max_salary


class JobDescriptionInput(BaseModel):
    number_of_experience: int = Field(description="number of experience this job requries",
                                      default=1)
    job_type: str = Field(enum=["on site", "hybrid", "remote"],
                          description="type of job. Ex: On-site, Hybrid, Remote",
                          default=JobType.On_Site.value)
    skills: List[str] = Field(description="list of skills for this role. Ex: [AWS, Airflow, Python]",
                              default=None)
    is_working_right: bool = Field(description="is working rights required for this role",
                                    default=True)

    @validator("job_type", pre=True, always=True)
    def set_default_job_type(cls, value, values):
        if values.get("job type") not in ["on site", "hybrid", "remote"]:
            return "on site"
        return value

@task(max_active_tis_per_dagrun=2)
def extract_job_description(pg_hook, list_data: List[dict]):

    import json
    import os
    from langchain.tools import tool
    from langchain_openai import ChatOpenAI
    from langchain.agents import AgentExecutor, create_openai_functions_agent
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.prompts import (
        HumanMessagePromptTemplate,
        SystemMessagePromptTemplate,
        MessagesPlaceholder,
        PromptTemplate,
    )
    from utils import (
        merge_2_dicts,
        get_openai_api_key_from_sm,
        get_crawled_website_id,
        hash_string,
        create_file_path,
    )
    from pendulum import now

    @tool("extract-job-info-tool", args_schema=JobInfoInput, return_direct=True)
    def extract_job_info_tool(
        location: str = None,
        role: str = None,
        company: str = None,
        listed_date: str = None,
        min_salary: int = None,
        max_salary: int = None,
        salary: int = None,
        contract_type: str = None,
    ):
        """extract job info."""
        out = {
            "location": location,
            "role": role,
            "company": company,
            "listed_date": listed_date,
            "min_salary": min_salary,
            "max_salary": max_salary,
            "salary": salary,
            "contract_type": contract_type,
        }
        return out

    @tool("extract-job-description-tool", args_schema=JobDescriptionInput, return_direct=True)
    def extract_job_description_tool(
            number_of_experience: int = None,
            job_type: str = None,
            skills: List[str] = None,
            is_working_right: bool = True
    ):
        """extract job description"""
        return {
            "number_of_experience": number_of_experience,
            "job_type": job_type,
            "skills": skills,
            "is_working_right": is_working_right,
        }

    def create_job_info_agent(llm):
        template = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate(prompt=PromptTemplate(input_variables=[],
                                                              template='You are AI assistant who help me extract useful data '
                                                                       'from job info such as: location, role, company,'
                                                                       'listed_date, salary, minimum salary, maximum salary, contract type')),
            HumanMessagePromptTemplate(prompt=PromptTemplate(input_variables=['input'], template='{input}')),
            MessagesPlaceholder(variable_name='agent_scratchpad')
        ])

        agent = create_openai_functions_agent(llm,
                                              [extract_job_info_tool],
                                              template)
        agent_executor = AgentExecutor(agent=agent,
                                       tools=[extract_job_info_tool],
                                       verbose=True)
        return agent_executor


    def create_job_description_agent(llm):
        template = ChatPromptTemplate.from_messages([
            SystemMessagePromptTemplate(prompt=PromptTemplate(input_variables=[],
                                                              template='You are AI assistant who help me extract useful data '
                                                                       'from job description such as: number of experience, '
                                                                       'job type, skills, is working rights')),
            HumanMessagePromptTemplate(prompt=PromptTemplate(input_variables=['input'], template='{input}')),
            MessagesPlaceholder(variable_name='agent_scratchpad')
        ])

        agent = create_openai_functions_agent(llm,
                                              [extract_job_description_tool],
                                              template)
        agent_executor = AgentExecutor(agent=agent,
                                       tools=[extract_job_description_tool],
                                       verbose=True)
        return agent_executor

    openai_api_key = get_openai_api_key_from_sm()
    website_id_dict = get_crawled_website_id(pg_hook)
    llm = ChatOpenAI(
        openai_api_key=openai_api_key,
        model_name="gpt-3.5-turbo-0125",
    )

    job_info_agent = create_job_info_agent(llm)
    job_description_agent = create_job_description_agent(llm)

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
        if data.get("job_info"):
            print(f"job_info_input: {json.dumps(data.get('job_info'))}")
            job_info = job_info_agent.invoke({"input": json.dumps(data.get('job_info'))})["output"]
        else:
            job_info = dict()
        job_des_input = data["job_description"]
        job_des = dict()
        for e in [job_des_input[i : i + chunk_size] for i in range(0, len(job_des_input), chunk_size)]:
            try:
                job_des_temp = job_description_agent.invoke({"input": e})["output"]
                job_des_temp = {k: v for k, v in job_des_temp.items() if v}
                merge_2_dicts(job_des, job_des_temp)
            except Exception as e:
                print(f"call openai with error: {e}")
                continue
        job_info["role"] = data.get("job_info", {}).get("role", "") if data.get("job_info", {}).get("role", "") != "" else job_info["role"]
        job_info["company"] = data.get("job_info", {}).get("company", "") if data.get("job_info", {}).get("company", "") != "" else job_info["company"]
        job_info["number_of_experience"] = job_des.get("number_of_experience", -1)
        job_info["job_type"] = job_des.get("job_type", "")
        job_info["skills"] = job_des.get("skills", [])
        job_info["is_working_right"] = job_des.get("is_working_right", "")
        job_info_db = JobInfoForDB(**job_info)
        job_info_db.post_salary_validator()
        job_info_db_json = job_info_db.dict()
        job_info_db_json["crawled_website_id"] = website_id_dict.get(data["crawled_website"], -1)
        job_info_db_json["raw_content_file"] = file_path
        job_info_db_json["url"] = data.get("crawled_url", "")
        job_info_db_json["searched_location"] = data.get("searched_location", "")
        job_info_db_json["searched_role"] = data.get("searched_role", "")
        print(f"job_info: {job_info_db_json}")
        out.append(job_info_db_json)

    return out