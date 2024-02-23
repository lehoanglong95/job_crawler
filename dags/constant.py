from airflow.models import Variable


class CustomVariable:

    def __init__(self,
                 key,
                 val,
                 normalize_text=None,
                 default_value=None):
        self.key = key
        self.val = val
        self.normalize_text = normalize_text
        self.default_value = default_value
        Variable.set(key, val)

    def __call__(self):
        return Variable.get(self.key, default_var=self.default_value)

    def __repr__(self):
        return self.normalize_text if self.normalize_text else self.val

# job_crawler_postgres_conn = CustomVariable("postgres_job_crawler_conn_id",
#                                           "postgres_job_crawler_conn_id")
job_crawler_postgres_conn = "postgres_job_crawler_conn_id"
email = CustomVariable("my_email", "lehoanglong95@gmail.com")
jora_searched_data_engineer = CustomVariable("jora_data_engineer",
                                             "Data+Engineer",
                                             "data engineer")
jora_searched_ai_engineer = CustomVariable("jora_ai_engineer",
                                           "AI+Engineer",
                                           "ai engineer")
jora_searched_full_stack_developer = CustomVariable("jora_full_stack",
                                                    "Full+Stack+Developer",
                                                    "full stack engineer")
jora_searched_backend_engineer = CustomVariable("jora_backend_engineer",
                                                 "Backend+Engineer",
                                                 "backend engineer")
jora_searched_frontend_engineer = CustomVariable("jora_frontend_engineer",
                                                 "frontend+developer",
                                                 "frontend engineer")
jora_searched_devops_engineer = CustomVariable("jora_devops_engineer",
                                                 "DevOps+Engineer",
                                                 "devops engineer")
jora_searched_cybersecurity_engineer = CustomVariable("jora_cybersecurity_engineer",
                                                 "cyber+security",
                                                 "cyber security engineer")
jora_searched_sydney = CustomVariable("jora_sydney",
                                      "Sydney+NSW",
                                      "sydney")
jora_searched_melbourne = CustomVariable("jora_melbourne",
                                         "Melbourne+VIC",
                                         "melbourne")
seek_searched_data_engineer = CustomVariable("seek_data_engineer",
                                             "data-engineer-jobs",
                                             "data engineer")
seek_searched_ai_engineer = CustomVariable("seek_ai_engineer",
                                           "AI-Engineer-jobs",
                                           "ai engineer")
seek_searched_full_stack_developer = CustomVariable("seek_full_stack",
                                                    "Full-Stack-Developer-jobs",
                                                    "full stack engineer")
seek_searched_backend_engineer = CustomVariable("seek_backend_engineer",
                                                "Backend-Developer-jobs",
                                                "backend engineer")
seek_searched_frontend_engineer = CustomVariable("seek_frontend_engineer",
                                                 "Front-End-Developer-jobs",
                                                 "frontend engineer")
seek_searched_devops_engineer = CustomVariable("seek_devops_engineer",
                                                 "DevOps-Engineer-jobs",
                                                 "devops engineer")
seek_searched_cybersecurity_engineer = CustomVariable("seek_cybersecurity_engineer",
                                                      "cyber-security-jobs",
                                                      "cyber security engineer")
seek_searched_sydney = CustomVariable("seek_sydney",
                                      "in-All-Sydney-NSW",
                                      "sydney")
seek_searched_melbourne = CustomVariable("seek_melbourne",
                                         "in-Melbourne-VIC-3000",
                                         "melbourne")
