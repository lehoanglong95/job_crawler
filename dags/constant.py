from airflow.models import Variable


class CustomVariable:

    def __init__(self, key, val, default_value=None):
        self.key = key
        self.val = val
        self.default_value = default_value
        Variable.set(key, val)

    def __call__(self):
        return Variable.get(self.key, default_var=self.default_value)


job_crawler_postgres_conn = CustomVariable("postgres_job_crawler_conn_id",
                                          "postgres_job_crawler_conn_id")
email = CustomVariable("my_email", "lehoanglong95@gmail.com")