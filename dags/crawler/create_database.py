from datetime import datetime
from base_dag import (
    DAG
)
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from constant import (
    job_crawler_postgres_conn,
)

# Define the DAG
with DAG(
    'create_tables_dag',
    description='DAG to create tables and database in PostgreSQL',
    schedule_interval=None,  # Set to None for one-time execution
    start_date=datetime(2024, 2, 11),
    catchup=False,
) as dag:

    @task
    def create_tables():
        # Define SQL statements to create tables
        sql_statements = [
            """
            CREATE TABLE IF NOT EXISTS crawled_website (
                id SERIAL PRIMARY KEY,
                website_name VARCHAR(255) NOT NULL
            );
            """,

            """
            INSERT INTO crawled_website (website_name)
            VALUES ('jora'), ('seek'), ('careerone');
            """,


            """
            CREATE TABLE IF NOT EXISTS job_metadata (
                id TEXT,
                crawled_website_id INTEGER,
                url TEXT NOT NULL,
                location TEXT,
                searched_location TEXT,
                role TEXT,
                searched_role TEXT,
                company TEXT,
                listed_date DATE,
                raw_listed_date TEXT,
                crawled_date DATE,
                min_salary INTEGER,
                max_salary INTEGER,
                contract_type TEXT,
                number_of_experience INTEGER,
                job_type TEXT,
                is_working_right BOOLEAN,
                raw_content_file TEXT,
                level TEXT
            );
            """,

            """
            ALTER TABLE job_metadata 
            ADD CONSTRAINT unique_job_metadata_constraint 
            UNIQUE (crawled_website_id, location, role, company, listed_date, contract_type);
            """,

            """
            CREATE TABLE IF NOT EXISTS skills (
                id SERIAL PRIMARY KEY,
                job_id TEXT REFERENCES job_metadata(id),
                skill TEXT
            );
            """
        ]

        # Connect to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn, schema='jobs')

        # Execute each SQL statement
        for sql_statement in sql_statements:
            pg_hook.run(sql_statement)
            print(f"Executed SQL statement:\n{sql_statement}")


    # Set the task execution order
    create_tables()
