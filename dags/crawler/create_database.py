from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the DAG
with DAG(
    'create_tables_dag',
    description='DAG to create tables and database in PostgreSQL',
    schedule_interval=None,  # Set to None for one-time execution
    start_date=datetime(2024, 1, 30),
    catchup=False,
) as dag:

    @task
    def create_database():
        # Define SQL statement to create the database
        sql_statement = "CREATE DATABASE IF NOT EXISTS Jobs"

        # Connect to the default PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')

        # Execute the SQL statement to create the database
        pg_hook.run(sql_statement)
        print(f"Executed SQL statement:\n{sql_statement}")

    @task
    def create_tables():
        # Define SQL statements to create tables
        sql_statements = [
            """
            CREATE TABLE IF NOT EXISTS crawled_website (
                id SERIAL PRIMARY KEY,
                website_name VARCHAR(255) NOT NULL
            )
            """,

            """
            INSERT INTO crawled_website (website_name)
            VALUES ('jora'), ('seek'), ('careerone');
            """,


            """
            CREATE TABLE IF NOT EXISTS job_metadata (
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL,
                city VARCHAR(255),
                role VARCHAR(255),
                company VARCHAR(255),
                listed_date DATE,
                min_salary INTEGER,
                max_salary INTEGER,
                state VARCHAR(255),
                contract_type_id INTEGER,
                raw_content_file TEXT,
                career_level_id INTEGER,
                job_type INTEGER,
            )
            """,

            """
            CREATE TABLE IF NOT EXISTS career_level (
                id SERIAL PRIMARY KEY,
                job_id INTEGER REFERENCES job_metadata(id),
                career_level TEXT
            );
            """,

            """
            CREATE TABLE IF NOT EXISTS skills (
                id SERIAL PRIMARY KEY,
                job_id INTEGER REFERENCES job_metadata(id),
                skill TEXT
            );
            """
        ]

        # Connect to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id='redshift_conn_id', schema='Jobs')

        # Execute each SQL statement
        for sql_statement in sql_statements:
            pg_hook.run(sql_statement)
            print(f"Executed SQL statement:\n{sql_statement}")


    # Set the task execution order
    create_database >> create_tables
