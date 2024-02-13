from base_dag import DAG
from pendulum import datetime

with DAG(
    dag_id="analytic_views",
    start_date=datetime(2024, 2, 11),
    description="create analytic views",
    schedule_interval=None,
    concurrency=8,
    max_active_tasks=3,
    tags=["analytics", "postgres view"],
):
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from constant import job_crawler_postgres_conn

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    @task
    def create_analytics_view():
        sql_statements = [
            # create de skills view
            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'data_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW data_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'data engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            # create aie skills view
            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'ai_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW data_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'ai engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            # create fullstack skills view
            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'ai_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW data_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'full stack developer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """
            
            # create cloud view
            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'ai_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW data_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'full stack developer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """
        ]

        for sql_statement in sql_statements:
            pg_hook.run(sql_statement)
            print(f"Executed SQL statement:\n{sql_statement}")

    create_analytics_view()