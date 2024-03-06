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

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn, schema='jobs')

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
                    CREATE VIEW ai_engineer_skills AS
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
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'full_stack_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW full_stack_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'full stack engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,
            
            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'backend_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW backend_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'backend engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'frontend_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW frontend_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'frontend engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'devops_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW devops_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'devops engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'cybersecurity_engineer_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW cybersecurity_engineer_skills AS
                    WITH de_job_metadata AS (
                        SELECT id 
                        FROM job_metadata 
                        WHERE searched_role = 'cyber security engineer'
                    )
                    SELECT A.id, B.skill
                    FROM de_job_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

            """
            DO
            $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'salary_skills') THEN
                    -- Create the view if it doesn't exist
                    CREATE VIEW salary_skills AS
                    WITH salary_skills_metadata AS (
                        SELECT id, (COALESCE(max_salary, 0) + COALESCE(min_salary, 0)) / 2 AS salary
                        FROM job_metadata WHERE min_salary IS NOT NULL AND max_salary IS NOT NULL
                    )
                    SELECT A.id, A.salary, B.skill
                    FROM salary_skills_metadata A
                    JOIN skills B ON A.id = B.job_id;
                END IF;
            END
            $$
            """,

           """
           DO
           $$
           BEGIN
               IF NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'role_and_skills') THEN
                   -- Create the view if it doesn't exist
                   CREATE VIEW data_engineer_skills AS
                   WITH extraction_role_table AS (
                       SELECT id, extraction_role 
                       FROM job_metadata 
                   )
                   SELECT A.id, A.extraction_role, B.skill
                   FROM extraction_role_table A
                   JOIN skills B ON A.id = B.job_id;
               END IF;
           END
           $$
           """,
        ]

        for sql_statement in sql_statements:
            pg_hook.run(sql_statement)
            print(f"Executed SQL statement:\n{sql_statement}")

    create_analytics_view()