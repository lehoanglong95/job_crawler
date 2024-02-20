from base_dag import DAG
from pendulum import datetime

with DAG(
    'level_extraction',
    description='DAG to extract level from role in database',
    schedule_interval="0 13 * * *",  # Set to None for one-time execution
    start_date=datetime(2024, 2, 19),
    catchup=False,
) as dag:
    from typing import List
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from constant import job_crawler_postgres_conn
    import pandas as pd
    from utils import get_level_from_role

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn(), schema='jobs')

    @task
    def update_level():
        sql_query = """
        SELECT id, role 
        FROM job_metadata 
        WHERE level IS NULL
        """

        # Execute the SQL query and fetch the results
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        # Convert the results to a Pandas DataFrame
        df = pd.DataFrame(rows, columns=['id', 'role'])
        df["level"] = df["role"].apply(get_level_from_role)

        try:
            # Batch update level in database
            for _, row in df.iterrows():
                cursor.execute("""
                UPDATE job_metadata
                SET level = %s
                WHERE id = %s
                """, (row['level'], row['id']))

            # Commit the transaction
            connection.commit()
            print("Batch update successful.")
        except Exception as e:
            # Rollback the transaction in case of error
            connection.rollback()
        print("Batch update failed:", e)

        cursor.close()
        connection.close()

    update_level()
