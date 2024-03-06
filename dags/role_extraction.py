from base_dag import DAG
from pendulum import datetime

with DAG(
        'role_extraction',
        description='DAG to extract role from searched role in database',
        schedule_interval="*/30 * * * *",  # Set to None for one-time execution
        start_date=datetime(2024, 3, 6),
        catchup=False,
) as dag:
    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from constant import job_crawler_postgres_conn, Role
    import pandas as pd
    from utils import categorize_it_role

    pg_hook = PostgresHook(postgres_conn_id=job_crawler_postgres_conn, schema='jobs')

    @task
    def update_extraction_role():
        sql_query = f"""
        SELECT id, role 
        FROM job_metadata 
        WHERE extraction_role IS NULL OR extraction_role = '{Role.uncategorized}'
        """

        # Execute the SQL query and fetch the results
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        # Convert the results to a Pandas DataFrame
        df = pd.DataFrame(rows, columns=['id', 'role'])
        df["extraction_role"] = df["role"].apply(categorize_it_role)

        try:
            # Batch update level in database
            for _, row in df.iterrows():
                cursor.execute("""
                UPDATE job_metadata
                SET extraction_role = %s
                WHERE id = %s
                """, (row['extraction_role'], row['id']))

            # Commit the transaction
            connection.commit()
            print("Batch update successful.")
        except Exception as e:
            # Rollback the transaction in case of error
            connection.rollback()
            print("Batch update failed:", e)

        cursor.close()
        connection.close()

    update_extraction_role()
