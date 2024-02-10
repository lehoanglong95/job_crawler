from airflow.decorators import task
from base_dag import DAG
from pendulum import datetime

with DAG(
    dag_id="test",
    start_date=datetime(2024, 1, 30),
    description="test",
    schedule_interval=None,
) as dag:

    @task
    def test_fail():
        return 5 / 0

    test_fail()