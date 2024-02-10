from datetime import timedelta
from airflow import DAG as AirflowDag
from airflow.utils.email import send_email
from constant import (
    email
)


def fail_email_function(context):
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    msg = f"task {ti.task_id } failed in dag { ti.dag_id }"
    print(msg)
    subject = f"AIRFLOW: DAG {dag_run} has fail"
    send_email(to=email(), subject=subject, html_content=msg)


def DAG(*args, **kwargs) -> AirflowDag:
    owner = kwargs.get("owner", "longlh")
    retries = kwargs.get("retries", 3)
    retry_delay = kwargs.get("retry_delay", timedelta(minutes=1))
    return AirflowDag(
        *args,
        **kwargs,
        default_args={
            'owner': owner,
            'retries': retries,
            'retry_delay': retry_delay,
            'on_failure_callback': fail_email_function,
        }
    )