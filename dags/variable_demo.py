from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime


def check_environment():
    env = Variable.get("environment")
    print(f"Running in {env} environment")


with DAG(
    dag_id="variable_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    check_env = PythonOperator(
        task_id="check_environment",
        python_callable=check_environment,
    )

if __name__ == "__main__":
    dag.test()