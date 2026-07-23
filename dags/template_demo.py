from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="template_variables_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    show_templates = BashOperator(
        task_id="show_templates",
        bash_command="""
        echo "Execution Date : {{ ds }}"
        echo "Timestamp      : {{ ts }}"
        echo "Run ID         : {{ run_id }}"
        echo "DAG ID         : {{ dag.dag_id }}"
        echo "Task ID        : {{ task.task_id }}"
        """
    )

    show_templates

if __name__ == "__main__":
    dag.test()