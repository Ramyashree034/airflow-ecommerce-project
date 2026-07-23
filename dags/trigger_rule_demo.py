from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

def task_a():
    print("Task A completed.")

def task_b():
    print("Task B completed.")
    raise Exception("Task B Failed!")

def task_c():
    print("Task C completed.")

def final_task():
    print("Final task executed.")

with DAG(
    dag_id="trigger_rule_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="task_a",
        python_callable=task_a,
    )

    task2 = PythonOperator(
        task_id="task_b",
        python_callable=task_b,
    )

    task3 = PythonOperator(
        task_id="task_c",
        python_callable=task_c,
    )

    final = PythonOperator(
    task_id="final_task",
    python_callable=final_task,
     trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    task1 >> [task2, task3]
    [task2, task3] >> final

