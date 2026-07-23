from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Task 1
def generate_number():
    number = 100
    print(f"Generated Number: {number}")
    return number


# Task 2
def double_number(ti):
    number = ti.xcom_pull(task_ids="generate_number")

    doubled = number * 2

    print(f"Doubled Number: {doubled}")

    return doubled


# Task 3
def print_result(ti):
    result = ti.xcom_pull(task_ids="double_number")

    print(f"Final Result: {result}")


with DAG(
    dag_id="xcom_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="generate_number",
        python_callable=generate_number,
    )

    task2 = PythonOperator(
        task_id="double_number",
        python_callable=double_number,
    )

    task3 = PythonOperator(
        task_id="print_result",
        python_callable=print_result,
    )

    task1 >> task2 >> task3


if __name__ == "__main__":
    dag.test()