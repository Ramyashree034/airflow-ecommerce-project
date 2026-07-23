from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime


def read_file():
    with open("input/input.txt", "r") as file:
        lines = file.readlines()

    print("Reading File...")

    for item in lines:
        print(f"Processing Item: {item.strip()}")


with DAG(
    dag_id="file_sensor_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="input/input.txt",
        poke_interval=5,
        timeout=30,
    )

    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_file,
    )

    wait_for_file >> read_data


if __name__ == "__main__":
    dag.test()