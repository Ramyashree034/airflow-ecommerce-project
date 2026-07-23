from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from datetime import datetime


def create_table():
    hook = SqliteHook(sqlite_conn_id="sqlite_default")

    hook.run("""
        CREATE TABLE IF NOT EXISTS employees(
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )
    """)

    print("Table created successfully")


def insert_data():
    hook = SqliteHook(sqlite_conn_id="sqlite_default")

    hook.run("""
        INSERT INTO employees(name, age)
        VALUES
        ('Ramya',22),
        ('Rahul',25),
        ('Anu',24)
    """)

    print("Records inserted")


def read_data():
    hook = SqliteHook(sqlite_conn_id="sqlite_default")

    records = hook.get_records("SELECT * FROM employees")

    print("Employees")

    for row in records:
        print(row)


with DAG(
    dag_id="sqlite_hook_demo",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    create = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    insert = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    read = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    create >> insert >> read


if __name__ == "__main__":
    dag.test()