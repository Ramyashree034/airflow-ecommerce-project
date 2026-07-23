from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def fetch_orders():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM orders;")

    rows = cursor.fetchall()

    print("Orders Table:")
    for row in rows:
        print(row)

    cursor.close()
    conn.close()


with DAG(
    dag_id="postgres_hook_demo",
    start_date=datetime(2026, 7, 22),
    schedule=None,
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders,
    )