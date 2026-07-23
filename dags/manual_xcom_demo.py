from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def calculate_inventory(ti):

    total_products = 20
    low_stock = 4
    average_price = 2821.5

    ti.xcom_push(key="total_products", value=total_products)
    ti.xcom_push(key="low_stock", value=low_stock)
    ti.xcom_push(key="average_price", value=average_price)

    print("Inventory details stored in XCom.")


def display_inventory(ti):

    total = ti.xcom_pull(
        task_ids="calculate_inventory",
        key="total_products"
    )

    low = ti.xcom_pull(
        task_ids="calculate_inventory",
        key="low_stock"
    )

    avg = ti.xcom_pull(
        task_ids="calculate_inventory",
        key="average_price"
    )

    print(f"Total Products : {total}")
    print(f"Low Stock      : {low}")
    print(f"Average Price  : ₹{avg}")


with DAG(
    dag_id="manual_xcom_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="calculate_inventory",
        python_callable=calculate_inventory,
    )

    task2 = PythonOperator(
        task_id="display_inventory",
        python_callable=display_inventory,
    )

    task1 >> task2


if __name__ == "__main__":
    dag.test()