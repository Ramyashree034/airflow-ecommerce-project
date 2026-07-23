from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule


def check_stock():
    stock = 15
    print(f"Current Stock: {stock}")

    if stock > 10:
        return "process_order"
    else:
        return "send_alert"


def process_order():
    print("Processing customer order...")


def send_alert():
    print("Low stock! Sending alert...")

def end_pipeline():
    print("Pipeline completed successfully!")


with DAG(
    dag_id="branch_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id="check_stock",
        python_callable=check_stock,
    )

    process = PythonOperator(
        task_id="process_order",
        python_callable=process_order,
    )

    alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )

    end_task = PythonOperator(
    task_id="end_pipeline",
    python_callable=end_pipeline,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    branch >> [process, alert]

    process >> end_task
    alert >> end_task


if __name__ == "__main__":
    dag.test()