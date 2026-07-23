from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

def fetch_orders():
    df = pd.read_csv("data/orders.csv")
    logging.info("Orders fetched successfully!")
    logging.info(df.head())

def fetch_payments():
    df = pd.read_csv("data/payments.csv")
    logging.info("Payments fetched successfully!")
    logging.info(df.head())

def validate_payments():
    df = pd.read_csv("data/payments.csv")

    if df.isnull().sum().sum() > 0:
        raise ValueError("Payment data contains missing values!")

    if (df["payment_amount"] <= 0).any():
        raise ValueError("Invalid payment amount found!")

    logging.info("Payment validation successful!")

def reconcile_orders_payments():
    orders = pd.read_csv("data/orders.csv")
    payments = pd.read_csv("data/payments.csv")

    merged = pd.merge(
        orders,
        payments,
        on="order_id",
        how="left"
    )

    merged.to_csv(
        "reports/payment_reconciliation.csv",
        index=False
    )

    logging.info("Orders and payments reconciled successfully.")

def identify_mismatches():
    df = pd.read_csv("reports/payment_reconciliation.csv")

    mismatches = df[
        (df["payment_status"] != "Success") |
        (df["payment_amount"] != df["quantity"] * df["price"])
    ]

    mismatches.to_csv(
        "reports/payment_mismatches.csv",
        index=False
    )

    logging.info(f"{len(mismatches)} mismatches identified.")

def generate_reconciliation_report():
    df = pd.read_csv("reports/payment_reconciliation.csv")

    total_orders = len(df)
    successful = len(df[df["payment_status"] == "Success"])
    failed = len(df[df["payment_status"] == "Failed"])
    pending = len(df[df["payment_status"] == "Pending"])

    report = f"""
========== PAYMENT RECONCILIATION REPORT ==========

Total Orders          : {total_orders}
Successful Payments   : {successful}
Failed Payments       : {failed}
Pending Payments      : {pending}

===================================================
"""

    with open(
        "reports/reconciliation_report.txt",
        "w",
        encoding="utf-8"
    ) as f:
        f.write(report)

    logging.info(report)

def notify_finance_team():
    logging.info("Finance report generated successfully.")
    logging.info("Finance Team notified.")

default_args = {
    "owner": "Ramya",
    "start_date": datetime(2026, 7, 16),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="payment_reconciliation_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="Payment & Order Reconciliation Pipeline",
    tags=["ecommerce", "payments"],
    doc_md="""
# Payment & Order Reconciliation Pipeline

This DAG compares order data with payment records,
identifies mismatches, generates reconciliation reports,
and notifies the finance team.
""",
) as dag:
    
    task1 = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders,
    )

    task2 = PythonOperator(
        task_id="fetch_payments",
        python_callable=fetch_payments,
    )

    task3 = PythonOperator(
        task_id="validate_payments",
        python_callable=validate_payments,
    )

    task4 = PythonOperator(
        task_id="reconcile_orders_payments",
        python_callable=reconcile_orders_payments,
        execution_timeout=timedelta(minutes=5),
    )

    task5 = PythonOperator(
        task_id="identify_mismatches",
        python_callable=identify_mismatches,
    )

    task6 = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
    )

    task7 = PythonOperator(
        task_id="notify_finance_team",
        python_callable=notify_finance_team,
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7