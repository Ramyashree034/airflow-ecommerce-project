from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

def fetch_orders():
    df = pd.read_csv("data/orders.csv")
    logging.info("Orders fetched successfully!")
    logging.info(df.head())

def validate_orders():
    df = pd.read_csv("data/orders.csv")

    if df.isnull().sum().sum() > 0:
        raise ValueError("Orders contain missing values!")

    if (df["quantity"] <= 0).any():
        raise ValueError("Invalid quantity found!")

    if (df["price"] <= 0).any():
        raise ValueError("Invalid price found!")

    logging.info("Order validation successful!")

def calculate_revenue():
    df = pd.read_csv("data/orders.csv")

    delivered = df[df["status"] == "Delivered"]

    delivered["revenue"] = delivered["quantity"] * delivered["price"]

    total_revenue = delivered["revenue"].sum()

    delivered.to_csv("reports/revenue_summary.csv", index=False)

    logging.info(f"Total Revenue = Rs.{total_revenue}")

def identify_top_products():
    df = pd.read_csv("reports/revenue_summary.csv")

    top_products = (
        df.groupby("product_name")["quantity"]
        .sum()
        .sort_values(ascending=False)
    )

    top_products.to_csv("reports/top_products.csv")

    logging.info("Top selling products generated.")

def generate_sales_report():
    df = pd.read_csv("reports/revenue_summary.csv")

    report = f"""
========== SALES REPORT ==========

Total Orders : {len(df)}

Total Revenue : Rs.{df['revenue'].sum()}

Average Order Value : Rs.{round(df['revenue'].mean(),2)}

Delivered Orders : {len(df)}

=================================
"""

    with open(
        "reports/daily_sales_report.txt",
        "w",
        encoding="utf-8"
    ) as f:
        f.write(report)

    logging.info(report)

def notify_sales_team():
    logging.info("Sales report generated successfully.")
    logging.info("Sales Team notified.")

default_args = {
    "owner": "Ramya",
    "start_date": datetime(2026, 7, 16),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="order_processing_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="Order Processing & Sales Analytics Pipeline",
    tags=["ecommerce", "orders"],
    doc_md="""
# Order Processing & Sales Analytics

This DAG processes customer orders, validates them,
calculates revenue, identifies top-selling products,
generates sales reports and notifies the sales team.
""",
) as dag:
    
    task1 = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders,
    )

    task2 = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders,
    )

    task3 = PythonOperator(
        task_id="calculate_revenue",
        python_callable=calculate_revenue,
        execution_timeout=timedelta(minutes=5),
    )

    task4 = PythonOperator(
        task_id="identify_top_products",
        python_callable=identify_top_products,
    )

    task5 = PythonOperator(
        task_id="generate_sales_report",
        python_callable=generate_sales_report,
    )

    task6 = PythonOperator(
        task_id="notify_sales_team",
        python_callable=notify_sales_team,
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6