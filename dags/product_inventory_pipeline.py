from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging


# Task 1: Fetch Product Data
def fetch_products():
    df = pd.read_csv("data/products.csv")
    logging.info("Products fetched successfully!")
    print(df.head())

# Task 2: Validate Product Data
def validate_products():
    df = pd.read_csv("data/products.csv")

    if df.isnull().sum().sum() > 0:
        raise ValueError("Product data contains missing values!")

    if (df["stock"] < 0).any():
        raise ValueError("Negative stock found!")

    print("Product data validation successful!")


# Task 2: Update Inventory
def update_inventory():
    df = pd.read_csv("data/products.csv")
    df["stock"] = df["stock"] + 10   # Simulate receiving new stock
    df.to_csv("data/products_updated.csv", index=False)
    print("Inventory updated successfully!")


# Task 3: Check Low Stock
def check_low_stock():
    df = pd.read_csv("data/products_updated.csv")
    low_stock = df[df["stock"] < 20]

    print("Low Stock Products:")
    print(low_stock)

    low_stock.to_csv("reports/low_stock_report.csv", index=False)


# Task 4: Generate Inventory Report
def generate_report():
    df = pd.read_csv("data/products_updated.csv")

    report = f"""
========== INVENTORY REPORT ==========

Total Products : {len(df)}
Total Stock    : {df['stock'].sum()}
Average Price  : Rs.{round(df['price'].mean(),2)}

Low Stock Products : {len(df[df['stock'] < 20])}

Highest Stock Product :
{df.loc[df['stock'].idxmax(),'product_name']}

Lowest Stock Product :
{df.loc[df['stock'].idxmin(),'product_name']}

======================================
"""

    with open("reports/inventory_report.txt", "w", encoding="utf-8") as f:
        f.write(report)

    logging.info(report)

def generate_restock_list():
    df = pd.read_csv("data/products_updated.csv")

    restock = df[df["stock"] < 20]

    restock.to_csv(
        "reports/restock_products.csv",
        index=False
    )

    logging.info("Restock list generated.")


# Task 5: Send Alert
def send_alert():
    logging.info("Inventory report generated successfully.")
    logging.info("Alert sent to Inventory Team.")

default_args = {
    "owner": "Ramya",
    "start_date": datetime(2026, 7, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="product_inventory_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="Product Inventory Pipeline for E-commerce",
    tags=["ecommerce", "inventory"],
    doc_md="""
# Product & Inventory Pipeline

This Airflow DAG simulates an e-commerce inventory management workflow.

### Workflow
1. Fetch product data
2. Validate product records
3. Update inventory
4. Identify low-stock products
5. Generate inventory report
6. Generate restock list
7. Notify inventory team
""",
) as dag:

    task1 = PythonOperator(
        task_id="fetch_products",
        python_callable=fetch_products,
    )

    validate_task = PythonOperator(
    task_id="validate_product_data",
    python_callable=validate_products,
)

    task2 = PythonOperator(
        task_id="update_inventory",
        python_callable=update_inventory,
        execution_timeout=timedelta(minutes=5),
    )

    task3 = PythonOperator(
        task_id="check_low_stock",
        python_callable=check_low_stock,
    )

    task4 = PythonOperator(
        task_id="generate_inventory_report",
        python_callable=generate_report,
    )

    task6 = PythonOperator(
    task_id="generate_restock_list",
    python_callable=generate_restock_list,
)

    task5 = PythonOperator(
        task_id="send_inventory_alert",
        python_callable=send_alert,
    )

    task1 >> validate_task >> task2 >> task3 >> task4 >> task6 >> task5

    if __name__ == "__main__":
        dag.test()