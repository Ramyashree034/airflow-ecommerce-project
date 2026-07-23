from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="taskflow_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def taskflow_demo():

    @task
    def generate_number():
        print("Generated Number: 100")
        return 100

    @task
    def double_number(number):
        result = number * 2
        print(f"Doubled Number: {result}")
        return result

    @task
    def print_result(result):
        print(f"Final Result: {result}")

    number = generate_number()
    doubled = double_number(number)
    print_result(doubled)


dag = taskflow_demo()

if __name__ == "__main__":
    dag.test()