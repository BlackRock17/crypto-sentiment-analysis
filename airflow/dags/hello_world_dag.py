from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Дефиниране на аргументи по подразбиране
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Дефиниране на функция, която ще се изпълни
def print_hello():
    print("Здравей от Airflow!")
    return "Hello World успешно изпълнен"


# Създаване на DAG обект
with DAG(
        'hello_world_dag',
        default_args=default_args,
        description='Първи прост Airflow DAG',
        schedule_interval=timedelta(days=1),
        catchup=False
) as dag:
    # Дефиниране на задача, която изпълнява Python функция
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Задачата ще бъде "hello_task"
    hello_task