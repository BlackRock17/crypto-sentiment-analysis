from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Добавяме пътя към директорията с помощни функции
dags_folder = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(dags_folder, 'utils')
sys.path.append(utils_path)

# Импортираме помощните функции
from helpers import check_db_connection, check_kafka_connection

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'connection_test_dag',
        default_args=default_args,
        description='Проверка на връзките с Kafka и базата данни',
        schedule_interval=timedelta(hours=1),
        catchup=False
) as dag:
    def test_connections():
        """Тестване на връзките с Kafka и базата данни"""
        results = {
            'database': check_db_connection(),
            'kafka': check_kafka_connection()
        }

        for service, connected in results.items():
            status = "УСПЕШНА" if connected else "НЕУСПЕШНА"
            print(f"Връзка с {service}: {status}")

        if all(results.values()):
            return "Всички връзки са успешни"
        else:
            failed = [s for s, v in results.items() if not v]
            return f"Неуспешни връзки: {', '.join(failed)}"


    check_connections_task = PythonOperator(
        task_id='check_connections',
        python_callable=test_connections,
    )
