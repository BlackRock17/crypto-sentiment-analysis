from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import sys

# Достъпваме src директорията
sys.path.append('/opt/airflow/src')

# Добавяме зависимост към Kafka клиента
try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    print("Kafka библиотеката не е инсталирана")

# Аргументи по подразбиране
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Функция за мониторинг на Kafka топик
def monitor_kafka_topic(topic_name, **kwargs):
    print(f"Стартиране на мониторинг на Kafka топик: {topic_name}")

    # Проверка дали Kafka библиотеката е инсталирана
    if 'confluent_kafka' not in sys.modules:
        print("Kafka библиотеката не е инсталирана. Инсталирайте с 'pip install confluent-kafka'")
        return

    # Конфигурация на консуматора
    config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': f'airflow-monitor-{topic_name}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }

    try:
        # Създаване на консуматор
        consumer = Consumer(config)

        # Абониране за топика
        consumer.subscribe([topic_name])

        # Списък за съхранение на съобщения
        messages = []

        # Опит за получаване на съобщения (с таймаут)
        for _ in range(5):  # Опитваме 5 пъти да получим съобщения
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Достигнат е краят на партиция {msg.topic()}")
                else:
                    print(f"Грешка: {msg.error()}")
            else:
                # Декодиране на съобщението
                try:
                    value = msg.value().decode('utf-8')
                    messages.append(value)
                    print(f"Получено съобщение: {value[:100]}...")
                except Exception as e:
                    print(f"Грешка при декодиране: {e}")

        # Затваряне на консуматора
        consumer.close()

        # Запазване на резултатите
        print(f"Получени {len(messages)} съобщения от топик {topic_name}")

        # Предаване на резултатите на следващата задача
        kwargs['ti'].xcom_push(key=f'messages_{topic_name}', value=len(messages))

        return len(messages)

    except Exception as e:
        print(f"Грешка при мониторинг на Kafka: {e}")
        return 0


# Функция за принтиране на резултатите
def print_results(**kwargs):
    ti = kwargs['ti']

    # Вземане на резултатите от предишната задача
    messages_count = ti.xcom_pull(task_ids='monitor_raw_tweets', key='messages_twitter-raw-tweets')

    print(f"Отчет за Kafka мониторинг:")
    print(f"Брой съобщения в топик 'twitter-raw-tweets': {messages_count}")

    return "Мониторингът приключи успешно"


def test_kafka_connection(**kwargs):
    import socket

    try:
        # Опитай да свържеш сокет към Kafka
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5 секунди таймаут
        result = sock.connect_ex(('kafka', 9092))
        sock.close()

        if result == 0:
            print("Успешна връзка до Kafka на kafka:9092")
            return True
        else:
            print(f"Неуспешна връзка до Kafka на kafka:9092, код: {result}")
            return False
    except Exception as e:
        print(f"Грешка при свързване с Kafka: {e}")
        return False


# Създаване на DAG
with DAG(
        'kafka_monitor_dag',
        default_args=default_args,
        description='Мониторинг на Kafka топици',
        schedule_interval=timedelta(minutes=15),
        catchup=False
) as dag:
    # Задача за мониторинг на twitter-raw-tweets топик
    monitor_raw_tweets = PythonOperator(
        task_id='monitor_raw_tweets',
        python_callable=monitor_kafka_topic,
        op_kwargs={'topic_name': 'twitter-raw-tweets'},
    )

    # Задача за принтиране на резултатите
    print_results_task = PythonOperator(
        task_id='print_results',
        python_callable=print_results,
    )

    print_results_task_task = PythonOperator(
        task_id='test_kafka_connection',
        python_callable=test_kafka_connection,
    )

    # Дефиниране на последователност
    monitor_raw_tweets >> print_results_task >> print_results_task_task
