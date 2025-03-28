"""
DAG за мониторинг на основните инфраструктурни компоненти (Kafka и база данни).
Извършва регулярни проверки на свързаността и състоянието на компонентите.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import sys
import logging

# Конфигуриране на логер
logger = logging.getLogger(__name__)

# Добавяне на utils директорията към Python path
dags_folder = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(dags_folder, 'utils')
sys.path.append(utils_path)

# Импортиране на помощните функции
from helpers import check_db_connection, check_kafka_connection
from kafka_utils import test_kafka_detailed, check_kafka_topics_health

# Аргументи по подразбиране за DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

# Създаване на DAG
with DAG(
        'infrastructure_monitor',
        default_args=default_args,
        description='Мониторинг на инфраструктурните компоненти (Kafka и база данни)',
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=['monitoring', 'infrastructure']
) as dag:
    # Задача 1: Проверка на базовите връзки
    def test_base_connections():
        """
        Проверява основните връзки с Kafka и базата данни.
        Това е базова проверка, която само установява свързаност.
        """
        results = {
            'database': check_db_connection(),
            'kafka': check_kafka_connection()
        }

        # Форматиране на резултатите за по-добра четимост
        report = "=== ОТЧЕТ ЗА ПРОВЕРКА НА СВЪРЗАНОСТТА ===\n"
        report += f"Време: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

        for service, connected in results.items():
            status = "✅ УСПЕШНА" if connected else "❌ НЕУСПЕШНА"
            report += f"{service.upper()}: {status}\n"

        logger.info(report)

        if all(results.values()):
            return "Всички основни връзки са успешни"
        else:
            failed = [s for s, v in results.items() if not v]
            return f"Неуспешни връзки: {', '.join(failed)}"


    check_connections_task = PythonOperator(
        task_id='check_base_connections',
        python_callable=test_base_connections,
    )


    # Задача 2: Подробен тест на Kafka
    def run_detailed_kafka_test():
        """
        Изпълнява по-детайлен тест на Kafka връзката,
        включително проверка на достъпа до различни хостове и конфигурации.
        """
        results = test_kafka_detailed()

        # Форматиране на резултатите
        successful_tests = [test for test, result in results.items() if result]
        failed_tests = [test for test, result in results.items() if not result]

        report = "=== ПОДРОБЕН ОТЧЕТ ЗА KAFKA ===\n"
        report += f"Време: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += f"Успешни тестове: {len(successful_tests)}/{len(results)}\n"

        if failed_tests:
            report += "Неуспешни тестове:\n"
            for test in failed_tests:
                report += f"  - {test}\n"

        logger.info(report)

        if failed_tests:
            return f"Неуспешни Kafka тестове: {', '.join(failed_tests)}"
        else:
            return "Всички Kafka тестове са успешни"


    detailed_kafka_test_task = PythonOperator(
        task_id='run_detailed_kafka_test',
        python_callable=run_detailed_kafka_test,
    )


    # Задача 3: Проверка на Kafka топиците
    def check_kafka_topics():
        """
        Проверява състоянието на Kafka топиците, включително:
        - Наличие на необходимите топици
        - Брой партиции
        - Брой съобщения в топиците
        """
        results = check_kafka_topics_health()

        # Форматиране на резултатите
        report = "=== ОТЧЕТ ЗА KAFKA ТОПИЦИТЕ ===\n"
        report += f"Време: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += f"Общ брой топици: {len(results['topics'])}\n"

        # Показване на информация за всеки топик
        for topic, data in results['topics'].items():
            report += f"\nТопик: {topic}\n"
            report += f"  Брой партиции: {data.get('partition_count', 'Неизвестно')}\n"
            report += f"  Брой съобщения: {data.get('message_count', 'Неизвестно')}\n"
            health = "✅ Здрав" if data.get('is_healthy', False) else "⚠️ Има проблеми"
            report += f"  Статус: {health}\n"

        logger.info(report)

        if results['all_topics_healthy']:
            return "Всички Kafka топици са в добро състояние"
        else:
            unhealthy_topics = [t for t, d in results['topics'].items() if not d.get('is_healthy', False)]
            return f"Проблемни топици: {', '.join(unhealthy_topics)}"


    kafka_topics_task = PythonOperator(
        task_id='check_kafka_topics',
        python_callable=check_kafka_topics,
    )


    # Задача 4: Подробна проверка на базата данни
    def check_database_health():
        """
        Проверява състоянието на базата данни, включително:
        - Наличност на таблиците
        - Основни статистики
        """
        try:
            from db_utils import check_database_health
            results = check_database_health()

            # Форматиране на резултатите
            report = "=== ОТЧЕТ ЗА БАЗАТА ДАННИ ===\n"
            report += f"Време: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            report += f"Общ брой таблици: {len(results.get('tables', []))}\n"

            if 'row_counts' in results:
                report += "\nБрой редове в основните таблици:\n"
                for table, count in results['row_counts'].items():
                    report += f"  {table}: {count}\n"

            logger.info(report)

            if results.get('is_healthy', False):
                return "Базата данни е в добро състояние"
            else:
                return "Има проблеми с базата данни: " + results.get('message', 'Неизвестна грешка')
        except ImportError:
            logger.warning("db_utils модулът не е намерен. Пропускане на подробната проверка на базата данни.")
            return "Подробната проверка на базата данни е пропусната"
        except Exception as e:
            logger.error(f"Грешка при проверка на базата данни: {e}")
            return f"Грешка при проверка на базата данни: {str(e)}"


    db_health_task = PythonOperator(
        task_id='check_database_health',
        python_callable=check_database_health,
    )

    # Задаване на зависимостите между задачите
    check_connections_task >> [detailed_kafka_test_task, kafka_topics_task, db_health_task]
