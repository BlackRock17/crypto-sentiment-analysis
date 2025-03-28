# airflow/dags/utils/helpers.py
import logging
from typing import Dict, Any, Optional
import os

from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)


def check_kafka_connection(bootstrap_servers: str = "kafka:9092") -> bool:
    """
    Проверява връзката с Kafka клъстера.

    Args:
        bootstrap_servers: Адрес на Kafka клъстера

    Returns:
        bool: True ако има успешна връзка, False в противен случай
    """
    try:
        admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        topics = admin.list_topics(timeout=10)
        logger.info(f"Успешна връзка с Kafka. Намерени {len(topics.topics)} топика.")
        return True
    except Exception as e:
        logger.error(f"Грешка при свързване с Kafka: {e}")
        return False


def get_db_connection_params() -> Dict[str, Any]:
    """
    Взема параметрите за връзка с базата данни от средата.

    Returns:
        Dict: Речник с параметри за връзка
    """
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@db:5432/solana_sentiment')

    # Разбиване на URL на компоненти
    if 'postgresql://' in DATABASE_URL:
        url_parts = DATABASE_URL.replace('postgresql://', '').split('@')
        user_pass = url_parts[0].split(':')
        host_port_db = url_parts[1].split('/')
        host_port = host_port_db[0].split(':')

        return {
            'user': user_pass[0],
            'password': user_pass[1],
            'host': host_port[0],
            'port': host_port[1],
            'database': host_port_db[1]
        }
    else:
        return {
            'user': 'postgres',
            'password': 'password',
            'host': 'db',
            'port': '5432',
            'database': 'solana_sentiment'
        }


def check_db_connection() -> bool:
    """
    Проверява връзката с базата данни.

    Returns:
        bool: True ако има успешна връзка, False в противен случай
    """
    try:
        import psycopg2
        conn_params = get_db_connection_params()

        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()

        cursor.close()
        conn.close()

        logger.info(f"Успешна връзка с базата данни. PostgreSQL версия: {version[0]}")
        return True
    except Exception as e:
        logger.error(f"Грешка при свързване с базата данни: {e}")
        return False
