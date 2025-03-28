"""
Основни помощни функции за Airflow DAG-ове.
Включва основни функции за проверка на свързаността и други общи функционалности.
"""
import logging
import os
import socket
from typing import Dict, Any, Optional, List

# Конфигуриране на логер
logger = logging.getLogger(__name__)


def check_db_connection() -> bool:
    """
    Проверява връзката с базата данни.

    Returns:
        bool: True ако има успешна връзка, False в противен случай
    """
    try:
        import psycopg2
        from db_utils import get_connection_params

        conn_params = get_connection_params()

        # Показване на параметрите за дебъг (без паролата)
        debug_params = conn_params.copy()
        debug_params['password'] = '********' if 'password' in debug_params else None
        logger.info(f"Опит за свързване с база данни със следните параметри: {debug_params}")

        # Свързване с базата данни
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

    except ImportError:
        logger.error("Не може да се импортира psycopg2 или db_utils. Инсталирайте с 'pip install psycopg2-binary'")
        return False
    except Exception as e:
        logger.error(f"Грешка при свързване с базата данни: {e}")
        return False


def check_kafka_connection(bootstrap_servers: str = None) -> bool:
    """
    Проверява връзката с Kafka клъстера.

    Args:
        bootstrap_servers: Опционален адрес на Kafka клъстера

    Returns:
        bool: True ако има успешна връзка, False в противен случай
    """
    # Ако не е подаден адрес, опитваме с адресите от docker-compose
    if not bootstrap_servers:
        # Опитваме различни възможни адреси
        servers_to_try = ["kafka:9092", "localhost:29092", "host.docker.internal:29092"]

        for server in servers_to_try:
            if _check_kafka_server(server):
                return True

        return False
    else:
        return _check_kafka_server(bootstrap_servers)


def _check_kafka_server(bootstrap_servers: str) -> bool:
    """
    Проверява връзката към конкретен Kafka сървър.

    Args:
        bootstrap_servers: Адрес на Kafka сървъра

    Returns:
        bool: True ако има успешна връзка, False в противен случай
    """
    try:
        # Първо проверяваме TCP връзката
        host, port = bootstrap_servers.split(':')
        port = int(port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex((host, port))
        s.close()

        if result != 0:
            logger.warning(f"Неуспешна TCP връзка към {bootstrap_servers}, код: {result}")
            return False

        # Ако имаме TCP връзка, опитваме с AdminClient
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient({'bootstrap.servers': bootstrap_servers})
            topics = admin.list_topics(timeout=10)

            logger.info(f"Успешна връзка с Kafka на {bootstrap_servers}. Намерени {len(topics.topics)} топика.")
            return True

        except ImportError:
            logger.warning(
                "Не може да се импортира confluent_kafka.admin.AdminClient. Инсталирайте с 'pip install confluent-kafka'")
            # Връщаме True щом TCP връзката е успешна, въпреки че не можем да проверим с AdminClient
            return True

        except Exception as e:
            logger.error(f"Грешка при свързване с Kafka AdminClient на {bootstrap_servers}: {e}")
            return False

    except Exception as e:
        logger.error(f"Грешка при проверка на Kafka връзка към {bootstrap_servers}: {e}")
        return False


def format_duration(seconds: float) -> str:
    """
    Форматира продължителност в секунди към четим формат.

    Args:
        seconds: Продължителност в секунди

    Returns:
        str: Форматирана продължителност
    """
    if seconds < 60:
        return f"{seconds:.2f} секунди"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{int(minutes)} минути и {remaining_seconds:.2f} секунди"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        remaining_seconds = seconds % 60
        return f"{int(hours)} часа, {int(minutes)} минути и {remaining_seconds:.2f} секунди"


def generate_notification(title: str, message: str, level: str = "info") -> None:
    """
    Генерира известие чрез Airflow.

    Args:
        title: Заглавие на известието
        message: Съобщение на известието
        level: Ниво на известието (info, warning, error)
    """
    try:
        from airflow.utils.email import send_email

        admin_emails = os.getenv('AIRFLOW_ADMIN_EMAILS', '').split(',')
        if admin_emails and admin_emails[0]:
            send_email(
                to=admin_emails,
                subject=f"[Airflow] {title}",
                html_content=f"<h3>{title}</h3><p>{message}</p>"
            )
            logger.info(f"Изпратено известие до {admin_emails}: {title}")
    except Exception as e:
        logger.error(f"Грешка при генериране на известие: {e}")
