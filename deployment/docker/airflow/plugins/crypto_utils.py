"""
Utility functions and helpers for Apache Airflow DAGs in the Crypto Sentiment project.
"""
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable
from src.data_processing.database import get_db
from src.data_processing.kafka.producer import NotificationProducer

# Add the parent directory to the path to allow importing from src
sys.path.append("/opt/airflow")

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for DAGs
DEFAULT_ARGS = {
    'owner': 'crypto_sentiment',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Blockchain networks to monitor
BLOCKCHAIN_NETWORKS = [
    'solana',
    'ethereum',
    'binance',
    'polygon',
    'avalanche'
]

# Catchup setting for DAGs (whether to run for historical dates)
CATCHUP = False


def get_db_session():
    """
    Get a database session for the crypto sentiment database.

    Returns:
        SQLAlchemy Session object
    """
    return next(get_db())


def close_db_session(session):
    """
    Safely close a database session.

    Args:
        session: SQLAlchemy Session object
    """
    if session:
        try:
            session.close()
        except Exception as e:
            logger.error(f"Error closing database session: {e}")


def send_notification(title: str, message: str, priority: str = "medium", **kwargs):
    """
    Send a notification to the system.

    Args:
        title: Notification title
        message: Notification message
        priority: Priority level (low, medium, high)
        **kwargs: Additional metadata
    """
    try:

        producer = NotificationProducer()

        notification_data = {
            "type": "system",
            "title": title,
            "message": message,
            "priority": priority,
            "metadata": {
                "source": "airflow",
                **kwargs
            }
        }

        producer.send_notification(notification_data)
        logger.info(f"Notification sent: {title}")

    except Exception as e:
        logger.error(f"Failed to send notification: {e}")


def task_failure_callback(context):
    """
    Callback function for task failures.
    Called when a task fails to notify admins.

    Args:
        context: Airflow task context
    """
    task = context['task']
    dag_id = task.dag_id
    task_id = task.task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')

    message = f"Task {task_id} in DAG {dag_id} failed at {execution_date}. Error: {exception}"

    # Send notification
    send_notification(
        title=f"Airflow Task Failed: {task_id}",
        message=message,
        priority="high",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=str(execution_date)
    )
