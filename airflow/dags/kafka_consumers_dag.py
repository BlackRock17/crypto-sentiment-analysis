"""
DAG for managing Kafka consumers.
Handles starting, monitoring, and restarting Kafka consumers.
"""

import sys
import logging
import subprocess
import time
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add the project root to the Python path
sys.path.append("/opt/airflow")

# Import project modules
from plugins.crypto_utils import DEFAULT_ARGS, task_failure_callback, send_notification

# Configure logging
logger = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    'kafka_consumers_management',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': task_failure_callback
    },
    description='Manages Kafka consumer processes',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    catchup=False,
    tags=['kafka', 'consumers', 'management'],
)


def check_consumer_status(consumer_type: str, **kwargs):
    """
    Check if a specific Kafka consumer is running.

    Args:
        consumer_type: Type of consumer to check (tweet, token_mention, sentiment, token_categorization)

    Returns:
        Dictionary with consumer status information
    """
    logger.info(f"Checking status of {consumer_type} consumer")

    # Map consumer type to process name pattern
    consumer_patterns = {
        'tweet': 'TweetConsumer',
        'token_mention': 'TokenMentionConsumer',
        'sentiment': 'SentimentConsumer',
        'token_categorization': 'TokenCategorizationConsumer'
    }

    pattern = consumer_patterns.get(consumer_type)
    if not pattern:
        logger.error(f"Unknown consumer type: {consumer_type}")
        return {'status': 'unknown', 'running': False, 'error': f"Unknown consumer type: {consumer_type}"}

    # Check if process is running
    try:
        # Use 'ps' command to find process
        cmd = f"ps -ef | grep {pattern} | grep -v grep | wc -l"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        count = int(result.stdout.strip())
        running = count > 0

        logger.info(f"{consumer_type} consumer is {'running' if running else 'not running'}")

        # Get process details if running
        process_info = None
        if running:
            # Get process ID and details
            cmd_pid = f"ps -ef | grep {pattern} | grep -v grep | awk '{{print $2}}'"
            result_pid = subprocess.run(cmd_pid, shell=True, capture_output=True, text=True)
            pid = result_pid.stdout.strip()

            # Get process uptime and other details
            cmd_info = f"ps -p {pid} -o pid,etime,pcpu,pmem,args -o comm= | grep -v grep"
            result_info = subprocess.run(cmd_info, shell=True, capture_output=True, text=True)
            process_info = result_info.stdout.strip()

        return {
            'consumer_type': consumer_type,
            'status': 'running' if running else 'stopped',
            'running': running,
            'process_count': count,
            'process_info': process_info
        }

    except Exception as e:
        logger.error(f"Error checking {consumer_type} consumer status: {e}")
        return {'status': 'error', 'running': False, 'error': str(e)}


def start_consumer(consumer_type: str, **kwargs):
    """
    Start a specific Kafka consumer if it's not running.

    Args:
        consumer_type: Type of consumer to start (tweet, token_mention, sentiment, token_categorization)

    Returns:
        Dictionary with consumer startup information
    """
    logger.info(f"Starting {consumer_type} consumer")

    # Check if consumer is already running
    status = check_consumer_status(consumer_type)
    if status.get('running', False):
        logger.info(f"{consumer_type} consumer is already running")
        return {'status': 'already_running', 'consumer_type': consumer_type}

    # Map consumer type to class name
    consumer_classes = {
        'tweet': 'TweetConsumer',
        'token_mention': 'TokenMentionConsumer',
        'sentiment': 'SentimentConsumer',
        'token_categorization': 'TokenCategorizationConsumer'
    }

    class_name = consumer_classes.get(consumer_type)
    if not class_name:
        logger.error(f"Unknown consumer type: {consumer_type}")
        return {'status': 'error', 'error': f"Unknown consumer type: {consumer_type}"}

    # Start the consumer
    try:
        # Prepare Python script to start consumer
        script = f"""
import sys
import time
import logging
from src.data_processing.kafka.consumers.{consumer_type}_consumer import {class_name}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

logger.info("Starting {class_name}")
consumer = {class_name}()
consumer.start()

# Run in foreground
logger.info("{class_name} started")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("Stopping {class_name}")
    consumer.stop()
    logger.info("{class_name} stopped")
"""

        # Write script to temporary file
        script_file = f"/tmp/start_{consumer_type}_consumer.py"
        with open(script_file, 'w') as f:
            f.write(script)

        # Make script executable
        os.chmod(script_file, 0o755)

        # Start consumer in background
        cmd = f"nohup python {script_file} > /tmp/{consumer_type}_consumer.log 2>&1 &"
        subprocess.run(cmd, shell=True, check=True)

        # Wait a moment for process to start
        time.sleep(2)

        # Check if consumer is now running
        status = check_consumer_status(consumer_type)

        if status.get('running', False):
            logger.info(f"{consumer_type} consumer started successfully")
            send_notification(
                title=f"Kafka Consumer Started",
                message=f"{consumer_type} consumer was started successfully",
                priority="low"
            )
            return {'status': 'started', 'consumer_type': consumer_type}
        else:
            logger.error(f"Failed to start {consumer_type} consumer")
            return {'status': 'error', 'error': f"Failed to start {consumer_type} consumer"}

    except Exception as e:
        logger.error(f"Error starting {consumer_type} consumer: {e}")
        return {'status': 'error', 'error': str(e)}


def stop_consumer(consumer_type: str, **kwargs):
    """
    Stop a specific Kafka consumer.

    Args:
        consumer_type: Type of consumer to stop (tweet, token_mention, sentiment, token_categorization)

    Returns:
        Dictionary with consumer stop information
    """
    logger.info(f"Stopping {consumer_type} consumer")

    # Map consumer type to process name pattern
    consumer_patterns = {
        'tweet': 'TweetConsumer',
        'token_mention': 'TokenMentionConsumer',
        'sentiment': 'SentimentConsumer',
        'token_categorization': 'TokenCategorizationConsumer'
    }

    pattern = consumer_patterns.get(consumer_type)
    if not pattern:
        logger.error(f"Unknown consumer type: {consumer_type}")
        return {'status': 'error', 'error': f"Unknown consumer type: {consumer_type}"}

    # Check if consumer is running
    status = check_consumer_status(consumer_type)
    if not status.get('running', False):
        logger.info(f"{consumer_type} consumer is not running")
        return {'status': 'not_running', 'consumer_type': consumer_type}

    # Stop the consumer
    try:
        # Get process ID
        cmd_pid = f"ps -ef | grep {pattern} | grep -v grep | awk '{{print $2}}'"
        result = subprocess.run(cmd_pid, shell=True, capture_output=True, text=True)
        pid = result.stdout.strip()

        if pid:
            # Kill process
            cmd_kill = f"kill {pid}"
            subprocess.run(cmd_kill, shell=True, check=True)

            # Wait a moment for process to stop
            time.sleep(2)

            # Check if consumer is now stopped
            status = check_consumer_status(consumer_type)

            if not status.get('running', False):
                logger.info(f"{consumer_type} consumer stopped successfully")
                return {'status': 'stopped', 'consumer_type': consumer_type}
            else:
                # Force kill if still running
                cmd_kill = f"kill -9 {pid}"
                subprocess.run(cmd_kill, shell=True, check=True)

                time.sleep(1)

                logger.info(f"{consumer_type} consumer force stopped")
                return {'status': 'force_stopped', 'consumer_type': consumer_type}
        else:
            logger.error(f"No PID found for {consumer_type} consumer")
            return {'status': 'error', 'error': f"No PID found for {consumer_type} consumer"}

    except Exception as e:
        logger.error(f"Error stopping {consumer_type} consumer: {e}")
        return {'status': 'error', 'error': str(e)}


def ensure_consumer_running(consumer_type: str, **kwargs):
    """
    Ensure a specific Kafka consumer is running, starting it if needed.

    Args:
        consumer_type: Type of consumer to check/start

    Returns:
        Dictionary with consumer status information
    """
    logger.info(f"Ensuring {consumer_type} consumer is running")

    # Check if consumer is running
    status = check_consumer_status(consumer_type)

    if status.get('running', False):
        logger.info(f"{consumer_type} consumer is already running")
        return {'status': 'running', 'consumer_type': consumer_type}
    else:
        logger.info(f"{consumer_type} consumer is not running, starting it")
        return start_consumer(consumer_type)


def restart_all_consumers(**kwargs):
    """
    Restart all Kafka consumers.

    Returns:
        Dictionary with restart status information
    """
    logger.info("Restarting all Kafka consumers")

    consumer_types = ['tweet', 'token_mention', 'sentiment', 'token_categorization']
    ', '
    token_categorization
    ']
    results = {}

    # Stop all consumers
    for consumer_type in consumer_types:
        results[f"stop_{consumer_type}"] = stop_consumer(consumer_type)

    # Wait a moment to ensure all consumers are stopped
    time.sleep(5)

    # Start all consumers
    for consumer_type in consumer_types:
        results[f"start_{consumer_type}"] = start_consumer(consumer_type)

    return results


def check_all_consumers(**kwargs):
    """
    Check status of all Kafka consumers.

    Returns:
        Dictionary with status of all consumers
    """
    logger.info("Checking all Kafka consumers")

    consumer_types = ['tweet', 'token_mention', 'sentiment', 'token_categorization']
    results = {}

    for consumer_type in consumer_types:
        results[consumer_type] = check_consumer_status(consumer_type)

    # Create summary
    running_count = sum(1 for status in results.values() if status.get('running', False))

    logger.info(f"{running_count} out of {len(consumer_types)} consumers are running")

    # Notify if any consumers are not running
    if running_count < len(consumer_types):
        stopped_consumers = [c_type for c_type, status in results.items() if not status.get('running', False)]

        send_notification(
            title="Kafka Consumers Not Running",
            message=f"The following Kafka consumers are not running: {', '.join(stopped_consumers)}",
            priority="high",
            stopped_consumers=stopped_consumers
        )

    return {
        'summary': {
            'total': len(consumer_types),
            'running': running_count,
            'stopped': len(consumer_types) - running_count
        },
        'consumers': results
    }


def ensure_all_consumers_running(**kwargs):
    """
    Ensure all Kafka consumers are running, starting any that are not.

    Returns:
        Dictionary with status of all consumer startup operations
    """
    logger.info("Ensuring all Kafka consumers are running")

    consumer_types = ['tweet', 'token_mention', 'sentiment