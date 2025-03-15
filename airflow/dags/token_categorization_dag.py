"""
DAG for token categorization and maintenance.
Handles token categorization, duplicate detection, and network assignment.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from airflow.plugins.crypto_utils import DEFAULT_ARGS, task_failure_callback, send_notification
from src.scheduled_tasks.token_enrichment import auto_categorize_tokens, enrich_uncategorized_tokens
from src.scheduled_tasks.token_maintenance import advanced_duplicate_detection, auto_merge_exact_duplicates, archive_inactive_tokens

# Add the project root to the Python path
sys.path.append("/opt/airflow")

# Configure logging
logger = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    'token_categorization',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': task_failure_callback
    },
    description='Categorizes and maintains blockchain tokens in the database',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    catchup=False,
    tags=['tokens', 'categorization', 'maintenance'],
)


def auto_categorize_tokens_task(**kwargs):
    """
    Task to automatically categorize tokens with high confidence.
    """

    logger.info("Starting automatic token categorization")

    # Parameters for categorization
    min_confidence = 0.7  # Minimum confidence for auto-categorization
    max_tokens = 50  # Maximum tokens to process in one run

    # Run the categorization
    result = auto_categorize_tokens(min_confidence=min_confidence, max_tokens=max_tokens)

    # Log the results
    auto_categorized = result.get('auto_categorized', 0)
    flagged_for_review = result.get('flagged_for_review', 0)

    logger.info(f"Auto-categorized {auto_categorized} tokens")
    logger.info(f"Flagged {flagged_for_review} tokens for manual review")

    # Create a notification about the results
    send_notification(
        title=f"Token Categorization Results",
        message=f"Auto-categorized {auto_categorized} tokens and flagged {flagged_for_review} for review.",
        priority="medium",
        auto_categorized=auto_categorized,
        flagged_for_review=flagged_for_review
    )

    return {
        'auto_categorized': auto_categorized,
        'flagged_for_review': flagged_for_review
    }


def detect_duplicate_tokens_task(**kwargs):
    """
    Task to detect and report potential duplicate tokens.
    """

    logger.info("Starting duplicate token detection")

    # Parameters for duplicate detection
    similarity_threshold = 0.85  # Minimum similarity to consider tokens as duplicates
    max_groups = 20  # Maximum number of duplicate groups to process

    # Run the duplicate detection
    result = advanced_duplicate_detection(
        similarity_threshold=similarity_threshold,
        max_groups=max_groups
    )

    # Log the results
    duplicate_groups = result.get('duplicate_groups', 0)
    total_duplicates = result.get('total_duplicates', 0)

    logger.info(f"Found {duplicate_groups} duplicate groups with {total_duplicates} total duplicates")

    # Create a notification if duplicates were found
    if total_duplicates > 0:
        send_notification(
            title=f"Duplicate Tokens Detected",
            message=f"Found {duplicate_groups} duplicate groups with {total_duplicates} total duplicates.",
            priority="medium" if total_duplicates > 10 else "low",
            duplicate_groups=duplicate_groups,
            total_duplicates=total_duplicates
        )

    return {
        'duplicate_groups': duplicate_groups,
        'total_duplicates': total_duplicates
    }


def merge_exact_duplicates_task(**kwargs):
    """
    Task to automatically merge exact duplicate tokens.
    """

    logger.info("Starting automatic merging of exact duplicate tokens")

    # Run with dry_run=False to actually perform the merges
    result = auto_merge_exact_duplicates(dry_run=False)

    # Log the results
    groups_processed = result.get('groups_processed', 0)
    tokens_merged = result.get('tokens_merged', 0)

    logger.info(f"Processed {groups_processed} groups and merged {tokens_merged} tokens")

    # Create a notification if tokens were merged
    if tokens_merged > 0:
        send_notification(
            title=f"Tokens Automatically Merged",
            message=f"Merged {tokens_merged} duplicate tokens across {groups_processed} groups.",
            priority="medium",
            groups_processed=groups_processed,
            tokens_merged=tokens_merged
        )

    return {
        'groups_processed': groups_processed,
        'tokens_merged': tokens_merged
    }


def enrich_token_information_task(**kwargs):
    """
    Task to enrich token information from external sources.
    """

    logger.info("Starting token enrichment for uncategorized tokens")

    # Run the enrichment process
    # This is an asyncio function, so we need to run it in an event loop
    import asyncio
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(enrich_uncategorized_tokens())

    # Since this is a background task, we don't have a specific result to return
    # Just log that it was executed
    logger.info("Token enrichment task completed")

    return True


def archive_inactive_tokens_task(**kwargs):
    """
    Task to archive tokens that have had no activity for a specified period.
    """

    logger.info("Starting archival of inactive tokens")

    # Archive tokens with no activity for 90 days
    days_inactive = 90

    # Run the archival process
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(archive_inactive_tokens(days_inactive=days_inactive))

    logger.info(f"Archived inactive tokens (>= {days_inactive} days inactive)")

    return True


# Define tasks
auto_categorize_task = PythonOperator(
    task_id='auto_categorize_tokens',
    python_callable=auto_categorize_tokens_task,
    dag=dag,
)

detect_duplicates_task = PythonOperator(
    task_id='detect_duplicate_tokens',
    python_callable=detect_duplicate_tokens_task,
    dag=dag,
)

merge_duplicates_task = PythonOperator(
    task_id='merge_exact_duplicates',
    python_callable=merge_exact_duplicates_task,
    dag=dag,
)

enrich_tokens_task = PythonOperator(
    task_id='enrich_token_information',
    python_callable=enrich_token_information_task,
    dag=dag,
)

archive_tokens_task = PythonOperator(
    task_id='archive_inactive_tokens',
    python_callable=archive_inactive_tokens_task,
    dag=dag,
)

# Define dependencies
auto_categorize_task >> detect_duplicates_task >> merge_duplicates_task
enrich_tokens_task >> auto_categorize_task
merge_duplicates_task >> archive_tokens_task
