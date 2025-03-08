"""
Scheduler setup and configuration.
Provides functions to set up and manage scheduled tasks.
"""

import logging
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from pytz import utc

from config.settings import DATABASE_URL
from src.data_collection.twitter.config import twitter_config, CollectionFrequency, get_collection_frequency_hours
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets
from src.scheduled_tasks.token_maintenance import (
    check_for_duplicate_tokens,
    auto_merge_exact_duplicates,
    archive_inactive_tokens
)

# Import token enrichment tasks
from src.scheduled_tasks.token_enrichment import (
    enrich_uncategorized_tokens,
    update_token_information
)



# Configure logger
logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: Optional[AsyncIOScheduler] = None


def setup_scheduler() -> AsyncIOScheduler:
    """
    Set up and start the task scheduler.

    Returns:
        AsyncIOScheduler: Configured and started scheduler
    """
    global scheduler

    if scheduler is not None:
        logger.info("Scheduler already initialized")
        return scheduler

    # Configure job stores (using the same database as the application)
    jobstores = {
        'default': SQLAlchemyJobStore(url=DATABASE_URL)
    }

    # Configure executors
    executors = {
        'default': ThreadPoolExecutor(20)
    }

    # Create scheduler
    scheduler = AsyncIOScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone=utc
    )

    # Add scheduled jobs (will be defined later)
    _configure_scheduled_jobs(scheduler)

    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started successfully")

    return scheduler


def _configure_scheduled_jobs(scheduler: AsyncIOScheduler) -> None:
    """
    Configure all scheduled jobs.

    Args:
        scheduler: The scheduler instance
    """

    # Get collection frequency from config
    collection_frequency = twitter_config.collection_frequency
    hours_interval = get_collection_frequency_hours(collection_frequency)

    logger.info(f"Configuring Twitter collection with {collection_frequency.value} frequency ({hours_interval} hours)")

    # Schedule Twitter data collection tasks
    scheduler.add_job(
        collect_automated_tweets,
        'interval',
        hours=hours_interval,  # Use configured interval
        id='collect_automated_tweets',
        replace_existing=True
    )

    logger.info(f"Scheduled job: collect_automated_tweets (every {hours_interval} hours)")

    # Schedule token enrichment tasks

    # Enrich uncategorized tokens daily
    scheduler.add_job(
        enrich_uncategorized_tokens,
        'cron',
        hour=1,  # Run at 1:00 AM
        minute=0,
        id='enrich_uncategorized_tokens',
        replace_existing=True
    )

    logger.info("Scheduled job: enrich_uncategorized_tokens (daily at 1:00 AM)")

    # Update token information weekly
    scheduler.add_job(
        update_token_information,
        'cron',
        day_of_week='sun',  # Run on Sunday
        hour=2,  # Run at 2:00 AM
        minute=0,
        id='update_token_information',
        replace_existing=True
    )

    logger.info("Scheduled job: update_token_information (weekly on Sunday at 2:00 AM)")

    # Schedule token maintenance tasks

    # Check for duplicate tokens daily
    scheduler.add_job(
        check_for_duplicate_tokens,
        'cron',
        hour=3,  # Run at 3:00 AM
        minute=0,
        id='check_for_duplicate_tokens',
        replace_existing=True
    )

    logger.info("Scheduled job: check_for_duplicate_tokens (daily at 3:00 AM)")

    # Auto-merge exact duplicates weekly
    scheduler.add_job(
        auto_merge_exact_duplicates,
        'cron',
        day_of_week='mon',  # Run on Monday
        hour=4,  # Run at 4:00 AM
        minute=0,
        id='auto_merge_exact_duplicates',
        replace_existing=True
    )

    logger.info("Scheduled job: auto_merge_exact_duplicates (weekly on Monday at 4:00 AM)")

    # Archive inactive tokens monthly
    scheduler.add_job(
        archive_inactive_tokens,
        'cron',
        day=1,  # Run on the 1st day of the month
        hour=5,  # Run at 5:00 AM
        minute=0,
        id='archive_inactive_tokens',
        replace_existing=True
    )

    logger.info("Scheduled job: archive_inactive_tokens (monthly on the 1st at 5:00 AM)")


def shutdown_scheduler() -> None:
    """
    Shut down the task scheduler gracefully.
    """
    global scheduler

    if scheduler is not None:
        scheduler.shutdown()
        scheduler = None
        logger.info("Scheduler shut down successfully")
