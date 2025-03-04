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
    # Import tasks locally to avoid circular imports
    from src.data_collection.tasks.twitter_tasks import collect_automated_tweets

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


def shutdown_scheduler() -> None:
    """
    Shut down the task scheduler gracefully.
    """
    global scheduler

    if scheduler is not None:
        scheduler.shutdown()
        scheduler = None
        logger.info("Scheduler shut down successfully")
