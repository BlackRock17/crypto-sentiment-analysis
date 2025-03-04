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
    from src.data_collection.tasks.twitter_tasks import collect_influencer_tweets

    # Schedule Twitter data collection tasks
    scheduler.add_job(
        collect_influencer_tweets,
        'interval',
        minutes=30,  # Run every 30 minutes
        id='collect_influencer_tweets',
        replace_existing=True,
        args=[10]  # Limit to 10 tweets per influencer
    )

    logger.info("Scheduled job: collect_influencer_tweets (every 30 minutes)")


def shutdown_scheduler() -> None:
    """
    Shut down the task scheduler gracefully.
    """
    global scheduler

    if scheduler is not None:
        scheduler.shutdown()
        scheduler = None
        logger.info("Scheduler shut down successfully")