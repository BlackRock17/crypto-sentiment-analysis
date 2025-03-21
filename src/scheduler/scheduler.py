"""
Scheduler setup and configuration.
Provides functions to set up and manage scheduled tasks.
"""

import logging
from typing import Optional, Dict, Any
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
    archive_inactive_tokens, advanced_duplicate_detection
)

# Import token enrichment tasks
from src.scheduled_tasks.token_enrichment import (
    enrich_uncategorized_tokens,
    update_token_information, auto_categorize_tokens
)

# Import Kafka consumers
from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer
from src.data_processing.kafka.consumers.token_mention_consumer import TokenMentionConsumer
from src.data_processing.kafka.consumers.sentiment_consumer import SentimentConsumer
from src.data_processing.kafka.consumers.token_categorization_consumer import TokenCategorizationConsumer

# Configure logger
logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: Optional[AsyncIOScheduler] = None

# Global Kafka consumers
kafka_consumers: Dict[str, Any] = {}


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

    # Start Kafka consumers
    _start_kafka_consumers()

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

    # Schedule token categorization tasks (daily at 1:30 AM)
    scheduler.add_job(
        auto_categorize_tokens,
        'cron',
        hour=1,
        minute=30,
        id='auto_categorize_tokens',
        replace_existing=True
    )

    logger.info("Scheduled job: auto_categorize_tokens (daily at 1:30 AM)")

    # Schedule advanced duplicate detection (daily at 2:30 AM)
    scheduler.add_job(
        advanced_duplicate_detection,
        'cron',
        hour=2,
        minute=30,
        id='advanced_duplicate_detection',
        replace_existing=True
    )

    logger.info("Scheduled job: advanced_duplicate_detection (daily at 2:30 AM)")

    # Schedule automatic merging of exact duplicates (weekly on Tuesday at 3:30 AM)
    scheduler.add_job(
        auto_merge_exact_duplicates,
        'cron',
        day_of_week='tue',
        hour=3,
        minute=30,
        id='auto_merge_exact_duplicates',
        replace_existing=True
    )

    logger.info("Scheduled job: auto_merge_exact_duplicates (weekly on Tuesday at 3:30 AM)")


def _start_kafka_consumers():
    """
    Start Kafka consumers as background services.
    """
    global kafka_consumers

    try:
        logger.info("Starting Kafka consumers...")

        # Initialize and start tweet consumer
        tweet_consumer = TweetConsumer()
        tweet_consumer.start()
        kafka_consumers['tweet_consumer'] = tweet_consumer
        logger.info("Tweet consumer started")

        # Initialize and start token mention consumer
        token_mention_consumer = TokenMentionConsumer()
        token_mention_consumer.start()
        kafka_consumers['token_mention_consumer'] = token_mention_consumer
        logger.info("Token mention consumer started")

        # Initialize and start sentiment consumer
        sentiment_consumer = SentimentConsumer()
        sentiment_consumer.start()
        kafka_consumers['sentiment_consumer'] = sentiment_consumer
        logger.info("Sentiment consumer started")

        # Initialize and start token categorization consumer
        token_categorization_consumer = TokenCategorizationConsumer()
        token_categorization_consumer.start()
        kafka_consumers['token_categorization_consumer'] = token_categorization_consumer
        logger.info("Token categorization consumer started")

        logger.info("All Kafka consumers started successfully")

    except Exception as e:
        logger.error(f"Error starting Kafka consumers: {e}")


def shutdown_scheduler() -> None:
    """
    Shut down the task scheduler and Kafka consumers gracefully.
    """
    global scheduler, kafka_consumers

    # Stop Kafka consumers
    for name, consumer in kafka_consumers.items():
        try:
            logger.info(f"Stopping Kafka consumer: {name}")
            consumer.stop()
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer {name}: {e}")

    kafka_consumers = {}

    # Stop scheduler
    if scheduler is not None:
        scheduler.shutdown()
        scheduler = None
        logger.info("Scheduler shut down successfully")
