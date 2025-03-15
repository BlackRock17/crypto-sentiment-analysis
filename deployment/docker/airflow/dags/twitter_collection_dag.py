"""
DAG for Twitter data collection from crypto influencers.
Collects tweets from various blockchain networks and crypto influencers.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.plugins.crypto_utils import DEFAULT_ARGS, task_failure_callback, get_db_session, BLOCKCHAIN_NETWORKS, \
    close_db_session
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets
from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.processor import TwitterDataProcessor
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.twitter.config import twitter_config
from src.data_processing.models.database import BlockchainNetwork
from src.data_processing.models.twitter import TwitterInfluencer

# Add the project root to the Python path
sys.path.append("/opt/airflow")

# Configure logging
logger = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    'twitter_data_collection',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': task_failure_callback
    },
    description='Collects tweets from crypto influencers across different blockchain networks',
    schedule_interval='0 */3 * * *',  # Run every 3 hours
    catchup=False,
    tags=['twitter', 'collection', 'crypto'],
)


def collect_automated_tweets_task(**kwargs):
    """
    Task to collect tweets from automated influencers.
    """

    logger.info("Starting automated tweet collection")
    success = collect_automated_tweets()

    if success:
        logger.info("Successfully collected tweets from automated influencers")
    else:
        logger.error("Failed to collect tweets from automated influencers")
        raise Exception("Tweet collection failed")

    return success


def collect_network_specific_tweets(network: str, **kwargs):
    """
    Task to collect tweets for a specific blockchain network.

    Args:
        network: Blockchain network name
    """

    logger.info(f"Collecting tweets for network: {network}")

    # Get database session
    db = get_db_session()

    try:
        # Create service
        service = TwitterCollectionService(db)

        # Get network-specific hashtags
        network_hashtags = get_network_hashtags(network, db)

        if not network_hashtags:
            logger.warning(f"No hashtags found for network {network}")
            return 0

        total_tweets = 0

        # For each hashtag, collect recent tweets
        for hashtag in network_hashtags:
            # In a real implementation, we would use the Twitter API search here
            # Since we're using the existing codebase, we'll simulate this by adding dummy data
            logger.info(f"Would collect tweets for hashtag #{hashtag}")

            # For demonstration, we'll use the manual tweet addition function
            # In a real implementation, this would be replaced with a search function
            influencer = get_network_primary_influencer(network, db)

            if influencer:
                tweet_text = f"This is a test tweet about #{hashtag} for {network} blockchain. #Crypto"
                service.add_manual_tweet(
                    influencer_username=influencer.username,
                    tweet_text=tweet_text
                )
                total_tweets += 1

        return total_tweets

    except Exception as e:
        logger.error(f"Error collecting tweets for network {network}: {e}")
        raise
    finally:
        # Close database session
        close_db_session(db)


def get_network_hashtags(network: str, db):
    """Get hashtags for a specific blockchain network."""

    network_obj = db.query(BlockchainNetwork).filter(BlockchainNetwork.name == network).first()

    if network_obj and network_obj.hashtags:
        return network_obj.hashtags

    # Default hashtags by network if not found in DB
    default_hashtags = {
        'solana': ['solana', 'sol', 'solanasummer', 'solananft'],
        'ethereum': ['ethereum', 'eth', 'ethdev', 'web3'],
        'binance': ['binance', 'bnb', 'bsc', 'binancesmartchain'],
        'polygon': ['polygon', 'matic', 'polygonnetwork'],
        'avalanche': ['avalanche', 'avax', 'avalanchenetwork']
    }

    return default_hashtags.get(network.lower(), [network])


def get_network_primary_influencer(network: str, db):
    """Get primary influencer for a specific blockchain network."""

    # Try to find influencer that mentions the network in their description
    influencer = db.query(TwitterInfluencer).filter(
        TwitterInfluencer.description.ilike(f'%{network}%'),
        TwitterInfluencer.is_active == True
    ).first()

    if influencer:
        return influencer

    # Return any active influencer if no network-specific one is found
    return db.query(TwitterInfluencer).filter(
        TwitterInfluencer.is_active == True
    ).first()


# Task for collecting tweets from automated influencers
collect_automated_task = PythonOperator(
    task_id='collect_automated_tweets',
    python_callable=collect_automated_tweets_task,
    dag=dag,
)

# Create tasks for each blockchain network
network_tasks = []
for network in BLOCKCHAIN_NETWORKS:
    network_task = PythonOperator(
        task_id=f'collect_{network}_tweets',
        python_callable=collect_network_specific_tweets,
        op_kwargs={'network': network},
        dag=dag,
    )
    network_tasks.append(network_task)

# Define task dependencies
collect_automated_task >> network_tasks
