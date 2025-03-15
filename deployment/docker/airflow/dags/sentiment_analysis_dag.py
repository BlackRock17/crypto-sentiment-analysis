"""
DAG for sentiment analysis of crypto token tweets.
Processes and analyzes sentiment across different blockchain networks.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# Add the project root to the Python path
sys.path.append("/opt/airflow")

# Import project modules
from airflow.plugins.crypto_utils import DEFAULT_ARGS, task_failure_callback, get_db_session, BLOCKCHAIN_NETWORKS, \
    close_db_session, send_notification

from src.data_processing.crud.core_queries import (get_network_sentiment_timeline,
                                                   compare_blockchain_networks_sentiment,
                                                   detect_trending_tokens, get_global_sentiment_trends)
# Configure logging
logger = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    'sentiment_analysis',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': task_failure_callback
    },
    description='Analyzes sentiment for crypto tokens across blockchain networks',
    schedule_interval='0 */4 * * *',  # Run every 4 hours
    catchup=False,
    tags=['sentiment', 'analysis', 'crypto'],
)


def process_network_sentiment(network: str, lookback_hours: int = 24, **kwargs):
    """
    Task to process and analyze sentiment for a specific blockchain network.

    Args:
        network: Blockchain network name
        lookback_hours: Hours to look back for tweets
    """

    logger.info(f"Processing sentiment for network: {network}")

    # Get database session
    db = get_db_session()

    try:
        # Calculate sentiment for the network
        sentiment_data = get_network_sentiment_timeline(
            db=db,
            blockchain_network=network,
            days_back=int(lookback_hours / 24),  # Convert hours to days
            interval="hour"
        )

        # Extract summary data
        total_mentions = sentiment_data.get('total_mentions', 0)
        overall_sentiment = sentiment_data.get('overall_sentiment', {})
        positive_pct = overall_sentiment.get('positive_pct', 0)
        negative_pct = overall_sentiment.get('negative_pct', 0)
        sentiment_score = overall_sentiment.get('sentiment_score', 0)

        logger.info(f"Network {network}: {total_mentions} mentions, sentiment score: {sentiment_score}")

        # Generate an alert if sentiment changes drastically
        if abs(sentiment_score) > 0.5:  # Strong sentiment in either direction
            sentiment_type = "positive" if sentiment_score > 0 else "negative"
            send_notification(
                title=f"Strong {sentiment_type} sentiment for {network}",
                message=f"{network} shows {sentiment_type} sentiment ({sentiment_score:.2f}) with {total_mentions} mentions in the last {lookback_hours} hours.",
                priority="medium" if total_mentions > 10 else "low",
                network=network,
                sentiment_score=sentiment_score,
                total_mentions=total_mentions
            )

        return {
            'network': network,
            'total_mentions': total_mentions,
            'sentiment_score': sentiment_score,
            'positive_pct': positive_pct,
            'negative_pct': negative_pct
        }

    except Exception as e:
        logger.error(f"Error processing sentiment for network {network}: {e}")
        raise
    finally:
        # Close database session
        close_db_session(db)


def compare_network_sentiments_task(lookback_hours: int = 24, **kwargs):
    """
    Task to compare sentiment across different blockchain networks.

    Args:
        lookback_hours: Hours to look back for comparison
    """

    logger.info("Comparing sentiment across blockchain networks")

    # Get database session
    db = get_db_session()

    try:
        # Get comparison data
        days_back = int(lookback_hours / 24)  # Convert hours to days
        comparison = compare_blockchain_networks_sentiment(
            db=db,
            network_names=BLOCKCHAIN_NETWORKS,
            days_back=days_back
        )

        # Process results
        networks_data = comparison.get('networks', {})

        # Sort networks by sentiment score (most positive first)
        sorted_networks = sorted(
            [(name, data.get('sentiment_score', 0)) for name, data in networks_data.items()],
            key=lambda x: x[1],
            reverse=True
        )

        # Log the rankings
        logger.info("Network sentiment rankings:")
        for rank, (network, score) in enumerate(sorted_networks, 1):
            mentions = networks_data[network].get('total_mentions', 0)
            logger.info(f"{rank}. {network}: {score:.2f} (mentions: {mentions})")

        # Check for significant differences between networks
        if len(sorted_networks) >= 2:
            top_network, top_score = sorted_networks[0]
            bottom_network, bottom_score = sorted_networks[-1]

            if top_score - bottom_score > 0.5:  # Significant difference
                send_notification(
                    title=f"Significant sentiment disparity between blockchain networks",
                    message=f"{top_network} shows much more positive sentiment ({top_score:.2f}) than {bottom_network} ({bottom_score:.2f}).",
                    priority="medium",
                    top_network=top_network,
                    bottom_network=bottom_network,
                    score_difference=top_score - bottom_score
                )

        return {
            'rankings': sorted_networks,
            'period': comparison.get('period', '')
        }

    except Exception as e:
        logger.error(f"Error comparing network sentiments: {e}")
        raise
    finally:
        # Close database session
        close_db_session(db)


def identify_trending_tokens_task(lookback_hours: int = 24, **kwargs):
    """
    Task to identify trending tokens based on sentiment and mention growth.

    Args:
        lookback_hours: Hours to look back for trending analysis
    """

    logger.info("Identifying trending tokens")

    # Get database session
    db = get_db_session()

    try:
        # Run trending token detection
        trending_data = detect_trending_tokens(
            db=db,
            lookback_window=1,  # Days for current period
            comparison_window=1,  # Days for previous period
            min_mentions=5,  # Minimum mentions to consider
            limit=10  # Top tokens to return
        )

        # Process results
        trending_tokens = trending_data.get('trending_tokens', [])

        # Log the trending tokens
        logger.info(f"Found {len(trending_tokens)} trending tokens")
        for token in trending_tokens:
            symbol = token.get('symbol', 'unknown')
            network = token.get('blockchain_network', 'unknown')
            growth = token.get('percentage_growth', 0)
            mentions = token.get('current_mentions', 0)

            logger.info(f"Token {symbol} ({network}): {growth:.1f}% growth, {mentions} mentions")

            # Notify about significant growth
            if growth > 100 and mentions >= 10:  # 100% growth and at least 10 mentions
                send_notification(
                    title=f"Trending token: {symbol} ({network})",
                    message=f"{symbol} on {network} is trending with {growth:.1f}% growth and {mentions} mentions.",
                    priority="medium",
                    symbol=symbol,
                    network=network,
                    growth=growth,
                    mentions=mentions
                )

        return {
            'trending_tokens': trending_tokens,
            'period': trending_data.get('current_period', '')
        }

    except Exception as e:
        logger.error(f"Error identifying trending tokens: {e}")
        raise
    finally:
        # Close database session
        close_db_session(db)


def generate_global_sentiment_report_task(**kwargs):
    """
    Task to generate a global sentiment report across all networks.
    """

    logger.info("Generating global sentiment report")

    # Get database session
    db = get_db_session()

    try:
        # Get global sentiment data
        global_sentiment = get_global_sentiment_trends(
            db=db,
            days_back=7,  # Past week
            interval="day",  # Daily aggregation
            top_networks=5  # Include top 5 networks
        )

        # Extract overall sentiment
        overall = global_sentiment.get('overall_sentiment', {})
        total_mentions = global_sentiment.get('total_mentions', 0)
        sentiment_score = overall.get('sentiment_score', 0)
        positive_pct = overall.get('positive_pct', 0)
        negative_pct = overall.get('negative_pct', 0)

        # Get sentiment by network
        network_sentiment = global_sentiment.get('network_sentiment', {})

        # Log the report
        logger.info(f"Global sentiment report: Score {sentiment_score:.2f}, {total_mentions} mentions")
        logger.info(f"Positive: {positive_pct:.1f}%, Negative: {negative_pct:.1f}%")

        for network, data in network_sentiment.items():
            net_score = data.get('sentiment_score', 0)
            net_mentions = data.get('total', 0)
            logger.info(f"Network {network}: Score {net_score:.2f}, {net_mentions} mentions")

        # Generate weekly report notification
        current_time = kwargs.get('execution_date', datetime.now())
        if current_time.hour < 2 and current_time.weekday() == 0:  # Monday morning
            # Format network data for the report
            network_summary = "\n".join([
                f"- {network}: {data.get('sentiment_score', 0):.2f} ({data.get('total', 0)} mentions)"
                for network, data in network_sentiment.items()
            ])

            send_notification(
                title="Weekly Crypto Sentiment Report",
                message=f"""
                Global sentiment for the past week: {sentiment_score:.2f}
                Total mentions: {total_mentions}
                Positive: {positive_pct:.1f}%
                Negative: {negative_pct:.1f}%

                Network sentiment:
                {network_summary}
                """,
                priority="medium",
                sentiment_score=sentiment_score,
                total_mentions=total_mentions
            )

        return {
            'sentiment_score': sentiment_score,
            'total_mentions': total_mentions,
            'networks': list(network_sentiment.keys())
        }

    except Exception as e:
        logger.error(f"Error generating global sentiment report: {e}")
        raise
    finally:
        # Close database session
        close_db_session(db)


# Wait for Twitter data collection to complete
twitter_collection_sensor = ExternalTaskSensor(
    task_id='wait_for_twitter_collection',
    external_dag_id='twitter_data_collection',
    external_task_id=None,  # Wait for the entire DAG
    allowed_states=['success'],
    timeout=3600,
    mode='reschedule',
    poke_interval=300,
    dag=dag,
)

# Create tasks for sentiment analysis by network
network_sentiment_tasks = []
for network in BLOCKCHAIN_NETWORKS:
    network_task = PythonOperator(
        task_id=f'process_{network}_sentiment',
        python_callable=process_network_sentiment,
        op_kwargs={'network': network},
        dag=dag,
    )
    network_sentiment_tasks.append(network_task)

# Network comparison task
compare_networks_task = PythonOperator(
    task_id='compare_network_sentiments',
    python_callable=compare_network_sentiments_task,
    dag=dag,
)

# Trending tokens task
trending_tokens_task = PythonOperator(
    task_id='identify_trending_tokens',
    python_callable=identify_trending_tokens_task,
    dag=dag,
)

# Global sentiment report task
global_report_task = PythonOperator(
    task_id='generate_global_sentiment_report',
    python_callable=generate_global_sentiment_report_task,
    dag=dag,
)

# Define task dependencies
twitter_collection_sensor >> network_sentiment_tasks
network_sentiment_tasks >> compare_networks_task
network_sentiment_tasks >> trending_tokens_task
[compare_networks_task, trending_tokens_task] >> global_report_task
