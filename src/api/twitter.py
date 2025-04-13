"""
Twitter API endpoints for handling tweets.
"""
import logging
from fastapi import APIRouter, HTTPException, status
from datetime import datetime

from src.data_processing.kafka.setup import check_kafka_connection
from src.schemas.twitter import TweetCreate
from src.data_processing.kafka.producer import send_tweet

# Set up logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/twitter", tags=["twitter"])


@router.post("/tweets", status_code=status.HTTP_202_ACCEPTED, response_model=dict)
async def add_manual_tweet(tweet: TweetCreate):
    """
    Add a manually entered tweet to the system by sending it to Kafka.
    """
    # Set current time if not provided
    if not tweet.created_at:
        tweet.created_at = datetime.utcnow()

    try:
        # Prepare the message for Kafka
        tweet_data = {
            "tweet_id": tweet.tweet_id,
            "text": tweet.text,
            "created_at": tweet.created_at.isoformat(),
            "source": "manual"  # To identify manually added tweets
        }

        # Send to Kafka topic
        success = send_tweet(tweet_data)

        if success:
            logger.info(f"Manual tweet sent to Kafka: {tweet.tweet_id}")
            return {
                "status": "processing",
                "message": "Tweet sent for processing",
                "tweet_id": tweet.tweet_id
            }
        else:
            logger.error(f"Failed to send tweet to Kafka: {tweet.tweet_id}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process tweet: Kafka send error"
            )

    except Exception as e:
        logger.error(f"Failed to send tweet to Kafka: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process tweet: {str(e)}"
        )


@router.get("/status", status_code=status.HTTP_200_OK, response_model=dict)
async def get_twitter_status():
    """
    Get the status of Twitter data processing pipeline.
    """
    try:
        # Check Kafka connection
        kafka_connected = check_kafka_connection()

        return {
            "twitter_connection": "ok" if kafka_connected else "error"
        }
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get status: {str(e)}"
        )
