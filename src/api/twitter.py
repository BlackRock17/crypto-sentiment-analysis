"""
Twitter API endpoints for handling tweets.
"""
from fastapi import APIRouter, HTTPException, status
from datetime import datetime
import logging

from src.schemas.twitter import TweetCreate
from src.data_processing.kafka.producer import send_tweet

# Set up logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/twitter", tags=["twitter"])


@router.post("/tweets", status_code=status.HTTP_202_ACCEPTED, response_model=dict)
async def add_manual_tweet(tweet: TweetCreate):
    """
    Add a manually entered tweet to the system by sending it to Kafka.

    Args:
        tweet: The tweet data from the request body

    Returns:
        Dictionary with status and message
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
                "status": "success",
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
