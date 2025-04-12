"""
Twitter API endpoints for handling tweets.
"""
from fastapi import APIRouter, HTTPException, status
from datetime import datetime
import logging

from src.data_processing.kafka.setup import check_kafka_connection
from src.schemas.twitter import TweetCreate
from src.data_processing.kafka.producer import send_tweet

# Set up logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/twitter", tags=["twitter"])


@router.get("/status", status_code=status.HTTP_200_OK, response_model=dict)
async def get_twitter_status():
    """
    Get the status of Twitter data processing pipeline.

    Returns:
        Dictionary with current stats and status
    """
    try:
        # Check Kafka connection
        kafka_connected = check_kafka_connection()

        # Add logic for counting tweets and mentions (temporary stub)
        # In real implementation, you would query the database
        # This is just a placeholder
        return {
            "twitter_connection": "ok" if kafka_connected else "error",
            "stored_tweets": 0,  # Later will be a DB query
            "token_mentions": 0,  # Later will be a DB query
        }
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get status: {str(e)}"
        )


@router.get("/tweets/events", response_class=EventSourceResponse)
async def tweet_events(request: Request):
    """
    Server-Sent Events endpoint for real-time tweet processing updates.
    """

    # Създаваме очакващ генератор, който ще се пробуди, когато има събитие
    async def event_generator():
        # Инициализираме queue от събитията
        if not hasattr(request.app.state, 'sse_queues'):
            request.app.state.sse_queues = {}

        # Създаваме уникален queue_id за този клиент
        queue_id = str(uuid.uuid4())
        queue = asyncio.Queue()
        request.app.state.sse_queues[queue_id] = queue

        try:
            # Изпращаме първоначално събитие с queue_id
            yield {
                "event": "connected",
                "id": queue_id,
                "data": json.dumps({"status": "connected", "queue_id": queue_id})
            }

            # Чакаме за нови събития в queue
            while True:
                # Изчакваме за събитие с timeout (за да поддържаме връзката жива)
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    yield event
                except asyncio.TimeoutError:
                    # Изпращаме heartbeat събитие, за да поддържаме връзката
                    yield {
                        "event": "heartbeat",
                        "data": json.dumps({"time": datetime.utcnow().isoformat()})
                    }
        except asyncio.CancelledError:
            # Почистваме, когато клиентът прекъсне връзката
            pass
        finally:
            # Премахваме queue, когато връзката прекъсне
            if hasattr(request.app.state, 'sse_queues') and queue_id in request.app.state.sse_queues:
                del request.app.state.sse_queues[queue_id]

    return EventSourceResponse(event_generator())


@router.post("/tweets", status_code=status.HTTP_202_ACCEPTED, response_model=dict)
async def add_manual_tweet(tweet: TweetCreate, request: Request):
    """
    Add a manually entered tweet to the system by sending it to Kafka.
    """
    # Set current time if not provided
    if not tweet.created_at:
        tweet.created_at = datetime.utcnow()

    # Генерираме уникален processing_id за тази заявка
    processing_id = str(uuid.uuid4())

    try:
        # Prepare the message for Kafka with processing_id
        tweet_data = {
            "processing_id": processing_id,
            "tweet_id": tweet.tweet_id,
            "text": tweet.text,
            "created_at": tweet.created_at.isoformat(),
            "source": "manual"  # To identify manually added tweets
        }

        # Send to Kafka topic
        success = send_tweet(tweet_data)

        if success:
            logger.info(f"Manual tweet sent to Kafka: {tweet.tweet_id}, processing_id: {processing_id}")
            return {
                "status": "processing",
                "message": "Tweet sent for processing",
                "tweet_id": tweet.tweet_id,
                "processing_id": processing_id
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


# Тази функция ще се използва за известяване на клиентите
async def notify_tweet_processed(app, processing_id: str, result: dict):
    """
    Notify all connected clients about a processed tweet.
    """
    if hasattr(app.state, 'sse_queues'):
        event = {
            "event": "tweet_processed",
            "data": json.dumps({
                "processing_id": processing_id,
                **result
            })
        }

        # Изпращаме event до всички свързани клиенти
        for queue in app.state.sse_queues.values():
            await queue.put(event)