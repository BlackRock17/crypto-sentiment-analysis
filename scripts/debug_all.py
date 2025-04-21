"""
Debug script that runs both the API and consumer in one process.
"""
import threading
import time
import uvicorn
from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer


def run_api():
    """Run the FastAPI application."""
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=False)


def run_consumer():
    """Run the tweet consumer."""
    consumer = TweetConsumer()
    consumer.start()


def run_sentiment_consumer():
    """Run the market sentiment consumer."""
    from src.data_processing.kafka.consumers.market_sentiment_consumer import MarketSentimentConsumer

    consumer = MarketSentimentConsumer()
    consumer.start()


if __name__ == "__main__":
    # Start API in a separate thread
    api_thread = threading.Thread(target=run_api)
    api_thread.daemon = True
    api_thread.start()

    # Start tweet consumer in a separate thread
    tweet_thread = threading.Thread(target=run_consumer)
    tweet_thread.daemon = True
    tweet_thread.start()

    # Give API time to start
    print("Starting API...")
    time.sleep(2)

    # Start sentiment consumer in the main thread
    print("Starting sentiment consumer...")
    run_sentiment_consumer()
