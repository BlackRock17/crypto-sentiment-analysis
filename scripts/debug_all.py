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


if __name__ == "__main__":
    # Start API in a separate thread
    api_thread = threading.Thread(target=run_api)
    api_thread.daemon = True
    api_thread.start()

    # Give API time to start
    print("Starting API...")
    time.sleep(2)

    # Start consumer in the main thread (easier to debug)
    print("Starting consumer...")
    run_consumer()
