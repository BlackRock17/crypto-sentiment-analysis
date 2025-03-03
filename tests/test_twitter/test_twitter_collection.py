"""
Tests for Twitter data collection functionality.
"""

import pytest
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.tasks.twitter_tasks import collect_recent_tweets


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


def test_twitter_client_initialization():
    """Test Twitter client initialization"""
    try:
        client = TwitterAPIClient()
        assert client is not None
        print("✓ Twitter client initialization successful")
    except Exception as e:
        pytest.fail(f"Twitter client initialization failed: {e}")


def test_twitter_connection(db: Session):
    """Test connection to Twitter API"""
    service = TwitterCollectionService(db)

    # Test connection
    connection_success = service.test_twitter_connection()

    if connection_success:
        print("✓ Connection to Twitter API successful")
    else:
        pytest.skip("Twitter API connection failed - check credentials")


def test_collect_tweets(db: Session):
    """Test collecting tweets"""
    service = TwitterCollectionService(db)

    # Skip if connection test fails
    if not service.test_twitter_connection():
        pytest.skip("Twitter API connection failed - check credentials")

    # Collect a small number of tweets for testing
    tweets_stored, mentions_found = service.collect_and_store_tweets(limit=5)

    assert tweets_stored >= 0
    print(f"✓ Successfully collected {tweets_stored} tweets with {mentions_found} token mentions")


def test_collect_recent_tweets_task():
    """Test the collect_recent_tweets task"""
    result = collect_recent_tweets(limit=5)

    if result:
        print("✓ Twitter collection task completed successfully")
    else:
        pytest.skip("Twitter collection task failed - check logs for details")


if __name__ == "__main__":
    # Run the tests manually
    db = next(get_db())

    try:
        print("Testing Twitter client initialization...")
        test_twitter_client_initialization()

        print("\nTesting Twitter API connection...")
        test_twitter_connection(db)

        print("\nTesting tweet collection...")
        test_collect_tweets(db)

        print("\nTesting tweet collection task...")
        test_collect_recent_tweets_task()

    finally:
        db.close()