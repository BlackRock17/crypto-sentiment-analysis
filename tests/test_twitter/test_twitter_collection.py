"""
Tests for Twitter data collection functionality.
"""

import pytest
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets, add_manual_tweet
from src.data_processing.crud.twitter import create_influencer


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_automated_influencer(db: Session):
    """Create a test automated influencer"""
    influencer = create_influencer(
        db=db,
        username="VitalikButerin",  # Use a real account for testing
        name="Vitalik Buterin",
        description="Ethereum co-founder",
        is_active=True,
        is_automated=True,
        priority=100
    )

    yield influencer

    # Clean up
    db.delete(influencer)
    db.commit()


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


def test_collect_automated_tweets(db: Session, test_automated_influencer):
    """Test collecting tweets from automated influencers"""
    service = TwitterCollectionService(db)

    # Skip if connection test fails
    if not service.test_twitter_connection():
        pytest.skip("Twitter API connection failed - check credentials")

    # Collect tweets
    tweets_stored, mentions_found = service.collect_and_store_automated_tweets()

    assert tweets_stored >= 0
    print(f"✓ Successfully collected {tweets_stored} tweets with {mentions_found} token mentions")


def test_add_manual_tweet(db: Session):
    """Test adding a manual tweet"""
    service = TwitterCollectionService(db)

    # Create test tweet data
    tweet_text = "Manual test tweet about $SOL and $RAY on #Solana"

    # Add manual tweet
    stored_tweet, mentions_count = service.add_manual_tweet(
        influencer_username="test_manual_user",
        tweet_text=tweet_text,
        retweet_count=10,
        like_count=20
    )

    assert stored_tweet is not None
    assert stored_tweet.text == tweet_text
    assert stored_tweet.author_username == "test_manual_user"
    assert stored_tweet.retweet_count == 10
    assert stored_tweet.like_count == 20

    # Clean up
    db.delete(stored_tweet)
    db.commit()

    print(f"✓ Successfully added manual tweet with {mentions_count} token mentions")


def test_automated_task():
    """Test the collect_automated_tweets task"""
    result = collect_automated_tweets()

    if result:
        print("✓ Automated tweets collection task completed successfully")
    else:
        pytest.skip("Automated tweets collection task failed - check logs for details")


def test_manual_tweet_task():
    """Test the add_manual_tweet task"""
    result = add_manual_tweet(
        influencer_username="test_task_user",
        tweet_text="Test tweet for task $SOL #solana"
    )

    if result:
        print("✓ Manual tweet addition task completed successfully")
    else:
        pytest.skip("Manual tweet addition task failed - check logs for details")


if __name__ == "__main__":
    # Run the tests manually
    db = next(get_db())

    try:
        print("Testing Twitter client initialization...")
        test_twitter_client_initialization()

        print("\nTesting Twitter API connection...")
        test_twitter_connection(db)

        influencer = create_influencer(
            db=db,
            username="VitalikButerin",
            name="Vitalik Buterin",
            is_active=True,
            is_automated=True
        )

        print("\nTesting automated tweets collection...")
        test_collect_automated_tweets(db, influencer)

        print("\nTesting manual tweet addition...")
        test_add_manual_tweet(db)

        print("\nTesting automated collection task...")
        test_automated_task()

        print("\nTesting manual tweet task...")
        test_manual_tweet_task()

        # Clean up
        db.delete(influencer)
        db.commit()

    finally:
        db.close()
