import pytest
import uuid
from datetime import datetime, timedelta
from src.data_processing.database import get_db
from src.data_processing.crud.create import (
    create_solana_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
)
from src.data_processing.crud.read import (
    get_solana_token_by_id,
    get_tweet_by_id,
    get_sentiment_analysis_by_id,
    get_token_mention_by_id
)
from src.data_processing.crud.update import (
    update_solana_token,
    update_tweet,
    update_sentiment_analysis,
    update_token_mention,
    update_tweet_by_twitter_id
)
from src.data_processing.models.database import SentimentEnum


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


def generate_unique_address():
    """Generate unique Solana address"""
    return f"So{uuid.uuid4().hex[:40]}2"


def generate_unique_tweet_id():
    """Generate unique tweet ID"""
    return str(uuid.uuid4().int)[:15]


def test_update_solana_token(db):
    """Test updating a Solana token"""
    # Create a token to update
    token = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TEST",
        name="Test Token"
    )

    # Update the token
    updated_token = update_solana_token(
        db=db,
        token_id=token.id,
        symbol="UPDATED",
        name="Updated Token"
    )

    # Verify the update
    assert updated_token is not None
    assert updated_token.symbol == "UPDATED"
    assert updated_token.name == "Updated Token"

    # Clean up
    db.delete(token)
    db.commit()

    print("✓ Successfully updated and verified Solana token")


def test_update_tweet(db):
    """Test updating a tweet"""
    # Create a tweet to update
    tweet_id = generate_unique_tweet_id()
    tweet = create_tweet(
        db=db,
        tweet_id=tweet_id,
        text="Original tweet text",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="original_user"
    )

    # Update the tweet
    updated_tweet = update_tweet(
        db=db,
        tweet_id=tweet.id,
        text="Updated tweet text",
        author_username="updated_user",
        retweet_count=10,
        like_count=20
    )

    # Verify the update
    assert updated_tweet is not None
    assert updated_tweet.text == "Updated tweet text"
    assert updated_tweet.author_username == "updated_user"
    assert updated_tweet.retweet_count == 10
    assert updated_tweet.like_count == 20

    # Clean up
    db.delete(tweet)
    db.commit()

    print("✓ Successfully updated and verified Tweet")