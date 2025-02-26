import pytest
import uuid
from datetime import datetime
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
from src.data_processing.crud.delete import (
    delete_solana_token,
    delete_tweet,
    delete_sentiment_analysis,
    delete_token_mention,
    delete_tweet_by_twitter_id,
    delete_solana_token_by_address,
    delete_solana_token_cascade
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


def test_delete_sentiment_analysis(db):
    """Test deleting a sentiment analysis record"""
    # Create a tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test tweet for sentiment deletion",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="test_user"
    )

    # Create sentiment analysis
    sentiment = create_sentiment_analysis(
        db=db,
        tweet_id=tweet.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.85
    )

    # Verify the sentiment analysis exists
    assert get_sentiment_analysis_by_id(db, sentiment.id) is not None

    # Delete the sentiment analysis
    result = delete_sentiment_analysis(db, sentiment.id)
    assert result is True

    # Verify the sentiment analysis is gone
    assert get_sentiment_analysis_by_id(db, sentiment.id) is None

    # Clean up
    db.delete(tweet)
    db.commit()

    print("✓ Successfully deleted sentiment analysis")


def test_delete_token_mention(db):
    """Test deleting a token mention"""
    # Create a token
    token = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TEST",
        name="Test Token"
    )

    # Create a tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test tweet for mention deletion",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="test_user"
    )

    # Create a token mention
    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token.id
    )

    # Verify the mention exists
    assert get_token_mention_by_id(db, mention.id) is not None

    # Delete the mention
    result = delete_token_mention(db, mention.id)
    assert result is True

    # Verify the mention is gone
    assert get_token_mention_by_id(db, mention.id) is None

    # Clean up
    db.delete(tweet)
    db.delete(token)
    db.commit()

    print("✓ Successfully deleted token mention")