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


def test_delete_tweet(db):
    """Test deleting a tweet"""
    # Create a tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test tweet for deletion",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="test_user"
    )

    # Verify the tweet exists
    assert get_tweet_by_id(db, tweet.id) is not None

    # Delete the tweet
    result = delete_tweet(db, tweet.id)
    assert result is True

    # Verify the tweet is gone
    assert get_tweet_by_id(db, tweet.id) is None

    print("✓ Successfully deleted tweet")


def test_delete_tweet_cascade(db):
    """Test deleting a tweet with cascade"""
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
        text="Test tweet for cascade deletion",
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

    # Create a token mention
    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token.id
    )

    # Verify the tweet and associated records exist
    assert get_tweet_by_id(db, tweet.id) is not None
    assert get_sentiment_analysis_by_id(db, sentiment.id) is not None
    assert get_token_mention_by_id(db, mention.id) is not None

    # Delete the tweet with cascade
    result = delete_tweet(db, tweet.id, cascade=True)
    assert result is True

    # Verify the tweet and associated records are gone
    assert get_tweet_by_id(db, tweet.id) is None
    assert get_sentiment_analysis_by_id(db, sentiment.id) is None
    assert get_token_mention_by_id(db, mention.id) is None

    # Clean up
    db.delete(token)
    db.commit()

    print("✓ Successfully deleted tweet with cascade")


def test_delete_tweet_no_cascade(db):
    """Test deleting a tweet without cascade but with associated records"""
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
        text="Test tweet for no cascade deletion",
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

    # Try to delete the tweet without cascade
    try:
        delete_tweet(db, tweet.id, cascade=False)
        assert False, "Should have raised ValueError for deleting tweet with associated records"
    except ValueError:
        # This is expected behavior
        pass

    # Verify the tweet and sentiment still exist
    assert get_tweet_by_id(db, tweet.id) is not None
    assert get_sentiment_analysis_by_id(db, sentiment.id) is not None

    # Clean up
    db.delete(sentiment)
    db.delete(tweet)
    db.delete(token)
    db.commit()

    print("✓ Successfully prevented deletion of tweet with associated records without cascade")


def test_delete_solana_token(db):
    """Test deleting a Solana token"""
    # Create a token
    token = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TEST",
        name="Test Token"
    )

    # Verify the token exists
    assert get_solana_token_by_id(db, token.id) is not None

    # Delete the token
    result = delete_solana_token(db, token.id)
    assert result is True

    # Verify the token is gone
    assert get_solana_token_by_id(db, token.id) is None

    print("✓ Successfully deleted Solana token")


def test_delete_solana_token_with_mentions(db):
    """Test deleting a Solana token that has mentions"""
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
        text="Test tweet for token deletion",
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

    # Try to delete the token with check_mentions=True
    try:
        delete_solana_token(db, token.id, check_mentions=True)
        assert False, "Should have raised ValueError for deleting token with mentions"
    except ValueError:
        # This is expected behavior
        pass

    # Verify the token still exists
    assert get_solana_token_by_id(db, token.id) is not None

    # Clean up
    db.delete(mention)
    db.delete(tweet)
    db.delete(token)
    db.commit()

    print("✓ Successfully prevented deletion of token with mentions")


def test_delete_solana_token_cascade(db):
    """Test deleting a Solana token with cascade"""
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
        text="Test tweet for token cascade deletion",
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

    # Verify the token and mention exist
    assert get_solana_token_by_id(db, token.id) is not None
    assert get_token_mention_by_id(db, mention.id) is not None

    # Delete the token with cascade
    result = delete_solana_token_cascade(db, token.id)
    assert result is True

    # Verify the token and mention are gone
    assert get_solana_token_by_id(db, token.id) is None
    assert get_token_mention_by_id(db, mention.id) is None

    # Clean up
    db.delete(tweet)
    db.commit()

    print("✓ Successfully deleted Solana token with cascade")


def test_delete_tweet_by_twitter_id(db):
    """Test deleting a tweet by Twitter ID"""
    # Create a tweet
    twitter_id = generate_unique_tweet_id()
    tweet = create_tweet(
        db=db,
        tweet_id=twitter_id,
        text="Test tweet for Twitter ID deletion",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="test_user"
    )

    # Verify the tweet exists
    assert get_tweet_by_id(db, tweet.id) is not None

    # Delete the tweet by Twitter ID
    result = delete_tweet_by_twitter_id(db, twitter_id)
    assert result is True

    # Verify the tweet is gone
    assert get_tweet_by_id(db, tweet.id) is None

    print("✓ Successfully deleted tweet by Twitter ID")


def test_delete_solana_token_by_address(db):
    """Test deleting a Solana token by address"""
    # Create a token
    address = generate_unique_address()
    token = create_solana_token(
        db=db,
        token_address=address,
        symbol="TEST",
        name="Test Token"
    )

    # Verify the token exists
    assert get_solana_token_by_id(db, token.id) is not None

    # Delete the token by address
    result = delete_solana_token_by_address(db, address)
    assert result is True

    # Verify the token is gone
    assert get_solana_token_by_id(db, token.id) is None

    print("✓ Successfully deleted Solana token by address")