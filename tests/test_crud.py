import pytest
import uuid
from datetime import datetime
from src.data_processing.database import get_db
from src.data_processing.crud import (
    create_solana_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
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


def test_create_solana_token(db):
    """Test creating a Solana token"""
    token = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="USDC",
        name="USD Coin"
    )

    assert token.symbol == "USDC"
    assert token.name == "USD Coin"

    db.delete(token)
    db.commit()

    print("✓ Successfully created and verified Solana token")


def test_create_tweet(db):
    """Test creating a tweet"""
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Testing $USDC on Solana!",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="crypto_tester"
    )

    assert tweet.text == "Testing $USDC on Solana!"

    db.delete(tweet)
    db.commit()

    print("✓ Successfully created and verified Tweet")


def test_create_sentiment_analysis(db):
    """Test creating a sentiment analysis"""
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Solana to the moon! 🚀",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="solana_fan"
    )

    sentiment = create_sentiment_analysis(
        db=db,
        tweet_id=tweet.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.95
    )

    assert sentiment.sentiment == SentimentEnum.POSITIVE
    assert sentiment.confidence_score == 0.95

    db.delete(sentiment)
    db.delete(tweet)
    db.commit()

    print("✓ Successfully created and verified Sentiment Analysis")


def test_create_token_mention(db):
    """Test creating a token mention"""
    token = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="RAY",
        name="Raydium"
    )

    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Check out $RAY!",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="defi_lover"
    )

    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token.id
    )

    assert mention.tweet_id == tweet.id
    assert mention.token_id == token.id

    db.delete(mention)
    db.delete(tweet)
    db.delete(token)
    db.commit()

    print("✓ Successfully created and verified Token Mention")