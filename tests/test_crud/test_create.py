import pytest
import uuid
from datetime import datetime
from src.data_processing.database import get_db
from src.data_processing.crud.create import (
    create_blockchain_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention,
    create_blockchain_network
)
from src.data_processing.models.database import SentimentEnum


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


def generate_unique_address():
    """Generate unique blockchain address"""
    return f"So{uuid.uuid4().hex[:40]}2"


def generate_unique_tweet_id():
    """Generate unique tweet ID"""
    return str(uuid.uuid4().int)[:15]


def test_create_blockchain_network(db):
    """Test creating a blockchain network"""
    network = create_blockchain_network(
        db=db,
        name="ethereum",
        display_name="Ethereum",
        description="Ethereum blockchain network",
        hashtags=["ethereum", "eth", "erc20"],
        keywords=["ethereum", "eth", "defi", "metamask"]
    )

    assert network.name == "ethereum"
    assert network.display_name == "Ethereum"
    assert network.is_active == True
    assert "ethereum" in network.hashtags
    assert "defi" in network.keywords

    db.delete(network)
    db.commit()

    print("✓ Successfully created and verified blockchain network")


def test_create_blockchain_token(db):
    """Test creating a blockchain token"""
    # First create a network
    network = create_blockchain_network(
        db=db,
        name="solana",
        display_name="Solana"
    )

    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="SOL",
        name="Solana",
        blockchain_network="solana",
        blockchain_network_id=network.id,
        network_confidence=1.0,
        manually_verified=True
    )

    assert token.symbol == "SOL"
    assert token.name == "Solana"
    assert token.blockchain_network == "solana"
    assert token.blockchain_network_id == network.id
    assert token.network_confidence == 1.0
    assert token.manually_verified == True

    # Clean up
    db.delete(token)
    db.delete(network)
    db.commit()

    print("✓ Successfully created and verified blockchain token")


def test_create_token_without_network(db):
    """Test creating a token without a specific blockchain network"""
    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="UNKNOWN",
        name="Unknown Token",
        network_confidence=0.0,
        needs_review=True
    )

    assert token.symbol == "UNKNOWN"
    assert token.blockchain_network is None
    assert token.blockchain_network_id is None
    assert token.network_confidence == 0.0
    assert token.needs_review == True

    db.delete(token)
    db.commit()

    print("✓ Successfully created and verified token without network")


def test_create_tweet(db):
    """Test creating a tweet"""
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Testing $SOL on Solana!",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="crypto_tester"
    )

    assert tweet.text == "Testing $SOL on Solana!"

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
    # Create network
    network = create_blockchain_network(
        db=db,
        name="solana",
        display_name="Solana"
    )

    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="RAY",
        name="Raydium",
        blockchain_network="solana",
        blockchain_network_id=network.id
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
    db.delete(network)
    db.commit()

    print("✓ Successfully created and verified Token Mention")
