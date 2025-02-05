from datetime import datetime
from src.data_processing.database import get_db
from src.data_processing.crud import create_solana_token, create_tweet, create_sentiment_analysis
from src.data_processing.models.database import SentimentEnum


def test_create_solana_token():
    db = next(get_db())

    # Test creating a token with unique address
    token = create_solana_token(
        db=db,
        token_address="So11111111111111111111111111111111111111115",  # Changed address
        symbol="USDTt",
        name="USDt Coin"
    )

    # Verify token was created
    assert token.token_address == "So11111111111111111111111111111111111111115"
    assert token.symbol == "USDTt"
    assert token.name == "USDt Coin"

    print("✓ Successfully created and verified Solana token")


def test_create_tweet():
    """Test creating a tweet"""
    db = next(get_db())

    tweet = create_tweet(
        db=db,
        tweet_id="1234567893",  # Different from test_database.py
        text="Testing $USDTt on Solana!",
        created_at=datetime.utcnow(),
        author_id="123457",
        author_username="crypto_tester1"
    )

    assert tweet.tweet_id == "1234567893"
    assert tweet.text == "Testing $USDTt on Solana!"
    assert tweet.author_id == "123457"

    print("✓ Successfully created and verified Tweet")


def test_create_sentiment_analysis():
    """Test creating a sentiment analysis"""
    db = next(get_db())

    # First we create a tweet
    tweet = create_tweet(
        db=db,
        tweet_id="9876543210",
        text="Solana to the moon! 🚀",
        created_at=datetime.utcnow(),
        author_id="555555",
        author_username="solana_fan"
    )

    # Create a sentiment analysis
    sentiment = create_sentiment_analysis(
        db=db,
        tweet_id=tweet.id,  # We use the ID from the database
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.95
    )

    assert sentiment.tweet_id == tweet.id
    assert sentiment.sentiment == SentimentEnum.POSITIVE
    assert sentiment.confidence_score == 0.95

    print("✓ Successfully created and verified Sentiment Analysis")