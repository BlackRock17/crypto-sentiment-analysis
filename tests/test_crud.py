from datetime import datetime
from src.data_processing.database import get_db
from src.data_processing.crud import create_solana_token, create_tweet


def test_create_solana_token():
    db = next(get_db())

    # Test creating a token with unique address
    token = create_solana_token(
        db=db,
        token_address="So11111111111111111111111111111111111111114",  # Changed address
        symbol="USDT",
        name="USD Coin"
    )

    # Verify token was created
    assert token.token_address == "So11111111111111111111111111111111111111114"
    assert token.symbol == "USDT"
    assert token.name == "USD Coin"

    print("✓ Successfully created and verified Solana token")


def test_create_tweet():
    """Test creating a tweet"""
    db = next(get_db())

    tweet = create_tweet(
        db=db,
        tweet_id="1234567892",  # Different from test_database.py
        text="Testing $USDT on Solana!",
        created_at=datetime.utcnow(),
        author_id="123457",
        author_username="crypto_tester1"
    )

    assert tweet.tweet_id == "1234567892"
    assert tweet.text == "Testing $USDT on Solana!"
    assert tweet.author_id == "123457"

    print("✓ Successfully created and verified Tweet")