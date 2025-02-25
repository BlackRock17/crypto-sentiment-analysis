import pytest
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
    get_solana_token_by_address,
    get_solana_token_by_symbol,
    get_all_solana_tokens,
    get_tweet_by_id,
    get_tweets,
    get_sentiment_analysis_by_tweet_id,
    get_token_mentions_by_token_id
)
from src.data_processing.models.database import SentimentEnum


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_data(db):
    """Create test data for read operations"""
    # Create tokens
    token1 = create_solana_token(
        db=db,
        token_address="So11111111111111111111111111111111111111112",
        symbol="SOL",
        name="Solana"
    )

    token2 = create_solana_token(
        db=db,
        token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol="USDC",
        name="USD Coin"
    )

    # Create tweets
    tweet1 = create_tweet(
        db=db,
        tweet_id="123456789",
        text="Solana is amazing! $SOL to the moon! #solana",
        created_at=datetime.utcnow() - timedelta(days=1),
        author_id="111222",
        author_username="solana_fan"
    )

    tweet2 = create_tweet(
        db=db,
        tweet_id="987654321",
        text="Just bought some $USDC on Solana. Fees are so low!",
        created_at=datetime.utcnow(),
        author_id="333444",
        author_username="crypto_trader"
    )

    # Create sentiment analyses
    sentiment1 = create_sentiment_analysis(
        db=db,
        tweet_id=tweet1.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.92
    )

    sentiment2 = create_sentiment_analysis(
        db=db,
        tweet_id=tweet2.id,
        sentiment=SentimentEnum.NEUTRAL,
        confidence_score=0.78
    )

    # Create token mentions
    mention1 = create_token_mention(
        db=db,
        tweet_id=tweet1.id,
        token_id=token1.id
    )

    mention2 = create_token_mention(
        db=db,
        tweet_id=tweet2.id,
        token_id=token2.id
    )

    # Return all created objects for test use
    test_objects = {
        "tokens": [token1, token2],
        "tweets": [tweet1, tweet2],
        "sentiments": [sentiment1, sentiment2],
        "mentions": [mention1, mention2]
    }

    yield test_objects

    # Clean up test data after tests
    for mention in [mention1, mention2]:
        db.delete(mention)

    for sentiment in [sentiment1, sentiment2]:
        db.delete(sentiment)

    for tweet in [tweet1, tweet2]:
        db.delete(tweet)

    for token in [token1, token2]:
        db.delete(token)

    db.commit()


def test_get_solana_token_by_id(db, test_data):
    """Test getting a Solana token by ID"""
    token = test_data["tokens"][0]
    retrieved_token = get_solana_token_by_id(db, token.id)

    assert retrieved_token is not None
    assert retrieved_token.id == token.id
    assert retrieved_token.symbol == "SOL"

    print("✓ Successfully retrieved token by ID")


def test_get_solana_token_by_address(db, test_data):
    """Test getting a Solana token by address"""
    token = test_data["tokens"][0]
    retrieved_token = get_solana_token_by_address(db, "So11111111111111111111111111111111111111112")

    assert retrieved_token is not None
    assert retrieved_token.id == token.id
    assert retrieved_token.symbol == "SOL"

    print("✓ Successfully retrieved token by address")


def test_get_all_solana_tokens(db, test_data):
    """Test getting all Solana tokens with filtering"""
    # Get all tokens
    all_tokens = get_all_solana_tokens(db)
    assert len(all_tokens) >= 2  # At least our 2 test tokens

    # Test filtering by symbol
    sol_tokens = get_all_solana_tokens(db, symbol_filter="SOL")
    assert any(token.symbol == "SOL" for token in sol_tokens)

    # Test filtering by name
    usdc_tokens = get_all_solana_tokens(db, name_filter="USD")
    assert any(token.name == "USD Coin" for token in usdc_tokens)

    print("✓ Successfully retrieved and filtered tokens")


def test_get_tweets(db, test_data):
    """Test getting tweets with filtering"""
    # Get tweets by author
    fan_tweets = get_tweets(db, author_username="solana_fan")
    assert len(fan_tweets) > 0
    assert fan_tweets[0].author_username == "solana_fan"

    # Get tweets by sentiment
    positive_tweets = get_tweets(db, sentiment=SentimentEnum.POSITIVE)
    assert len(positive_tweets) > 0

    # Get tweets by token
    sol_tweets = get_tweets(db, token_symbol="SOL")
    assert len(sol_tweets) > 0
    assert "SOL" in sol_tweets[0].text

    print("✓ Successfully retrieved and filtered tweets")


def test_get_sentiment_analysis(db, test_data):
    """Test getting sentiment analysis for a tweet"""
    tweet = test_data["tweets"][0]
    sentiment = get_sentiment_analysis_by_tweet_id(db, tweet.id)

    assert sentiment is not None
    assert sentiment.sentiment == SentimentEnum.POSITIVE
    assert sentiment.confidence_score > 0.9

    print("✓ Successfully retrieved sentiment analysis")


def test_get_token_mentions(db, test_data):
    """Test getting token mentions"""
    token = test_data["tokens"][0]
    mentions = get_token_mentions_by_token_id(db, token.id)

    assert len(mentions) > 0
    assert mentions[0].token_id == token.id

    print("✓ Successfully retrieved token mentions")