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


def test_update_sentiment_analysis(db):
    """Test updating sentiment analysis"""
    # Create a tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test tweet for sentiment analysis",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="sentiment_user"
    )

    # Create sentiment analysis to update
    sentiment = create_sentiment_analysis(
        db=db,
        tweet_id=tweet.id,
        sentiment=SentimentEnum.NEUTRAL,
        confidence_score=0.5
    )

    # Sleep briefly to ensure we can detect the timestamp change
    original_analyzed_at = sentiment.analyzed_at

    # Update the sentiment analysis
    updated_sentiment = update_sentiment_analysis(
        db=db,
        sentiment_id=sentiment.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.9
    )

    # Verify the update
    assert updated_sentiment is not None
    assert updated_sentiment.sentiment == SentimentEnum.POSITIVE
    assert updated_sentiment.confidence_score == 0.9
    assert updated_sentiment.analyzed_at > original_analyzed_at

    # Clean up
    db.delete(sentiment)
    db.delete(tweet)
    db.commit()

    print("✓ Successfully updated and verified Sentiment Analysis")


def test_update_token_mention(db):
    """Test updating token mention"""
    # Create tokens
    token1 = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TKN1",
        name="Token One"
    )

    token2 = create_solana_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TKN2",
        name="Token Two"
    )

    # Create tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test tweet for token mention",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="token_user"
    )

    # Create mention to update
    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token1.id
    )

    # Sleep briefly to ensure we can detect the timestamp change
    original_mentioned_at = mention.mentioned_at

    # Update the token mention
    updated_mention = update_token_mention(
        db=db,
        mention_id=mention.id,
        token_id=token2.id
    )

    # Verify the update
    assert updated_mention is not None
    assert updated_mention.token_id == token2.id
    assert updated_mention.mentioned_at > original_mentioned_at

    # Clean up
    db.delete(mention)
    db.delete(tweet)
    db.delete(token1)
    db.delete(token2)
    db.commit()

    print("✓ Successfully updated and verified Token Mention")


def test_update_tweet_by_twitter_id(db):
    """Test updating a tweet by Twitter ID"""
    # Create a tweet to update
    twitter_id = generate_unique_tweet_id()
    tweet = create_tweet(
        db=db,
        tweet_id=twitter_id,
        text="Original tweet text",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="original_user"
    )

    # Update the tweet by Twitter ID
    updated_tweet = update_tweet_by_twitter_id(
        db=db,
        twitter_id=twitter_id,
        retweet_count=15,
        like_count=25
    )

    # Verify the update
    assert updated_tweet is not None
    assert updated_tweet.text == "Original tweet text"  # Text shouldn't change
    assert updated_tweet.retweet_count == 15
    assert updated_tweet.like_count == 25

    # Clean up
    db.delete(tweet)
    db.commit()

    print("✓ Successfully updated and verified Tweet by Twitter ID")


def test_update_nonexistent_records(db):
    """Test updating records that don't exist"""
    # Try to update non-existent token
    updated_token = update_solana_token(db, 99999, symbol="NONEXISTENT")
    assert updated_token is None

    # Try to update non-existent tweet
    updated_tweet = update_tweet(db, 99999, text="Non-existent tweet")
    assert updated_tweet is None

    # Try to update non-existent sentiment analysis
    updated_sentiment = update_sentiment_analysis(db, 99999, confidence_score=0.8)
    assert updated_sentiment is None

    # Try to update non-existent token mention
    updated_mention = update_token_mention(db, 99999, token_id=1)
    assert updated_mention is None

    # Try to update non-existent tweet by Twitter ID
    updated_tweet_by_id = update_tweet_by_twitter_id(db, "nonexistent_id", like_count=10)
    assert updated_tweet_by_id is None

    print("✓ Successfully handled updates to non-existent records")