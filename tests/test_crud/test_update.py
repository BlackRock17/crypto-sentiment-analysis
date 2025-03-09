import pytest
import uuid
from datetime import datetime, timedelta
from src.data_processing.database import get_db
from src.data_processing.crud.create import (
    create_blockchain_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
)
from src.data_processing.crud.read import (
    get_blockchain_token_by_id,
    get_tweet_by_id,
    get_sentiment_analysis_by_id,
    get_token_mention_by_id
)
from src.data_processing.crud.update import (
    update_blockchain_token,
    update_tweet,
    update_sentiment_analysis,
    update_token_mention,
    update_tweet_by_twitter_id,
    update_token_blockchain_network,
    mark_token_as_verified,
    update_blockchain_network
)
from src.data_processing.models.database import SentimentEnum, BlockchainNetwork, BlockchainToken
# Ensure all models are properly imported to avoid mapper errors
from src.data_processing.models.auth import User, Token, ApiKey, PasswordReset
from src.data_processing.models.notifications import Notification, NotificationType, NotificationPriority


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


@pytest.fixture
def test_network(db):
    """Create a test blockchain network"""
    network = BlockchainNetwork(
        name="testnet",
        display_name="Test Network",
        description="Network for testing",
        hashtags=["test", "testnet"],
        keywords=["test", "blockchain", "network"]
    )
    db.add(network)
    db.commit()
    db.refresh(network)

    yield network

    # Cleanup
    db.delete(network)
    db.commit()


def test_update_blockchain_network(db, test_network):
    """Test updating a blockchain network"""
    # Update the network
    updated_network = update_blockchain_network(
        db=db,
        network_id=test_network.id,
        display_name="Updated Test Network",
        description="Updated description",
        hashtags=["updated", "test"],
        keywords=["updated", "testnet", "network"]
    )

    # Verify the update
    assert updated_network is not None
    assert updated_network.display_name == "Updated Test Network"
    assert updated_network.description == "Updated description"
    assert "updated" in updated_network.hashtags
    assert "updated" in updated_network.keywords

    print("✓ Successfully updated and verified blockchain network")


def test_update_blockchain_token(db, test_network):
    """Test updating a blockchain token"""
    # Create a token to update
    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TEST",
        name="Test Token",
        blockchain_network="testnet",
        blockchain_network_id=test_network.id,
        network_confidence=0.5
    )

    # Update the token
    updated_token = update_blockchain_token(
        db=db,
        token_id=token.id,
        symbol="UPDATED",
        name="Updated Token",
        network_confidence=0.8,
        manually_verified=True
    )

    # Verify the update
    assert updated_token is not None
    assert updated_token.symbol == "UPDATED"
    assert updated_token.name == "Updated Token"
    assert updated_token.network_confidence == 0.8
    assert updated_token.manually_verified == True
    assert updated_token.blockchain_network == "testnet"  # Should remain unchanged
    assert updated_token.blockchain_network_id == test_network.id  # Should remain unchanged

    # Clean up
    db.delete(token)
    db.commit()

    print("✓ Successfully updated and verified blockchain token")


def test_update_token_blockchain_network(db):
    """Test updating a token's blockchain network"""
    # Create two networks
    network1 = BlockchainNetwork(name="network1", display_name="Network 1")
    network2 = BlockchainNetwork(name="network2", display_name="Network 2")
    db.add(network1)
    db.add(network2)
    db.commit()

    # Create a token with network1
    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TOKEN",
        name="Token",
        blockchain_network="network1",
        blockchain_network_id=network1.id,
        network_confidence=0.5
    )

    # Update token to network2
    updated_token = update_token_blockchain_network(
        db=db,
        token_id=token.id,
        blockchain_network_id=network2.id,
        confidence=0.9,
        manually_verified=True
    )

    # Verify the update
    assert updated_token is not None
    assert updated_token.blockchain_network_id == network2.id
    assert updated_token.blockchain_network == "network2"
    assert updated_token.network_confidence == 0.9
    assert updated_token.manually_verified == True

    # Clean up
    db.delete(token)
    db.delete(network1)
    db.delete(network2)
    db.commit()

    print("✓ Successfully updated token blockchain network")


def test_mark_token_as_verified(db, test_network):
    """Test marking a token as verified"""
    # Create a token that needs review
    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="REVIEW",
        name="Review Token",
        blockchain_network="testnet",
        blockchain_network_id=test_network.id,
        network_confidence=0.6,
        manually_verified=False,
        needs_review=True
    )

    # Mark token as verified
    verified_token = mark_token_as_verified(
        db=db,
        token_id=token.id,
        verified=True,
        needs_review=False
    )

    # Verify the update
    assert verified_token is not None
    assert verified_token.manually_verified == True
    assert verified_token.needs_review == False

    # Clean up
    db.delete(token)
    db.commit()

    print("✓ Successfully marked token as verified")


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

    import time
    time.sleep(0.1)

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
    # Create networks
    network1 = BlockchainNetwork(name="network1", display_name="Network 1")
    network2 = BlockchainNetwork(name="network2", display_name="Network 2")
    db.add(network1)
    db.add(network2)
    db.commit()

    # Create tokens
    token1 = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TKN1",
        name="Token One",
        blockchain_network="network1",
        blockchain_network_id=network1.id
    )

    token2 = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="TKN2",
        name="Token Two",
        blockchain_network="network2",
        blockchain_network_id=network2.id
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

    import time
    time.sleep(0.1)

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
    db.delete(network1)
    db.delete(network2)
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
    updated_token = update_blockchain_token(db, 99999, symbol="NONEXISTENT")
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

    # Try to update non-existent blockchain network
    updated_network = update_blockchain_network(db, 99999, display_name="Non-existent Network")
    assert updated_network is None

    print("✓ Successfully handled updates to non-existent records")


def test_update_validation(db):
    """Test validation in update functions"""
    # Create a test tweet
    tweet = create_tweet(
        db=db,
        tweet_id=generate_unique_tweet_id(),
        text="Test validation",
        created_at=datetime.utcnow(),
        author_id=str(uuid.uuid4().int)[:6],
        author_username="validation_user"
    )

    # Create sentiment analysis
    sentiment = create_sentiment_analysis(
        db=db,
        tweet_id=tweet.id,
        sentiment=SentimentEnum.NEUTRAL,
        confidence_score=0.5
    )

    # Test validation for confidence score
    try:
        update_sentiment_analysis(db, sentiment.id, confidence_score=1.5)
        assert False, "Should have raised ValueError for confidence score > 1"
    except ValueError:
        # This is expected
        pass

    try:
        update_sentiment_analysis(db, sentiment.id, confidence_score=-0.1)
        assert False, "Should have raised ValueError for confidence score < 0"
    except ValueError:
        # This is expected
        pass

    # Create network and token
    network = BlockchainNetwork(name="validation_network", display_name="Validation Network")
    db.add(network)
    db.commit()

    token = create_blockchain_token(
        db=db,
        token_address=generate_unique_address(),
        symbol="VAL",
        name="Validation Token",
        blockchain_network="validation_network",
        blockchain_network_id=network.id
    )

    # Create mention
    mention = create_token_mention(
        db=db,
        tweet_id=tweet.id,
        token_id=token.id
    )

    # Test validation for non-existent references
    try:
        update_token_mention(db, mention.id, tweet_id=99999)
        assert False, "Should have raised ValueError for non-existent tweet"
    except ValueError:
        # This is expected
        pass

    try:
        update_token_mention(db, mention.id, token_id=99999)
        assert False, "Should have raised ValueError for non-existent token"
    except ValueError:
        # This is expected
        pass

    # Clean up
    db.delete(mention)
    db.delete(sentiment)
    db.delete(tweet)
    db.delete(token)
    db.delete(network)
    db.commit()

    print("✓ Successfully validated update constraints")
