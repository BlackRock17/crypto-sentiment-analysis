# tests/test_models.py
"""
Tests for database models.
"""
import pytest
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from src.data_processing.database import get_db
from src.data_processing.models.database import (
    BlockchainToken, BlockchainNetwork, Tweet, SentimentAnalysis,
    TokenMention, SentimentEnum
)
# Import User and Notification models to ensure they are fully loaded
from src.data_processing.models.auth import User, Token, ApiKey, PasswordReset
from src.data_processing.models.notifications import Notification, NotificationType, NotificationPriority


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def blockchain_network(db):
    """Create a test blockchain network fixture"""
    network = BlockchainNetwork(
        name="ethereum",
        display_name="Ethereum",
        description="Ethereum blockchain network",
        hashtags=["ethereum", "eth", "erc20"],
        keywords=["ethereum", "eth", "defi", "metamask"],
        is_active=True
    )
    db.add(network)
    db.commit()
    db.refresh(network)

    yield network

    # Clean up
    db.delete(network)
    db.commit()


@pytest.fixture
def blockchain_token(db, blockchain_network):
    """Create a test blockchain token fixture"""
    token = BlockchainToken(
        token_address="0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        symbol="UNI",
        name="Uniswap",
        blockchain_network="ethereum",
        blockchain_network_id=blockchain_network.id,
        network_confidence=0.95,
        manually_verified=True,
        needs_review=False
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    yield token

    # Clean up
    db.delete(token)
    db.commit()


def test_blockchain_network_model(db):
    """Test the BlockchainNetwork model"""
    # Create a test network
    network = BlockchainNetwork(
        name="solana",
        display_name="Solana",
        description="Solana blockchain network",
        hashtags=["solana", "sol"],
        keywords=["solana", "phantom"],
        is_active=True,
        website_url="https://solana.com",
        explorer_url="https://explorer.solana.com"
    )
    db.add(network)
    db.commit()

    # Retrieve the network from the database
    db_network = db.query(BlockchainNetwork).filter_by(name="solana").first()

    # Verify all fields
    assert db_network is not None
    assert db_network.name == "solana"
    assert db_network.display_name == "Solana"
    assert db_network.description == "Solana blockchain network"
    assert db_network.hashtags == ["solana", "sol"]
    assert db_network.keywords == ["solana", "phantom"]
    assert db_network.is_active is True
    assert db_network.website_url == "https://solana.com"
    assert db_network.explorer_url == "https://explorer.solana.com"
    assert db_network.created_at is not None
    assert db_network.updated_at is not None

    # Clean up
    db.delete(db_network)
    db.commit()

    print("✓ BlockchainNetwork model successfully tested")


def test_blockchain_network_unique_name(db):
    """Test that BlockchainNetwork name must be unique"""
    # Create a network
    network1 = BlockchainNetwork(name="bitcoin", display_name="Bitcoin")
    db.add(network1)
    db.commit()

    # Try to create another network with the same name
    network2 = BlockchainNetwork(name="bitcoin", display_name="Bitcoin Copy")
    db.add(network2)

    # Should raise an integrity error
    with pytest.raises(IntegrityError):
        db.commit()

    # Rollback and clean up
    db.rollback()
    db.delete(network1)
    db.commit()

    print("✓ BlockchainNetwork unique name constraint successfully tested")


def test_blockchain_token_model(db, blockchain_network):
    """Test the BlockchainToken model"""
    # Create a test token
    token = BlockchainToken(
        token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        symbol="USDC",
        name="USD Coin",
        blockchain_network="ethereum",
        blockchain_network_id=blockchain_network.id,
        network_confidence=1.0,
        manually_verified=True,
        needs_review=False
    )
    db.add(token)
    db.commit()

    # Retrieve the token from the database
    db_token = db.query(BlockchainToken).filter_by(symbol="USDC").first()

    # Verify all fields
    assert db_token is not None
    assert db_token.token_address == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    assert db_token.symbol == "USDC"
    assert db_token.name == "USD Coin"
    assert db_token.blockchain_network == "ethereum"
    assert db_token.blockchain_network_id == blockchain_network.id
    assert db_token.network_confidence == 1.0
    assert db_token.manually_verified is True
    assert db_token.needs_review is False
    assert db_token.created_at is not None
    assert db_token.updated_at is not None

    # Clean up
    db.delete(db_token)
    db.commit()

    print("✓ BlockchainToken model successfully tested")


def test_blockchain_token_network_relationship(db, blockchain_network):
    """Test the relationship between BlockchainToken and BlockchainNetwork"""
    # Create a token with a network relationship
    token = BlockchainToken(
        token_address="0x2b591e99afE9f32eAA6214f7B7629768c40Eeb39",
        symbol="HEX",
        name="HEX",
        blockchain_network="ethereum",
        blockchain_network_id=blockchain_network.id
    )
    db.add(token)
    db.commit()

    # Retrieve the token with its network
    db_token = db.query(BlockchainToken).filter_by(symbol="HEX").first()

    # Verify the relationship
    assert db_token is not None
    assert db_token.network is not None
    assert db_token.network.id == blockchain_network.id
    assert db_token.network.name == "ethereum"

    # Check reverse relationship
    network_tokens = db.query(BlockchainToken).filter_by(blockchain_network_id=blockchain_network.id).all()
    assert len(network_tokens) >= 1
    assert any(t.symbol == "HEX" for t in network_tokens)

    # Clean up
    db.delete(db_token)
    db.commit()

    print("✓ BlockchainToken-to-BlockchainNetwork relationship successfully tested")


def test_token_mention_relationships(db, blockchain_token):
    """Test relationships between tokens, tweets, and mentions"""
    # Create a tweet
    tweet = Tweet(
        tweet_id="987654321",
        text="Check out $UNI on Ethereum! #defi",
        created_at=datetime.utcnow(),
        author_id="user123",
        author_username="defi_user"
    )
    db.add(tweet)
    db.commit()

    # Create a token mention
    mention = TokenMention(
        tweet_id=tweet.id,
        token_id=blockchain_token.id
    )
    db.add(mention)
    db.commit()

    # Add a sentiment analysis
    sentiment = SentimentAnalysis(
        tweet_id=tweet.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.82
    )
    db.add(sentiment)
    db.commit()

    # Test the relationships
    db_mention = db.query(TokenMention).filter_by(tweet_id=tweet.id).first()
    assert db_mention is not None
    assert db_mention.token_id == blockchain_token.id

    # Test relationship from token to mentions
    assert len(blockchain_token.mentions) > 0
    assert blockchain_token.mentions[0].tweet_id == tweet.id

    # Test tweet to sentiment analysis relationship
    assert tweet.sentiment_analysis is not None
    assert tweet.sentiment_analysis.sentiment == SentimentEnum.POSITIVE

    # Clean up
    db.delete(sentiment)
    db.delete(db_mention)
    db.delete(tweet)
    db.commit()

    print("✓ Token mention relationships successfully tested")


def test_token_with_unknown_network(db):
    """Test creating a token with unknown/uncategorized network"""
    # Create a token without specifying a network
    token = BlockchainToken(
        token_address="unknown_address_XYZ",
        symbol="XYZ",
        name="Unknown Token",
        network_confidence=0.0,
        needs_review=True
    )
    db.add(token)
    db.commit()

    # Retrieve the token
    db_token = db.query(BlockchainToken).filter_by(symbol="XYZ").first()

    # Verify token properties
    assert db_token is not None
    assert db_token.blockchain_network is None
    assert db_token.blockchain_network_id is None
    assert db_token.network_confidence == 0.0
    assert db_token.needs_review is True

    # Clean up
    db.delete(db_token)
    db.commit()

    print("✓ Token with unknown network successfully tested")


def test_token_with_low_confidence_network(db, blockchain_network):
    """Test creating a token with low confidence in network assignment"""
    # Create a token with low confidence network assignment
    token = BlockchainToken(
        token_address="uncertain_address_ABC",
        symbol="ABC",
        name="Uncertain Token",
        blockchain_network="ethereum",  # Assigned network
        blockchain_network_id=blockchain_network.id,
        network_confidence=0.3,  # Low confidence
        needs_review=True  # Marked for review
    )
    db.add(token)
    db.commit()

    # Retrieve the token
    db_token = db.query(BlockchainToken).filter_by(symbol="ABC").first()

    # Verify token properties
    assert db_token is not None
    assert db_token.blockchain_network == "ethereum"
    assert db_token.blockchain_network_id == blockchain_network.id
    assert db_token.network_confidence == 0.3
    assert db_token.needs_review is True

    # Clean up
    db.delete(db_token)
    db.commit()

    print("✓ Token with low confidence network successfully tested")
