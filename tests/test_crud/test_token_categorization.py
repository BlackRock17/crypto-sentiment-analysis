"""
Tests for token categorization functionality.
"""
import pytest
from sqlalchemy.orm import Session

from src.data_processing.models.database import (
    BlockchainToken, BlockchainNetwork, TokenCategorizationHistory, Tweet, TokenMention, SentimentAnalysis
)
from src.data_processing.crud.create import (
    create_blockchain_token, create_blockchain_network
)
from src.data_processing.crud.update import update_token_blockchain_network
from src.data_processing.crud.token_categorization import (
    create_categorization_record, get_token_categorization_history, get_categorization_stats
)
from src.data_processing.crud.core_queries import analyze_token_for_network_detection


def test_token_categorization_history(db: Session):
    """Test token categorization history creation and retrieval."""
    # Create test networks
    network1 = create_blockchain_network(
        db=db,
        name="testnet1",
        display_name="Test Network 1"
    )

    network2 = create_blockchain_network(
        db=db,
        name="testnet2",
        display_name="Test Network 2"
    )

    # Create test token
    token = create_blockchain_token(
        db=db,
        token_address="0xtest123",
        symbol="TEST",
        name="Test Token",
        blockchain_network=None  # Start with no network
    )

    # Update the token's network and create a history record
    update_token_blockchain_network(
        db=db,
        token_id=token.id,
        blockchain_network_id=network1.id,
        confidence=0.8,
        manually_verified=True,
        notes="Initial categorization"
    )

    # Get history
    history = get_token_categorization_history(db, token.id)

    # Assertions
    assert len(history) == 1
    assert history[0].token_id == token.id
    assert history[0].previous_network_id is None
    assert history[0].new_network_id == network1.id
    assert history[0].new_confidence == 0.8
    assert history[0].notes == "Initial categorization"

    # Update again to a different network
    update_token_blockchain_network(
        db=db,
        token_id=token.id,
        blockchain_network_id=network2.id,
        confidence=0.9,
        manually_verified=True,
        notes="Network correction"
    )

    # Get updated history
    history = get_token_categorization_history(db, token.id)

    # Assertions
    assert len(history) == 2
    assert history[0].previous_network_id == network1.id
    assert history[0].new_network_id == network2.id
    assert history[0].notes == "Network correction"

    # Clean up
    # First, delete history records (due to foreign key constraints)
    for record in history:
        db.delete(record)
    db.delete(token)
    db.delete(network1)
    db.delete(network2)
    db.commit()


def test_token_network_detection(db: Session):
    """Test token network detection functionality."""
    try:
        # Create test network
        network = create_blockchain_network(
            db=db,
            name="ethereum",
            display_name="Ethereum",
            hashtags=["eth", "ethereum"],
            keywords=["ethereum", "erc20", "eth"]
        )

        # Create test token
        token = create_blockchain_token(
            db=db,
            token_address="0xtest456",
            symbol="ETH20",
            name="Ethereum Test Token",
            blockchain_network=None  # Start with no network
        )

        # Create a test tweet mentioning Ethereum
        from src.data_processing.crud.create import create_tweet, create_token_mention
        from datetime import datetime

        tweet = create_tweet(
            db=db,
            tweet_id="12345",
            text="Check out this new #ethereum token ETH20! Great erc20 project.",
            created_at=datetime.utcnow(),
            author_id="testuser",
            author_username="testuser"
        )

        # Create token mention
        mention = create_token_mention(
            db=db,
            tweet_id=tweet.id,
            token_id=token.id
        )

        # Perform network detection
        result = analyze_token_for_network_detection(db, token.id)

        # Assertions
        assert result["token_id"] == token.id
        assert result["token_symbol"] == token.symbol
        assert len(result["detected_networks"]) > 0

        # At least one detected network should be ethereum
        ethereum_detected = False
        for detected in result["detected_networks"]:
            if detected["network"] == "ethereum":
                ethereum_detected = True
                break

        assert ethereum_detected, "Ethereum network should be detected"

        # If a recommendation was made, it should be ethereum
        if result["recommended_network"]:
            assert result["recommended_network"]["name"] == "ethereum"

    finally:
        # Clean up
        db.delete(mention)
        db.delete(tweet)
        db.delete(token)
        db.delete(network)
        db.commit()


def test_categorization_stats(db: Session):
    """Test categorization statistics."""
    # Create test networks
    network1 = create_blockchain_network(
        db=db,
        name="testnet3",
        display_name="Test Network 3"
    )

    network2 = create_blockchain_network(
        db=db,
        name="testnet4",
        display_name="Test Network 4"
    )

    # Create test tokens
    token1 = create_blockchain_token(
        db=db,
        token_address="0xtest789",
        symbol="TEST1",
        name="Test Token 1",
        blockchain_network=None
    )

    token2 = create_blockchain_token(
        db=db,
        token_address="0xtest101112",
        symbol="TEST2",
        name="Test Token 2",
        blockchain_network=None
    )

    # Create categorization records
    record1 = create_categorization_record(
        db=db,
        token_id=token1.id,
        new_network_id=network1.id,
        new_confidence=0.8,
        is_auto_categorized=True
    )

    record2 = create_categorization_record(
        db=db,
        token_id=token2.id,
        new_network_id=network2.id,
        new_confidence=0.9,
        is_auto_categorized=False
    )

    # Additional categorization for token1
    record3 = create_categorization_record(
        db=db,
        token_id=token1.id,
        previous_network_id=network1.id,
        new_network_id=network2.id,
        new_confidence=0.95,
        is_auto_categorized=False
    )

    # Get stats
    stats = get_categorization_stats(db, days_back=30)

    # Assertions
    assert stats["total_categorizations"] == 3
    assert stats["auto_categorizations"] == 1
    assert stats["manual_categorizations"] == 2
    assert stats["unique_tokens_categorized"] == 2
    assert stats["tokens_recategorized"] == 1

    # Clean up
    db.delete(record1)
    db.delete(record2)
    db.delete(record3)
    db.delete(token1)
    db.delete(token2)
    db.delete(network1)
    db.delete(network2)
    db.commit()
