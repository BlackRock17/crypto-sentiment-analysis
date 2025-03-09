import pytest
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
    get_blockchain_token_by_address,
    get_blockchain_token_by_symbol,
    get_all_blockchain_tokens,
    get_tweet_by_id,
    get_tweets,
    get_sentiment_analysis_by_tweet_id,
    get_token_mentions_by_token_id,
    get_blockchain_network_by_id,
    get_blockchain_network_by_name,
    get_all_blockchain_networks
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


@pytest.fixture
def test_data(db):
    """Create test data for read operations"""
    # Create networks
    network_solana = BlockchainNetwork(
        name="solana",
        display_name="Solana",
        description="Solana blockchain network",
        hashtags=["solana", "sol"],
        keywords=["solana", "phantom"]
    )

    network_ethereum = BlockchainNetwork(
        name="ethereum",
        display_name="Ethereum",
        description="Ethereum blockchain network",
        hashtags=["ethereum", "eth"],
        keywords=["ethereum", "metamask"]
    )

    db.add(network_solana)
    db.add(network_ethereum)
    db.commit()

    # Create tokens
    token1 = create_blockchain_token(
        db=db,
        token_address="So11111111111111111111111111111111111111112",
        symbol="SOL",
        name="Solana",
        blockchain_network="solana",
        blockchain_network_id=network_solana.id,
        network_confidence=1.0,
        manually_verified=True
    )

    token2 = create_blockchain_token(
        db=db,
        token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol="USDC",
        name="USD Coin",
        blockchain_network="solana",
        blockchain_network_id=network_solana.id,
        network_confidence=0.9,
        manually_verified=True
    )

    token3 = create_blockchain_token(
        db=db,
        token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        symbol="USDC",
        name="USD Coin",
        blockchain_network="ethereum",
        blockchain_network_id=network_ethereum.id,
        network_confidence=1.0,
        manually_verified=True
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

    tweet3 = create_tweet(
        db=db,
        tweet_id="555666777",
        text="$USDC on Ethereum is great for stability. #ethereum",
        created_at=datetime.utcnow() - timedelta(hours=12),
        author_id="555666",
        author_username="eth_lover"
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

    sentiment3 = create_sentiment_analysis(
        db=db,
        tweet_id=tweet3.id,
        sentiment=SentimentEnum.POSITIVE,
        confidence_score=0.85
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

    mention3 = create_token_mention(
        db=db,
        tweet_id=tweet3.id,
        token_id=token3.id
    )

    # Return all created objects for test use
    test_objects = {
        "networks": [network_solana, network_ethereum],
        "tokens": [token1, token2, token3],
        "tweets": [tweet1, tweet2, tweet3],
        "sentiments": [sentiment1, sentiment2, sentiment3],
        "mentions": [mention1, mention2, mention3]
    }

    yield test_objects

    # Clean up test data after tests
    for mention in [mention1, mention2, mention3]:
        db.delete(mention)

    for sentiment in [sentiment1, sentiment2, sentiment3]:
        db.delete(sentiment)

    for tweet in [tweet1, tweet2, tweet3]:
        db.delete(tweet)

    for token in [token1, token2, token3]:
        db.delete(token)

    for network in [network_solana, network_ethereum]:
        db.delete(network)

    db.commit()


def test_get_blockchain_token_by_id(db, test_data):
    """Test getting a blockchain token by ID"""
    token = test_data["tokens"][0]
    retrieved_token = get_blockchain_token_by_id(db, token.id)

    assert retrieved_token is not None
    assert retrieved_token.id == token.id
    assert retrieved_token.symbol == "SOL"
    assert retrieved_token.blockchain_network == "solana"

    print("✓ Successfully retrieved token by ID")


def test_get_blockchain_token_by_address(db, test_data):
    """Test getting a blockchain token by address"""
    token = test_data["tokens"][0]
    retrieved_token = get_blockchain_token_by_address(db, "So11111111111111111111111111111111111111112")

    assert retrieved_token is not None
    assert retrieved_token.id == token.id
    assert retrieved_token.symbol == "SOL"
    assert retrieved_token.blockchain_network == "solana"

    print("✓ Successfully retrieved token by address")


def test_get_blockchain_token_by_address_with_network(db, test_data):
    """Test getting a blockchain token by address and network"""
    # Test with USDC which exists on both networks
    eth_usdc = get_blockchain_token_by_address(db, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "ethereum")
    sol_usdc = get_blockchain_token_by_address(db, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "solana")

    assert eth_usdc is not None
    assert sol_usdc is not None
    assert eth_usdc.symbol == "USDC"
    assert sol_usdc.symbol == "USDC"
    assert eth_usdc.blockchain_network == "ethereum"
    assert sol_usdc.blockchain_network == "solana"

    print("✓ Successfully retrieved tokens by address and network")


def test_get_blockchain_token_by_symbol(db, test_data):
    """Test getting blockchain tokens by symbol"""
    # Get SOL token (unique symbol)
    sol_token = get_blockchain_token_by_symbol(db, "SOL")
    assert sol_token is not None
    assert sol_token.symbol == "SOL"

    # Get USDC token (exists on multiple networks) - should return any match
    usdc_token = get_blockchain_token_by_symbol(db, "USDC")
    assert usdc_token is not None
    assert usdc_token.symbol == "USDC"

    print("✓ Successfully retrieved tokens by symbol")


def test_get_blockchain_token_by_symbol_and_network(db, test_data):
    """Test getting a blockchain token by symbol and network"""
    # Get USDC on Ethereum
    eth_usdc = get_blockchain_token_by_symbol(db, "USDC", "ethereum")
    assert eth_usdc is not None
    assert eth_usdc.symbol == "USDC"
    assert eth_usdc.blockchain_network == "ethereum"

    # Get USDC on Solana
    sol_usdc = get_blockchain_token_by_symbol(db, "USDC", "solana")
    assert sol_usdc is not None
    assert sol_usdc.symbol == "USDC"
    assert sol_usdc.blockchain_network == "solana"

    print("✓ Successfully retrieved tokens by symbol and network")


def test_get_all_blockchain_tokens(db, test_data):
    """Test getting all blockchain tokens with filtering"""
    # Get all tokens
    all_tokens = get_all_blockchain_tokens(db)
    assert len(all_tokens) >= 3  # At least our 3 test tokens

    # Test filtering by symbol
    sol_tokens = get_all_blockchain_tokens(db, symbol_filter="SOL")
    assert len(sol_tokens) >= 1
    assert any(token.symbol == "SOL" for token in sol_tokens)

    # Test filtering by name
    usdc_tokens = get_all_blockchain_tokens(db, name_filter="USD")
    assert len(usdc_tokens) >= 2
    assert all(token.name == "USD Coin" for token in usdc_tokens)

    # Test filtering by blockchain network
    ethereum_tokens = get_all_blockchain_tokens(db, blockchain_network="ethereum")
    assert len(ethereum_tokens) >= 1
    assert all(token.blockchain_network == "ethereum" for token in ethereum_tokens)

    solana_tokens = get_all_blockchain_tokens(db, blockchain_network="solana")
    assert len(solana_tokens) >= 2
    assert all(token.blockchain_network == "solana" for token in solana_tokens)

    print("✓ Successfully retrieved and filtered tokens")


def test_get_blockchain_network_by_id(db, test_data):
    """Test getting a blockchain network by ID"""
    network = test_data["networks"][0]
    retrieved_network = get_blockchain_network_by_id(db, network.id)

    assert retrieved_network is not None
    assert retrieved_network.id == network.id
    assert retrieved_network.name == "solana"

    print("✓ Successfully retrieved blockchain network by ID")


def test_get_blockchain_network_by_name(db, test_data):
    """Test getting a blockchain network by name"""
    retrieved_network = get_blockchain_network_by_name(db, "ethereum")

    assert retrieved_network is not None
    assert retrieved_network.name == "ethereum"
    assert retrieved_network.display_name == "Ethereum"

    print("✓ Successfully retrieved blockchain network by name")


def test_get_all_blockchain_networks(db, test_data):
    """Test getting all blockchain networks with filtering"""
    # Get all networks
    all_networks = get_all_blockchain_networks(db)
    assert len(all_networks) >= 2  # At least our 2 test networks

    # Test filtering active networks
    active_networks = get_all_blockchain_networks(db, active_only=True)
    assert len(active_networks) >= 2  # Default is active

    # Make one network inactive and test again
    test_data["networks"][0].is_active = False
    db.commit()

    active_networks = get_all_blockchain_networks(db, active_only=True)
    assert len(active_networks) >= 1
    assert all(network.is_active for network in active_networks)

    # Reset for other tests
    test_data["networks"][0].is_active = True
    db.commit()

    print("✓ Successfully retrieved and filtered blockchain networks")


def test_get_tweets(db, test_data):
    """Test getting tweets with filtering"""
    # Get tweets by author
    fan_tweets = get_tweets(db, author_username="solana_fan")
    assert len(fan_tweets) > 0
    assert fan_tweets[0].author_username == "solana_fan"

    # Get tweets by sentiment
    positive_tweets = get_tweets(db, sentiment=SentimentEnum.POSITIVE)
    assert len(positive_tweets) >= 2

    # Get tweets by token
    sol_tweets = get_tweets(db, token_symbol="SOL")
    assert len(sol_tweets) >= 1
    assert "SOL" in sol_tweets[0].text

    # Test tweets for USDC (should find both Ethereum and Solana mentions)
    usdc_tweets = get_tweets(db, token_symbol="USDC")
    assert len(usdc_tweets) >= 2

    # We should find tweets containing "ethereum" and "solana" when filtering for USDC
    eth_related = any("ethereum" in tweet.text.lower() for tweet in usdc_tweets)
    sol_related = any("solana" in tweet.text.lower() for tweet in usdc_tweets)

    assert eth_related, "Should find USDC tweets related to Ethereum"
    assert sol_related, "Should find USDC tweets related to Solana"

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
    token = test_data["tokens"][0]  # SOL token
    mentions = get_token_mentions_by_token_id(db, token.id)

    assert len(mentions) > 0
    assert mentions[0].token_id == token.id

    print("✓ Successfully retrieved token mentions")
