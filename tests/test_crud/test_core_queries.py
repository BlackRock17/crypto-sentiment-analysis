import pytest
from datetime import datetime, timedelta
from src.data_processing.database import get_db
from src.data_processing.crud.read import get_blockchain_token_by_address
from src.data_processing.crud.create import (
    create_blockchain_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
)
from src.data_processing.crud.core_queries import (
    get_token_sentiment_stats,
    get_token_sentiment_timeline,
    compare_token_sentiments,
    get_most_discussed_tokens,
    get_top_users_by_token, analyze_token_correlation, get_sentiment_momentum
)
from src.data_processing.models.database import SentimentEnum, TokenMention, SentimentAnalysis, Tweet
from src.data_processing.models.database import BlockchainToken, BlockchainNetwork
from src.data_processing.models.auth import User, Token, ApiKey, PasswordReset
from src.data_processing.models.notifications import Notification, NotificationType, NotificationPriority


def clean_test_data(db):
    """Clears test data if any are left over from previous tests"""
    # Clearing the mentions token
    db.query(TokenMention).delete()

    # Clear sentiment analysis
    db.query(SentimentAnalysis).delete()

    # Clear tweets
    db.query(Tweet).delete()

    # Clearing tokens with the specific addresses we use for testing
    test_addresses = [
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"
    ]

    for address in test_addresses:
        token = get_blockchain_token_by_address(db, address)
        if token:
            db.delete(token)

    db.commit()


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_data(db):
    """Create test data for core queries testing"""

    # Clear all previous test data
    clean_test_data(db)

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
    sol_token = create_blockchain_token(
        db=db,
        token_address="So11111111111111111111111111111111111111112",
        symbol="SOL",
        name="Solana",
        blockchain_network="solana",
        blockchain_network_id=network_solana.id,
        network_confidence=1.0,
        manually_verified=True
    )

    usdc_token = create_blockchain_token(
        db=db,
        token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol="USDC",
        name="USD Coin",
        blockchain_network="solana",
        blockchain_network_id=network_solana.id,
        network_confidence=1.0,
        manually_verified=True
    )

    ray_token = create_blockchain_token(
        db=db,
        token_address="4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        symbol="RAY",
        name="Raydium",
        blockchain_network="solana",
        blockchain_network_id=network_solana.id,
        network_confidence=1.0,
        manually_verified=True
    )

    # Create ETH version of USDC
    eth_usdc_token = create_blockchain_token(
        db=db,
        token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        symbol="USDC",
        name="USD Coin",
        blockchain_network="ethereum",
        blockchain_network_id=network_ethereum.id,
        network_confidence=1.0,
        manually_verified=True
    )

    # Create tweets over several days
    now = datetime.utcnow()
    tweet_data = []

    # Helper to create tweet batches
    def create_tweet_batch(count, days_ago, sentiment, token, confidence, author="user1"):
        for i in range(count):
            tweet = create_tweet(
                db=db,
                tweet_id=f"tweet_{token.symbol}_{days_ago}_{i}",
                text=f"Test tweet about ${token.symbol} with {sentiment.value.lower()} sentiment",
                created_at=now - timedelta(days=days_ago, hours=i),
                author_id=f"author_{author}_{i}",
                author_username=f"{author}",
                retweet_count=i,
                like_count=i * 2
            )

            sent_analysis = create_sentiment_analysis(
                db=db,
                tweet_id=tweet.id,
                sentiment=sentiment,
                confidence_score=confidence
            )

            mention = create_token_mention(
                db=db,
                tweet_id=tweet.id,
                token_id=token.id
            )

            tweet_data.append({
                "tweet": tweet,
                "sentiment": sent_analysis,
                "mention": mention
            })

    # Create tweet data for SOL with mixed sentiments across different days
    create_tweet_batch(5, 1, SentimentEnum.POSITIVE, sol_token, 0.85, "sol_fan")
    create_tweet_batch(3, 2, SentimentEnum.NEUTRAL, sol_token, 0.70, "crypto_trader")
    create_tweet_batch(2, 3, SentimentEnum.NEGATIVE, sol_token, 0.75, "critic")

    # Create tweet data for USDC on Solana
    create_tweet_batch(4, 1, SentimentEnum.POSITIVE, usdc_token, 0.80, "solana_stablecoin_user")
    create_tweet_batch(2, 2, SentimentEnum.NEUTRAL, usdc_token, 0.65, "solana_investor")

    # Create tweet data for USDC on Ethereum
    create_tweet_batch(3, 1, SentimentEnum.POSITIVE, eth_usdc_token, 0.82, "eth_stablecoin_user")
    create_tweet_batch(2, 3, SentimentEnum.NEGATIVE, eth_usdc_token, 0.78, "eth_critic")

    # Create tweet data for RAY
    create_tweet_batch(3, 1, SentimentEnum.POSITIVE, ray_token, 0.90, "defi_lover")
    create_tweet_batch(1, 3, SentimentEnum.NEGATIVE, ray_token, 0.85, "skeptic")

    # Create tweets mentioning multiple tokens
    multi_token_tweets = []
    for i in range(3):
        tweet = create_tweet(
            db=db,
            tweet_id=f"multi_token_{i}",
            text=f"Comparing $SOL and $USDC and $RAY performance",
            created_at=now - timedelta(days=1, hours=i * 2),
            author_id=f"author_comparison_{i}",
            author_username="comparison_expert",
            retweet_count=i * 3,
            like_count=i * 5
        )

        sent_analysis = create_sentiment_analysis(
            db=db,
            tweet_id=tweet.id,
            sentiment=SentimentEnum.NEUTRAL,
            confidence_score=0.75
        )

        # Create mentions for all three tokens
        mention1 = create_token_mention(
            db=db,
            tweet_id=tweet.id,
            token_id=sol_token.id
        )

        mention2 = create_token_mention(
            db=db,
            tweet_id=tweet.id,
            token_id=usdc_token.id
        )

        mention3 = create_token_mention(
            db=db,
            tweet_id=tweet.id,
            token_id=ray_token.id
        )

        multi_token_tweets.append({
            "tweet": tweet,
            "sentiment": sent_analysis,
            "mentions": [mention1, mention2, mention3]
        })

    # Return all created objects for test use
    test_objects = {
        "networks": {
            "solana": network_solana,
            "ethereum": network_ethereum
        },
        "tokens": {
            "sol": sol_token,
            "usdc": usdc_token,
            "eth_usdc": eth_usdc_token,
            "ray": ray_token
        },
        "tweet_data": tweet_data,
        "multi_token_tweets": multi_token_tweets
    }

    yield test_objects

    # Clean up test data after tests
    for mtt in multi_token_tweets:
        for mention in mtt["mentions"]:
            db.delete(mention)
        db.delete(mtt["sentiment"])
        db.delete(mtt["tweet"])

    for td in tweet_data:
        db.delete(td["mention"])
        db.delete(td["sentiment"])
        db.delete(td["tweet"])

    db.delete(sol_token)
    db.delete(usdc_token)
    db.delete(eth_usdc_token)
    db.delete(ray_token)
    db.delete(network_solana)
    db.delete(network_ethereum)

    db.commit()


def test_get_token_sentiment_stats(db, test_data):
    """Test getting sentiment statistics for a token"""
    # Get sentiment stats for SOL
    sol_stats = get_token_sentiment_stats(
        db=db,
        token_symbol="SOL",
        days_back=7
    )

    # Verify structure and data
    assert sol_stats["token"] == "SOL"
    assert "period" in sol_stats
    assert "total_mentions" in sol_stats
    assert sol_stats["total_mentions"] > 0
    assert "sentiment_breakdown" in sol_stats
    assert "positive" in sol_stats["sentiment_breakdown"]
    assert "negative" in sol_stats["sentiment_breakdown"]
    assert "neutral" in sol_stats["sentiment_breakdown"]

    # Check that the counts add up to the total mentions
    total_from_breakdown = sum(s["count"] for s in sol_stats["sentiment_breakdown"].values())
    assert total_from_breakdown == sol_stats["total_mentions"]

    # Test with blockchain network filter
    usdc_solana_stats = get_token_sentiment_stats(
        db=db,
        token_symbol="USDC",
        blockchain_network="solana",
        days_back=7
    )

    assert usdc_solana_stats["token"] == "USDC (solana)"
    assert usdc_solana_stats["blockchain_network"] == "solana"
    assert usdc_solana_stats["total_mentions"] > 0

    # Test for USDC on Ethereum
    usdc_eth_stats = get_token_sentiment_stats(
        db=db,
        token_symbol="USDC",
        blockchain_network="ethereum",
        days_back=7
    )

    assert usdc_eth_stats["token"] == "USDC (ethereum)"
    assert usdc_eth_stats["blockchain_network"] == "ethereum"
    assert usdc_eth_stats["total_mentions"] > 0

    # Verify the stats are different for the same token on different networks
    assert usdc_solana_stats["total_mentions"] != usdc_eth_stats["total_mentions"]

    print("✓ Successfully retrieved token sentiment statistics")


def test_get_token_sentiment_timeline(db, test_data):
    """Test getting sentiment timeline for a token"""
    # Get sentiment timeline for SOL
    sol_timeline = get_token_sentiment_timeline(
        db=db,
        token_symbol="SOL",
        days_back=7,
        interval="day"
    )

    # Verify structure and data
    assert "token_info" in sol_timeline
    assert "timeline" in sol_timeline
    assert isinstance(sol_timeline["timeline"], list)
    assert len(sol_timeline["timeline"]) > 0

    for data_point in sol_timeline["timeline"]:
        assert "date" in data_point
        assert "total" in data_point
        assert "positive" in data_point
        assert "negative" in data_point
        assert "neutral" in data_point
        assert "positive_pct" in data_point
        assert "negative_pct" in data_point
        assert "neutral_pct" in data_point

        # Check that percentages are between 0 and 100
        assert 0 <= data_point["positive_pct"] <= 100
        assert 0 <= data_point["negative_pct"] <= 100
        assert 0 <= data_point["neutral_pct"] <= 100

        # Check that counts add up to total
        assert data_point["positive"] + data_point["negative"] + data_point["neutral"] == data_point["total"]

    # Test with blockchain network filter
    usdc_solana_timeline = get_token_sentiment_timeline(
        db=db,
        token_symbol="USDC",
        blockchain_network="solana",
        days_back=7,
        interval="day"
    )

    assert usdc_solana_timeline["token_info"]["symbol"] == "USDC"
    assert usdc_solana_timeline["token_info"]["blockchain_network"] == "solana"

    # Test for USDC on Ethereum
    usdc_eth_timeline = get_token_sentiment_timeline(
        db=db,
        token_symbol="USDC",
        blockchain_network="ethereum",
        days_back=7,
        interval="day"
    )

    assert usdc_eth_timeline["token_info"]["symbol"] == "USDC"
    assert usdc_eth_timeline["token_info"]["blockchain_network"] == "ethereum"

    print("✓ Successfully retrieved token sentiment timeline")


def test_compare_token_sentiments(db, test_data):
    """Test comparing sentiments between tokens"""
    # Compare SOL, USDC, and RAY
    comparison = compare_token_sentiments(
        db=db,
        token_symbols=["SOL", "USDC", "RAY"],
        days_back=7
    )

    # Verify structure and data
    assert "period" in comparison
    assert "tokens" in comparison
    assert "SOL" in comparison["tokens"]
    assert "USDC" in comparison["tokens"]
    assert "RAY" in comparison["tokens"]

    for symbol, data in comparison["tokens"].items():
        assert "total_mentions" in data
        assert data["total_mentions"] > 0
        assert "sentiment_score" in data
        assert -1 <= data["sentiment_score"] <= 1  # Score should be between -1 and 1
        assert "sentiments" in data
        assert "positive" in data["sentiments"]
        assert "negative" in data["sentiments"]
        assert "neutral" in data["sentiments"]

    # Test with blockchain network filters
    # Compare SOL, USDC on Solana, and USDC on Ethereum
    comparison_with_networks = compare_token_sentiments(
        db=db,
        token_symbols=["SOL", "USDC", "USDC"],
        blockchain_networks=["solana", "solana", "ethereum"],
        days_back=7
    )

    assert "SOL" in comparison_with_networks["tokens"]
    assert "USDC (solana)" in comparison_with_networks["tokens"]
    assert "USDC (ethereum)" in comparison_with_networks["tokens"]

    print("✓ Successfully compared token sentiments")


def test_get_most_discussed_tokens(db, test_data):
    """Test getting the most discussed tokens"""
    # Get most discussed tokens
    top_tokens = get_most_discussed_tokens(
        db=db,
        days_back=7,
        limit=10
    )

    # Verify structure and data
    assert "metadata" in top_tokens
    assert "tokens" in top_tokens
    assert isinstance(top_tokens["tokens"], list)
    assert len(top_tokens["tokens"]) > 0

    for token_data in top_tokens["tokens"]:
        assert "token_id" in token_data
        assert "symbol" in token_data
        assert "name" in token_data
        assert "blockchain_network" in token_data
        assert "mention_count" in token_data
        assert token_data["mention_count"] > 0
        assert "sentiment_score" in token_data
        assert -1 <= token_data["sentiment_score"] <= 1
        assert "sentiment_breakdown" in token_data

    # Verify that tokens are sorted by mention count (descending)
    for i in range(1, len(top_tokens["tokens"])):
        assert top_tokens["tokens"][i - 1]["mention_count"] >= top_tokens["tokens"][i]["mention_count"]

    # Test with blockchain network filter
    solana_tokens = get_most_discussed_tokens(
        db=db,
        days_back=7,
        limit=10,
        blockchain_network="solana"
    )

    assert solana_tokens["metadata"]["blockchain_network"] == "solana"
    assert all(token["blockchain_network"] == "solana" for token in solana_tokens["tokens"])

    print("✓ Successfully retrieved most discussed tokens")


def test_get_top_users_by_token(db, test_data):
    """Test getting top users for a token"""
    # Get top users discussing SOL
    top_users = get_top_users_by_token(
        db=db,
        token_symbol="SOL",
        days_back=7,
        limit=5
    )

    # Verify structure and data
    assert "token" in top_users
    assert top_users["token"] == "SOL"
    assert "period" in top_users
    assert "top_users" in top_users
    assert isinstance(top_users["top_users"], list)
    assert len(top_users["top_users"]) > 0

    for user_data in top_users["top_users"]:
        assert "author_id" in user_data
        assert "username" in user_data
        assert "tweet_count" in user_data
        assert user_data["tweet_count"] > 0
        assert "total_likes" in user_data
        assert "total_retweets" in user_data
        assert "engagement_rate" in user_data
        assert "influence_score" in user_data
        assert "sentiment_distribution" in user_data

    # Verify that users are sorted by tweet count (descending)
    for i in range(1, len(top_users["top_users"])):
        assert top_users["top_users"][i - 1]["tweet_count"] >= top_users["top_users"][i]["tweet_count"]

    # Test with blockchain network filter
    usdc_solana_users = get_top_users_by_token(
        db=db,
        token_symbol="USDC",
        blockchain_network="solana",
        days_back=7,
        limit=5
    )

    assert "USDC (solana)" in usdc_solana_users["token"]
    assert usdc_solana_users["blockchain_network"] == "solana"

    # Check users are different for different networks
    usdc_eth_users = get_top_users_by_token(
        db=db,
        token_symbol="USDC",
        blockchain_network="ethereum",
        days_back=7,
        limit=5
    )

    assert "USDC (ethereum)" in usdc_eth_users["token"]
    assert usdc_eth_users["blockchain_network"] == "ethereum"

    # User lists should be different
    solana_usernames = [user["username"] for user in usdc_solana_users["top_users"]]
    eth_usernames = [user["username"] for user in usdc_eth_users["top_users"]]
    assert set(solana_usernames) != set(eth_usernames)

    print("✓ Successfully retrieved top users by token")


def test_analyze_token_correlation(db, test_data):
    """Test analyzing token correlations"""
    # Analyze tokens correlated with SOL
    correlation_data = analyze_token_correlation(
        db=db,
        primary_token_symbol="SOL",
        days_back=7,
        min_co_mentions=1  # Lower threshold for test data
    )

    # Verify structure and data
    assert "primary_token" in correlation_data
    assert correlation_data["primary_token"]["symbol"] == "SOL"
    assert "total_mentions" in correlation_data["primary_token"]
    assert "period" in correlation_data
    assert "correlated_tokens" in correlation_data
    assert isinstance(correlation_data["correlated_tokens"], list)

    # We should find at least USDC and RAY
    found_usdc = False
    found_ray = False

    for token_data in correlation_data["correlated_tokens"]:
        assert "token_id" in token_data
        assert "symbol" in token_data
        assert "blockchain_network" in token_data
        assert "co_mention_count" in token_data
        assert token_data["co_mention_count"] > 0
        assert "correlation_percentage" in token_data
        assert 0 <= token_data["correlation_percentage"] <= 100
        assert "combined_sentiment" in token_data

        if token_data["symbol"] == "USDC":
            found_usdc = True
        elif token_data["symbol"] == "RAY":
            found_ray = True

    assert found_usdc, "USDC should be correlated with SOL in test data"
    assert found_ray, "RAY should be correlated with SOL in test data"

    # Test with blockchain network
    correlation_data_solana = analyze_token_correlation(
        db=db,
        primary_token_symbol="USDC",
        blockchain_network="solana",
        days_back=7,
        min_co_mentions=1
    )

    assert correlation_data_solana["primary_token"]["symbol"] == "USDC"
    assert correlation_data_solana["primary_token"]["blockchain_network"] == "solana"

    print("✓ Successfully analyzed token correlations")


def test_get_sentiment_momentum(db, test_data):
    """Test getting sentiment momentum for tokens"""
    # Get sentiment momentum for specific tokens
    momentum_data = get_sentiment_momentum(
        db=db,
        token_symbols=["SOL", "USDC", "RAY"],
        days_back=6,  # Use a wider range for test data
        min_mentions=1  # Lower threshold for test data
    )

    # Verify structure and data
    assert "period_1" in momentum_data
    assert "period_2" in momentum_data
    assert "tokens" in momentum_data
    assert isinstance(momentum_data["tokens"], dict)

    for symbol, token_data in momentum_data["tokens"].items():
        assert "period_1" in token_data
        assert "total_mentions" in token_data["period_1"]
        assert "sentiment_score" in token_data["period_1"]
        assert "sentiment_breakdown" in token_data["period_1"]

        assert "period_2" in token_data
        assert "total_mentions" in token_data["period_2"]
        assert "sentiment_score" in token_data["period_2"]
        assert "sentiment_breakdown" in token_data["period_2"]

        assert "momentum" in token_data
        assert isinstance(token_data["momentum"], float)
        assert "mention_growth_percentage" in token_data

    # Test with blockchain networks
    momentum_with_networks = get_sentiment_momentum(
        db=db,
        token_symbols=["USDC", "USDC"],
        blockchain_networks=["solana", "ethereum"],
        days_back=6,
        min_mentions=1
    )

    assert "USDC (solana)" in momentum_with_networks["tokens"]
    assert "USDC (ethereum)" in momentum_with_networks["tokens"]

    # Network-specific data should be different
    solana_data = momentum_with_networks["tokens"]["USDC (solana)"]
    eth_data = momentum_with_networks["tokens"]["USDC (ethereum)"]

    assert solana_data["momentum"] != eth_data["momentum"]

    print("✓ Successfully calculated sentiment momentum for tokens")


def test_invalid_input_handling(db):
    """Test handling of invalid inputs in core queries"""
    # Test handling of non-existent token
    with pytest.raises(ValueError):
        get_token_sentiment_stats(db=db, token_symbol="NON_EXISTENT")

    # Test handling of missing required parameters
    with pytest.raises(ValueError):
        get_token_sentiment_stats(db=db)

    # Test handling of invalid date range
    with pytest.raises(ValueError):
        get_token_sentiment_timeline(db=db, token_symbol="SOL", days_back=-1)

    # Test handling of invalid interval
    with pytest.raises(ValueError):
        get_token_sentiment_timeline(db=db, token_symbol="SOL", interval="invalid_interval")

    # Test handling of invalid blockchain network
    with pytest.raises(ValueError):
        get_token_sentiment_stats(db=db, token_symbol="USDC", blockchain_network="nonexistent_network")

    print("✓ Successfully handled invalid inputs")
