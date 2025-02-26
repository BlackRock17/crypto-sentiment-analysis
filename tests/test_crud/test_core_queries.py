import pytest
from datetime import datetime, timedelta
from src.data_processing.database import get_db
from src.data_processing.crud.create import (
    create_solana_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
)
from src.data_processing.crud.core_queries import (
    get_token_sentiment_stats,
    get_token_sentiment_timeline,
    compare_token_sentiments,
    get_most_discussed_tokens,
    get_top_users_by_token
)
from src.data_processing.models.database import SentimentEnum


@pytest.fixture
def test_data(db):
    """Create test data for core queries testing"""
    # Create tokens
    sol_token = create_solana_token(
        db=db,
        token_address="So11111111111111111111111111111111111111112",
        symbol="SOL",
        name="Solana"
    )

    usdc_token = create_solana_token(
        db=db,
        token_address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol="USDC",
        name="USD Coin"
    )

    ray_token = create_solana_token(
        db=db,
        token_address="4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        symbol="RAY",
        name="Raydium"
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

    # Create tweet data for USDC
    create_tweet_batch(4, 1, SentimentEnum.POSITIVE, usdc_token, 0.80, "stablecoin_user")
    create_tweet_batch(2, 2, SentimentEnum.NEUTRAL, usdc_token, 0.65, "investor")

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
        "tokens": {
            "sol": sol_token,
            "usdc": usdc_token,
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
    db.delete(ray_token)

    db.commit()