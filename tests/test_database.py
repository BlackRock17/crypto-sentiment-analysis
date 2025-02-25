# tests/test_database.py
from datetime import datetime
from src.data_processing.database import get_db
from src.data_processing.models.database import Tweet, SentimentAnalysis, SolanaToken, TokenMention, SentimentEnum


def test_database_connection():
    """Database connection test"""
    try:
        db = next(get_db())
        print("✓ Successful database connection!")
        assert db is not None, "Database connection failed"
    except Exception as e:
        print("✗ Error connecting to database:", str(e))
        assert False, f"Database connection failed: {str(e)}"


def add_test_data(db):
    """Add test data"""
    try:
        # 1. Create a test token
        solana_token = SolanaToken(
            token_address="So11111111111111111111111111111111111111112",
            symbol="SOL",
            name="Solana"
        )
        db.add(solana_token)
        db.commit()
        print("✓ Test token successfully added!")

        # 2. Create a test tweet
        tweet = Tweet(
            tweet_id="1234567890",
            text="Solana ($SOL) is performing great today! #solana",
            created_at=datetime.utcnow(),
            author_id="123456",
            author_username="crypto_expert"
        )
        db.add(tweet)
        db.commit()
        print("✓ Test tweet added successfully!")

        # 3. Add sentiment analysis
        sentiment = SentimentAnalysis(
            tweet_id=tweet.id,
            sentiment=SentimentEnum.POSITIVE,
            confidence_score=0.85
        )
        db.add(sentiment)
        db.commit()
        print("✓ Mood analysis successfully added!")

        # 4. Creating a link between the tweet and the token
        mention = TokenMention(
            tweet_id=tweet.id,
            token_id=solana_token.id
        )
        db.add(mention)
        db.commit()
        print("✓ Token mention successfully added!")

        return True

    except Exception as e:
        print("✗ Error adding test data:", str(e))
        return False


def verify_data(db):
    """Checking the added data"""
    try:
        # Check for the token
        token = db.query(SolanaToken).filter_by(symbol="SOL").first()
        print(f"Token found: {token.symbol} ({token.name})")

        # Tweet verification
        tweet = db.query(Tweet).first()
        print(f"Tweet found: {tweet.text}")

        # Analysis check
        sentiment = db.query(SentimentAnalysis).first()
        print(f"Analysis found: {sentiment.sentiment.value} with confidence {sentiment.confidence_score}")

        # Checking for connection
        mention = db.query(TokenMention).first()
        print(f"Found mention of token with ID: {mention.token_id}")

        return True

    except Exception as e:
        print("✗ Data validation error:", str(e))
        return False


if __name__ == "__main__":
    # Testing the connection
    db = test_database_connection()
    if db is None:
        exit(1)

    # Add test data
    print("\nAdding test data...")
    if add_test_data(db):
        print("\nChecking added data...")
        verify_data(db)

    print("\nTesting is over!")