from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def update_solana_token(
        db: Session,
        token_id: int,
        symbol: str = None,
        name: str = None
) -> SolanaToken:
    """
    Update a Solana token record

    Args:
        db: Database session
        token_id: The ID of the token to update
        symbol: New token symbol (optional)
        name: New token name (optional)

    Returns:
        Updated SolanaToken instance or None if not found
    """
    # Get the token by ID
    db_token = db.query(SolanaToken).filter(SolanaToken.id == token_id).first()

    # Return None if token doesn't exist
    if db_token is None:
        return None

    # Update fields if provided
    if symbol is not None:
        db_token.symbol = symbol

    if name is not None:
        db_token.name = name

    # Commit changes to the database
    db.commit()
    db.refresh(db_token)

    return db_token


def update_tweet(
        db: Session,
        tweet_id: int,
        text: str = None,
        author_username: str = None,
        retweet_count: int = None,
        like_count: int = None
) -> Tweet:
    """
    Update a tweet record

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet to update
        text: New text content of the tweet (optional)
        author_username: New author username (optional)
        retweet_count: New retweet count (optional)
        like_count: New like count (optional)

    Returns:
        Updated Tweet instance or None if not found
    """
    # Get the tweet by ID
    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    # Return None if tweet doesn't exist
    if db_tweet is None:
        return None

    # Update fields if provided
    if text is not None:
        db_tweet.text = text

    if author_username is not None:
        db_tweet.author_username = author_username

    if retweet_count is not None:
        db_tweet.retweet_count = retweet_count

    if like_count is not None:
        db_tweet.like_count = like_count

    # Commit changes to the database
    db.commit()
    db.refresh(db_tweet)

    return db_tweet


def update_sentiment_analysis(
        db: Session,
        sentiment_id: int,
        sentiment: SentimentEnum = None,
        confidence_score: float = None
) -> SentimentAnalysis:
    """
    Update a sentiment analysis record

    Args:
        db: Database session
        sentiment_id: The ID of the sentiment analysis record to update
        sentiment: New sentiment value (optional)
        confidence_score: New confidence score (optional)

    Returns:
        Updated SentimentAnalysis instance or None if not found
    """
    # Get the sentiment analysis by ID
    db_sentiment = db.query(SentimentAnalysis).filter(SentimentAnalysis.id == sentiment_id).first()

    # Return None if record doesn't exist
    if db_sentiment is None:
        return None

    # Update sentiment if provided
    if sentiment is not None:
        db_sentiment.sentiment = sentiment

    # Update confidence score if provided
    if confidence_score is not None:
        # Validate that confidence score is between 0 and 1
        if not 0 <= confidence_score <= 1:
            raise ValueError("Confidence score must be between 0 and 1")
        db_sentiment.confidence_score = confidence_score

    # Update the analyzed_at timestamp to current time
    db_sentiment.analyzed_at = datetime.utcnow()

    # Commit changes to the database
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment