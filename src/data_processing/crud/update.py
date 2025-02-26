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