from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentAnalysis, TokenMention
from datetime import datetime


def delete_solana_token(db: Session, token_id: int, check_mentions: bool = True) -> bool:
    """
    Delete a Solana token record

    Args:
        db: Database session
        token_id: The ID of the token to delete
        check_mentions: If True, will check if token has mentions and prevent deletion

    Returns:
        True if deletion was successful, False if token not found or has mentions
    """
    # Get the token by ID
    db_token = db.query(SolanaToken).filter(SolanaToken.id == token_id).first()

    # Return False if token doesn't exist
    if db_token is None:
        return False

    # Check if the token has mentions
    if check_mentions:
        mentions_count = db.query(TokenMention).filter(TokenMention.token_id == token_id).count()
        if mentions_count > 0:
            raise ValueError(
                f"Cannot delete token with ID {token_id} as it has {mentions_count} mentions. Remove mentions first or use cascade delete.")

    # Delete the token
    db.delete(db_token)
    db.commit()

    return True


def delete_tweet(db: Session, tweet_id: int, cascade: bool = True) -> bool:
    """
    Delete a tweet record

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet to delete
        cascade: If True, will also delete associated sentiment analysis and token mentions

    Returns:
        True if deletion was successful, False if tweet not found
    """
    # Get the tweet by ID
    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    # Return False if tweet doesn't exist
    if db_tweet is None:
        return False

    # Handle cascade deletion
    if cascade:
        # Delete associated sentiment analysis
        db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).delete()

        # Delete associated token mentions
        db.query(TokenMention).filter(TokenMention.tweet_id == tweet_id).delete()
    else:
        # Check if tweet has associated records
        sentiment_count = db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).count()
        mentions_count = db.query(TokenMention).filter(TokenMention.tweet_id == tweet_id).count()

        if sentiment_count > 0 or mentions_count > 0:
            raise ValueError(
                f"Cannot delete tweet with ID {tweet_id} as it has {sentiment_count} sentiment analyses and {mentions_count} token mentions. Use cascade delete or remove associated records first.")

    # Delete the tweet
    db.delete(db_tweet)
    db.commit()

    return True