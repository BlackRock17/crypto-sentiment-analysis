from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet
from datetime import datetime


def create_solana_token(db: Session, token_address: str, symbol: str, name: str = None) -> SolanaToken:
    """
    Create a new Solana token record

    Args:
        db: Database session
        token_address: The token's address on Solana blockchain
        symbol: Token symbol (e.g. 'SOL')
        name: Optional full name of the token

    Returns:
        Created SolanaToken instance
    """
    db_token = SolanaToken(
        token_address=token_address,
        symbol=symbol,
        name=name,
        created_at=datetime.utcnow()
    )

    db.add(db_token)
    db.commit()
    db.refresh(db_token)

    return db_token


def create_tweet(
        db: Session,
        tweet_id: str,
        text: str,
        created_at: datetime,
        author_id: str,
        author_username: str = None,
        retweet_count: int = 0,
        like_count: int = 0
) -> Tweet:
    """
    Create a new tweet record
    """
    db_tweet = Tweet(
        tweet_id=tweet_id,
        text=text,
        created_at=created_at,
        author_id=author_id,
        author_username=author_username,
        retweet_count=retweet_count,
        like_count=like_count
    )

    db.add(db_tweet)
    db.commit()
    db.refresh(db_tweet)

    return db_tweet