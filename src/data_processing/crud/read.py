from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def get_solana_token_by_id(db: Session, token_id: int) -> SolanaToken:
    """
    Get a Solana token by its database ID

    Args:
        db: Database session
        token_id: The internal database ID of the token

    Returns:
        SolanaToken instance or None if not found
    """
    return db.query(SolanaToken).filter(SolanaToken.id == token_id).first()


def get_solana_token_by_address(db: Session, token_address: str) -> SolanaToken:
    """
    Get a Solana token by its blockchain address

    Args:
        db: Database session
        token_address: The token's address on Solana blockchain

    Returns:
        SolanaToken instance or None if not found
    """
    return db.query(SolanaToken).filter(SolanaToken.token_address == token_address).first()


def get_solana_token_by_symbol(db: Session, symbol: str) -> SolanaToken:
    """
    Get a Solana token by its symbol

    Args:
        db: Database session
        symbol: Token symbol (e.g. 'SOL')

    Returns:
        SolanaToken instance or None if not found
    """
    return db.query(SolanaToken).filter(SolanaToken.symbol == symbol).first()


def get_all_solana_tokens(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        symbol_filter: str = None,
        name_filter: str = None
) -> list[SolanaToken]:
    """
    Get all Solana tokens with optional filtering and pagination

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        symbol_filter: Filter tokens by symbol (case-insensitive partial match)
        name_filter: Filter tokens by name (case-insensitive partial match)

    Returns:
        List of SolanaToken instances
    """
    query = db.query(SolanaToken)

    # Apply filters if provided
    if symbol_filter:
        query = query.filter(SolanaToken.symbol.ilike(f"%{symbol_filter}%"))

    if name_filter:
        query = query.filter(SolanaToken.name.ilike(f"%{name_filter}%"))

    # Apply pagination and return results
    return query.offset(skip).limit(limit).all()


def get_tweet_by_id(db: Session, tweet_id: int) -> Tweet:
    """
    Get a tweet by its database ID

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet

    Returns:
        Tweet instance or None if not found
    """
    return db.query(Tweet).filter(Tweet.id == tweet_id).first()