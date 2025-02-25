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


def get_tweet_by_twitter_id(db: Session, twitter_id: str) -> Tweet:
    """
    Get a tweet by its Twitter ID

    Args:
        db: Database session
        twitter_id: The original Twitter ID

    Returns:
        Tweet instance or None if not found
    """
    return db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()


def get_tweets(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        author_username: str = None,
        sentiment: SentimentEnum = None,
        token_symbol: str = None,
        date_from: datetime = None,
        date_to: datetime = None
) -> list[Tweet]:
    """
    Get tweets with optional filtering and pagination

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        author_username: Filter by author username (partial match)
        sentiment: Filter by sentiment analysis result
        token_symbol: Filter by mentioned token symbol
        date_from: Filter tweets created after this date
        date_to: Filter tweets created before this date

    Returns:
        List of Tweet instances
    """
    query = db.query(Tweet).distinct()

    # Filter by author username
    if author_username:
        query = query.filter(Tweet.author_username.ilike(f"%{author_username}%"))

    # Filter by sentiment
    if sentiment:
        query = query.join(SentimentAnalysis).filter(SentimentAnalysis.sentiment == sentiment)

    # Filter by token mention
    if token_symbol:
        query = query.join(TokenMention).join(SolanaToken).filter(SolanaToken.symbol.ilike(f"%{token_symbol}%"))

    # Filter by date range
    if date_from:
        query = query.filter(Tweet.created_at >= date_from)

    if date_to:
        query = query.filter(Tweet.created_at <= date_to)

    # Apply pagination and return results
    return query.offset(skip).limit(limit).all()


def get_sentiment_analysis_by_id(db: Session, sentiment_id: int) -> SentimentAnalysis:
    """
    Get sentiment analysis by its ID

    Args:
        db: Database session
        sentiment_id: The ID of the sentiment analysis record

    Returns:
        SentimentAnalysis instance or None if not found
    """
    return db.query(SentimentAnalysis).filter(SentimentAnalysis.id == sentiment_id).first()


def get_sentiment_analysis_by_tweet_id(db: Session, tweet_id: int) -> SentimentAnalysis:
    """
    Get sentiment analysis for a specific tweet

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet

    Returns:
        SentimentAnalysis instance or None if not found
    """
    return db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).first()


def get_sentiment_analyses(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        sentiment: SentimentEnum = None,
        min_confidence: float = None,
        token_symbol: str = None
) -> list[SentimentAnalysis]:
    """
    Get sentiment analyses with optional filtering and pagination

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        sentiment: Filter by specific sentiment
        min_confidence: Filter by minimum confidence score
        token_symbol: Filter for tweets mentioning specific token

    Returns:
        List of SentimentAnalysis instances
    """
    query = db.query(SentimentAnalysis).distinct()

    # Filter by sentiment type
    if sentiment:
        query = query.filter(SentimentAnalysis.sentiment == sentiment)

    # Filter by minimum confidence score
    if min_confidence:
        query = query.filter(SentimentAnalysis.confidence_score >= min_confidence)

    # Filter by token mentions
    if token_symbol:
        query = query.join(Tweet).join(TokenMention).join(SolanaToken).filter(
            SolanaToken.symbol.ilike(f"%{token_symbol}%"))

    # Apply pagination and return results
    return query.offset(skip).limit(limit).all()


def get_token_mention_by_id(db: Session, mention_id: int) -> TokenMention:
    """
    Get token mention by its ID

    Args:
        db: Database session
        mention_id: The ID of the token mention record

    Returns:
        TokenMention instance or None if not found
    """
    return db.query(TokenMention).filter(TokenMention.id == mention_id).first()


def get_token_mentions_by_token_id(
        db: Session,
        token_id: int,
        skip: int = 0,
        limit: int = 100,
        date_from: datetime = None,
        date_to: datetime = None
) -> list[TokenMention]:
    """
    Get mentions of a specific token

    Args:
        db: Database session
        token_id: The ID of the token
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        date_from: Filter mentions after this date
        date_to: Filter mentions before this date

    Returns:
        List of TokenMention instances
    """
    query = db.query(TokenMention).filter(TokenMention.token_id == token_id)

    # Filter by date range
    if date_from:
        query = query.filter(TokenMention.mentioned_at >= date_from)

    if date_to:
        query = query.filter(TokenMention.mentioned_at <= date_to)

    # Apply pagination and return results
    return query.offset(skip).limit(limit).all()


def get_token_mentions_by_tweet_id(db: Session, tweet_id: int) -> list[TokenMention]:
    """
    Get all token mentions in a specific tweet

    Args:
        db: Database session
        tweet_id: The ID of the tweet

    Returns:
        List of TokenMention instances
    """
    return db.query(TokenMention).filter(TokenMention.tweet_id == tweet_id).all()