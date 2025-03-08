from typing import Type, List, Tuple

from sqlalchemy import func, or_, and_, desc
from sqlalchemy.orm import Session

from src.data_processing.models import BlockchainToken
from src.data_processing.models.database import BlockchainToken, BlockchainNetwork, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def get_blockchain_token_by_id(db: Session, token_id: int) -> BlockchainToken:
    """
    Gets a blockchain token by its database ID

    Args:
        db: Database session
        token_id: The internal ID of the token in the database

    Returns:
        BlockchainToken instance or None if not found
    """
    return db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()


def get_blockchain_token_by_address(db: Session, token_address: str, blockchain_network: str = None) -> BlockchainToken:
    """
    Gets a blockchain token by its address and optionally network

    Args:
        db: Database session
        token_address: The address of the token in the blockchain network
        blockchain_network: Optional blockchain network name

    Returns:
        BlockchainToken instance or None if not found
    """
    query = db.query(BlockchainToken).filter(BlockchainToken.token_address == token_address)

    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    return query.first()


def get_blockchain_token_by_symbol(db: Session, symbol: str, blockchain_network: str = None) -> BlockchainToken:
    """
    Gets a blockchain token by its symbol and optionally network

    Args:
        db: Database session
        symbol: Token symbol (e.g. 'SOL')
        blockchain_network: Optional blockchain network name

    Returns:
        BlockchainToken instance or None if not found
    """
    query = db.query(BlockchainToken).filter(BlockchainToken.symbol == symbol)

    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    return query.first()


def get_blockchain_token_by_symbol_and_network(db: Session, symbol: str, blockchain_network_id: int) \
                                              -> (Type[BlockchainToken] | None):
    """
    Gets a blockchain token by its symbol and network ID

    Args:
        db: Database session
        symbol: Token symbol (e.g. 'SOL')
        blockchain_network_id: Blockchain network ID

    Returns:
        BlockchainToken instance or None if not found
    """
    return (db.query(BlockchainToken)
            .filter(BlockchainToken.symbol == symbol)
            .filter(BlockchainToken.blockchain_network_id == blockchain_network_id)
            .first())


def get_all_blockchain_tokens(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        symbol_filter: str = None,
        name_filter: str = None,
        blockchain_network: str = None,
        blockchain_network_id: int = None,
        needs_review: bool = None,
        manually_verified: bool = None
) -> list[Type[BlockchainToken]]:
    """
    Gets all blockchain tokens with optional filtering and pagination

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        symbol_filter: Filter by symbol (case insensitive)
        name_filter: Filter by name (case insensitive)
        blockchain_network: Filter by blockchain network name
        blockchain_network_id: Filter by blockchain network ID
        needs_review: Filter by needs-review flag
        manually_verified: Filter by manual verification flag

    Returns:
        List of BlockchainToken instances
    """
    query = db.query(BlockchainToken)

    # Apply filters if provided
    if symbol_filter:
        query = query.filter(BlockchainToken.symbol.ilike(f"%{symbol_filter}%"))

    if name_filter:
        query = query.filter(BlockchainToken.name.ilike(f"%{name_filter}%"))

    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    if blockchain_network_id:
        query = query.filter(BlockchainToken.blockchain_network_id == blockchain_network_id)

    if needs_review is not None:
        query = query.filter(BlockchainToken.needs_review == needs_review)

    if manually_verified is not None:
        query = query.filter(BlockchainToken.manually_verified == manually_verified)

    # Implementing pagination and returning results
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
        query = query.join(TokenMention).join(BlockchainToken).filter(BlockchainToken.symbol.ilike(f"%{token_symbol}%"))

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
        query = query.join(Tweet).join(TokenMention).join(BlockchainToken).filter(
            BlockchainToken.symbol.ilike(f"%{token_symbol}%"))

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


def get_tokens_needing_review(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        min_confidence: float = None,
        max_confidence: float = None
) -> List[BlockchainToken]:
    """
    Retrieves tokens that need to be reviewed

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        min_confidence: Minimum confidence level
        max_confidence: Maximum confidence level

    Returns:
        List of BlockchainToken instances
    """
    query = db.query(BlockchainToken).filter(BlockchainToken.needs_review == True)

    if min_confidence is not None:
        query = query.filter(BlockchainToken.network_confidence >= min_confidence)

    if max_confidence is not None:
        query = query.filter(BlockchainToken.network_confidence <= max_confidence)

    return query.offset(skip).limit(limit).all()


# Functions for working with BlockchainNetwork

def get_blockchain_network_by_id(db: Session, network_id: int) -> BlockchainNetwork:
    """
    Gets a blockchain network by its ID

    Args:
        db: Database session
        network_id: Network ID

    Returns:
        BlockchainNetwork instance or None if not found
    """
    return db.query(BlockchainNetwork).filter(BlockchainNetwork.id == network_id).first()


def get_blockchain_network_by_name(db: Session, name: str) -> BlockchainNetwork:
    """
    Gets a blockchain network by its name

    Args:
        db: Database session
        name: Network name (e.g., 'solana', 'ethereum')

    Returns:
        BlockchainNetwork instance or None if not found
    """
    return db.query(BlockchainNetwork).filter(BlockchainNetwork.name == name).first()


def get_all_blockchain_networks(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False
) -> List[BlockchainNetwork]:
    """
    Gets a list of blockchain networks

    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        active_only: Only active networks if True

    Returns:
        List of BlockchainNetwork instances
    """
    query = db.query(BlockchainNetwork)

    if active_only:
        query = query.filter(BlockchainNetwork.is_active == True)

    return query.offset(skip).limit(limit).all()


def get_tokens_needing_categorization(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        min_mentions: int = 1
) -> List[Tuple[BlockchainToken, int]]:
    """
    Get tokens that need blockchain network categorization, along with their mention counts.

    Args:
        db: Database session
        skip: Number of records to skip
        limit: Maximum number of records to return
        min_mentions: Minimum number of mentions to include

    Returns:
        List of tuples (BlockchainToken, mention_count)
    """
    query = db.query(
        BlockchainToken,
        func.count(TokenMention.id).label("mention_count")
    ).outerjoin(
        TokenMention, TokenMention.token_id == BlockchainToken.id
    ).filter(
        or_(
            BlockchainToken.blockchain_network == None,  # No network assigned
            and_(
                BlockchainToken.network_confidence < 0.7,  # Low confidence score
                BlockchainToken.manually_verified == False  # Not manually verified
            ),
            BlockchainToken.needs_review == True  # Explicitly flagged for review
        )
    ).group_by(
        BlockchainToken.id
    ).having(
        func.count(TokenMention.id) >= min_mentions
    ).order_by(
        desc("mention_count")
    ).offset(skip).limit(limit)

    return query.all()
