from typing import List, Optional
from datetime import datetime
from sqlalchemy import func, or_, and_, desc
from sqlalchemy.orm import Session

from src.data_processing.models.database import (
    Tweet, Token, Network, MarketSentiment,
    TokenSentiment, NetworkSentiment, SentimentEnum, Influencer
)


def get_tweet_by_id(db: Session, tweet_id: int) -> Optional[Tweet]:

    return db.query(Tweet).filter(Tweet.id == tweet_id).first()


def get_tweet_by_twitter_id(db: Session, twitter_id: str) -> Optional[Tweet]:

    return db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()


def get_tweets(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
) -> List[Tweet]:
    """
    Get a list of tweets with filtering and pagination capabilities.
    """
    query = db.query(Tweet)

    # Applying filters by date
    if date_from:
        query = query.filter(Tweet.created_at >= date_from)

    if date_to:
        query = query.filter(Tweet.created_at <= date_to)

    # Applying pagination and returning results
    return query.order_by(desc(Tweet.created_at)).offset(skip).limit(limit).all()


def get_token_by_id(db: Session, token_id: int) -> Optional[Token]:

    return db.query(Token).filter(Token.id == token_id).first()


def get_token_by_symbol(db: Session, symbol: str) -> Optional[Token]:

    return db.query(Token).filter(Token.symbol == symbol).first()


def get_all_tokens(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        blockchain_network: Optional[str] = None
) -> List[Token]:

    query = db.query(Token)

    if blockchain_network:
        query = query.filter(Token.blockchain_network == blockchain_network)

    return query.offset(skip).limit(limit).all()


def get_network_by_id(db: Session, network_id: int) -> Optional[Network]:

    return db.query(Network).filter(Network.id == network_id).first()


def get_network_by_name(db: Session, name: str) -> Optional[Network]:

    return db.query(Network).filter(Network.name == name).first()


def get_all_networks(
        db: Session,
        skip: int = 0,
        limit: int = 100
) -> List[Network]:

    return db.query(Network).offset(skip).limit(limit).all()


def get_market_sentiment_by_id(db: Session, sentiment_id: int) -> Optional[MarketSentiment]:

    return db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()


def get_market_sentiment_by_tweet_id(db: Session, tweet_id: int) -> Optional[MarketSentiment]:

    return db.query(MarketSentiment).filter(MarketSentiment.tweet_id == tweet_id).first()


def get_market_sentiments(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        sentiment: Optional[SentimentEnum] = None,
        min_confidence: Optional[float] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
) -> List[MarketSentiment]:
    """
    Args:
        db: Database session
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
        sentiment: Filter by sentiment type
        min_confidence: Filter by minimum confidence
        date_from: Filter for sentiments after this date
        date_to: Filter for sentiments before this date
    """
    query = db.query(MarketSentiment)

    if sentiment:
        query = query.filter(MarketSentiment.sentiment == sentiment)

    if min_confidence is not None:
        query = query.filter(MarketSentiment.confidence_score >= min_confidence)

    if date_from:
        query = query.filter(MarketSentiment.analyzed_at >= date_from)

    if date_to:
        query = query.filter(MarketSentiment.analyzed_at <= date_to)

    return query.order_by(desc(MarketSentiment.analyzed_at)).offset(skip).limit(limit).all()


def get_token_sentiment_by_id(db: Session, sentiment_id: int) -> Optional[TokenSentiment]:

    return db.query(TokenSentiment).filter(TokenSentiment.id == sentiment_id).first()


def get_token_sentiments_by_token_id(
        db: Session,
        token_id: int,
        skip: int = 0,
        limit: int = 100,
        sentiment: Optional[SentimentEnum] = None
) -> List[TokenSentiment]:

    query = db.query(TokenSentiment).filter(TokenSentiment.token_id == token_id)

    if sentiment:
        query = query.filter(TokenSentiment.sentiment == sentiment)

    return query.order_by(desc(TokenSentiment.analyzed_at)).offset(skip).limit(limit).all()


def get_token_sentiments_by_tweet_id(db: Session, tweet_id: int) -> List[TokenSentiment]:

    return db.query(TokenSentiment).filter(TokenSentiment.tweet_id == tweet_id).all()


def get_network_sentiment_by_id(db: Session, sentiment_id: int) -> Optional[NetworkSentiment]:

    return db.query(NetworkSentiment).filter(NetworkSentiment.id == sentiment_id).first()


def get_network_sentiments_by_network_id(
        db: Session,
        network_id: int,
        skip: int = 0,
        limit: int = 100,
        sentiment: Optional[SentimentEnum] = None
) -> List[NetworkSentiment]:

    query = db.query(NetworkSentiment).filter(NetworkSentiment.network_id == network_id)

    if sentiment:
        query = query.filter(NetworkSentiment.sentiment == sentiment)

    return query.order_by(desc(NetworkSentiment.analyzed_at)).offset(skip).limit(limit).all()


def get_influencer_by_id(db: Session, influencer_id: int) -> Optional[Influencer]:

    return db.query(Influencer).filter(Influencer.id == influencer_id).first()


def get_influencer_by_username(db: Session, username: str) -> Optional[Influencer]:

    return db.query(Influencer).filter(Influencer.username == username).first()


def get_all_influencers(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        is_active: Optional[bool] = None
) -> List[Influencer]:

    query = db.query(Influencer)

    if is_active is not None:
        query = query.filter(Influencer.is_active == is_active)

    return query.offset(skip).limit(limit).all()
