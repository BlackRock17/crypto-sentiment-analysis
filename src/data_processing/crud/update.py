from typing import Optional
from datetime import datetime
from sqlalchemy.orm import Session

from src.data_processing.models.database import (
    Tweet, Token, Network, MarketSentiment,
    TokenSentiment, NetworkSentiment, SentimentEnum, Influencer
)


def update_tweet(
        db: Session,
        tweet_id: int,
        text: Optional[str] = None,
        created_at: Optional[datetime] = None
) -> Optional[Tweet]:

    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    if db_tweet is None:
        return None

    if text is not None:
        db_tweet.text = text

    if created_at is not None:
        db_tweet.created_at = created_at

    db.commit()
    db.refresh(db_tweet)

    return db_tweet


def update_tweet_by_twitter_id(
        db: Session,
        twitter_id: str,
        text: Optional[str] = None,
        created_at: Optional[datetime] = None
) -> Optional[Tweet]:

    db_tweet = db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()

    if db_tweet is None:
        return None

    return update_tweet(
        db=db,
        tweet_id=db_tweet.id,
        text=text,
        created_at=created_at
    )


def update_token(
        db: Session,
        token_id: int,
        symbol: Optional[str] = None,
        name: Optional[str] = None,
        blockchain_network: Optional[str] = None
) -> Optional[Token]:

    db_token = db.query(Token).filter(Token.id == token_id).first()

    if db_token is None:
        return None

    if symbol is not None:
        db_token.symbol = symbol

    if name is not None:
        db_token.name = name

    if blockchain_network is not None:
        db_token.blockchain_network = blockchain_network

    db.commit()
    db.refresh(db_token)

    return db_token


def update_network(
        db: Session,
        network_id: int,
        name: Optional[str] = None
) -> Optional[Network]:

    db_network = db.query(Network).filter(Network.id == network_id).first()

    if db_network is None:
        return None

    if name is not None:
        # Check if a name is already in use by another network
        existing = db.query(Network).filter(
            Network.name == name,
            Network.id != network_id
        ).first()

        if existing:
            raise ValueError(f"A network named '{name}' already exists.")

        db_network.name = name

    db.commit()
    db.refresh(db_network)

    return db_network


def update_market_sentiment(
        db: Session,
        sentiment_id: int,
        sentiment: Optional[SentimentEnum] = None,
        confidence_score: Optional[float] = None
) -> Optional[MarketSentiment]:

    db_sentiment = db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return None

    if sentiment is not None:
        db_sentiment.sentiment = sentiment

    if confidence_score is not None:
        # Checking if confidence_score is between 0 and 1
        if not 0 <= confidence_score <= 1:
            raise ValueError("Confidence score must be between 0 and 1")

        db_sentiment.confidence_score = confidence_score

    db_sentiment.analyzed_at = datetime.utcnow()

    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def update_token_sentiment(
        db: Session,
        sentiment_id: int,
        token_id: Optional[int] = None,
        tweet_id: Optional[int] = None,
        sentiment: Optional[SentimentEnum] = None,
        confidence_score: Optional[float] = None
) -> Optional[TokenSentiment]:

    db_sentiment = db.query(TokenSentiment).filter(TokenSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return None

    if token_id is not None:
        # Checking if the token exists
        token_exists = db.query(Token).filter(Token.id == token_id).first() is not None
        if not token_exists:
            raise ValueError(f"Token with ID {token_id} does not exist")

        db_sentiment.token_id = token_id

    if tweet_id is not None:
        # Checking if the tweet exists
        tweet_exists = db.query(Tweet).filter(Tweet.id == tweet_id).first() is not None
        if not tweet_exists:
            raise ValueError(f"Tweet with ID {tweet_id} does not exist.")

        db_sentiment.tweet_id = tweet_id

    if sentiment is not None:
        db_sentiment.sentiment = sentiment

    if confidence_score is not None:
        if not 0 <= confidence_score <= 1:
            raise ValueError("Confidence score must be between 0 and 1")

        db_sentiment.confidence_score = confidence_score

    db_sentiment.analyzed_at = datetime.utcnow()

    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def update_network_sentiment(
        db: Session,
        sentiment_id: int,
        network_id: Optional[int] = None,
        tweet_id: Optional[int] = None,
        sentiment: Optional[SentimentEnum] = None,
        confidence_score: Optional[float] = None
) -> Optional[NetworkSentiment]:

    db_sentiment = db.query(NetworkSentiment).filter(NetworkSentiment.id == sentiment_id).first()

    if db_sentiment is None:
        return None

    if network_id is not None:
        # Checking if the network exists
        network_exists = db.query(Network).filter(Network.id == network_id).first() is not None
        if not network_exists:
            raise ValueError(f"Network with ID {network_id} does not exist")

        db_sentiment.network_id = network_id

    if tweet_id is not None:
        # Checking if the tweet exists
        tweet_exists = db.query(Tweet).filter(Tweet.id == tweet_id).first() is not None
        if not tweet_exists:
            raise ValueError(f"Tweet with ID {tweet_id} does not exist.")

        db_sentiment.tweet_id = tweet_id

    if sentiment is not None:
        db_sentiment.sentiment = sentiment

    if confidence_score is not None:
        if not 0 <= confidence_score <= 1:
            raise ValueError("Confidence score must be between 0 and 1")

        db_sentiment.confidence_score = confidence_score

    db_sentiment.analyzed_at = datetime.utcnow()

    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def update_influencer(
        db: Session,
        influencer_id: int,
        username: Optional[str] = None,
        is_active: Optional[bool] = None
) -> Optional[Influencer]:

    db_influencer = db.query(Influencer).filter(Influencer.id == influencer_id).first()

    if db_influencer is None:
        return None

    if username is not None:
        # Check if the username is already used by another influencer
        existing = db.query(Influencer).filter(
            Influencer.username == username,
            Influencer.id != influencer_id
        ).first()

        if existing:
            raise ValueError(f"Influencer with username '{username}' already exists")

        db_influencer.username = username

    if is_active is not None:
        db_influencer.is_active = is_active

    db.commit()
    db.refresh(db_influencer)

    return db_influencer
