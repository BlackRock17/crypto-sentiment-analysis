from sqlalchemy.orm import Session
from datetime import datetime
from src.data_processing.models.database import Tweet, Token, Network, MarketSentiment, TokenSentiment, \
    NetworkSentiment, SentimentEnum, Influencer


def create_tweet(
        db: Session,
        tweet_id: str,
        text: str,
        created_at: datetime,
) -> Tweet:

    db_tweet = Tweet(
        tweet_id=tweet_id,
        text=text,
        created_at=created_at,
        collected_at=datetime.utcnow()
    )

    db.add(db_tweet)
    db.commit()
    db.refresh(db_tweet)

    return db_tweet


def create_token(
        db: Session,
        symbol: str,
        name: str = None,
        blockchain_network: str = None
) -> Token:

    db_token = Token(
        symbol=symbol,
        name=name,
        blockchain_network=blockchain_network,
        created_at=datetime.utcnow()
    )

    db.add(db_token)
    db.commit()
    db.refresh(db_token)

    return db_token


def create_network(
        db: Session,
        name: str
) -> Network:

    db_network = Network(
        name=name,
        created_at=datetime.utcnow()
    )

    db.add(db_network)
    db.commit()
    db.refresh(db_network)

    return db_network


def create_market_sentiment(
        db: Session,
        tweet_id: int,
        sentiment: SentimentEnum,
        confidence_score: float
) -> MarketSentiment:

    if not 0 <= confidence_score <= 1:
        raise ValueError("Confidence score must be between 0 and 1")

    db_sentiment = MarketSentiment(
        tweet_id=tweet_id,
        sentiment=sentiment,
        confidence_score=confidence_score,
        analyzed_at=datetime.utcnow()
    )

    db.add(db_sentiment)
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def create_token_sentiment(
        db: Session,
        token_id: int,
        tweet_id: int,
        sentiment: SentimentEnum,
        confidence_score: float
) -> TokenSentiment:

    if not 0 <= confidence_score <= 1:
        raise ValueError("Confidence score must be between 0 and 1")

    db_sentiment = TokenSentiment(
        token_id=token_id,
        tweet_id=tweet_id,
        sentiment=sentiment,
        confidence_score=confidence_score,
        analyzed_at=datetime.utcnow()
    )

    db.add(db_sentiment)
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def create_network_sentiment(
        db: Session,
        network_id: int,
        tweet_id: int,
        sentiment: SentimentEnum,
        confidence_score: float
) -> NetworkSentiment:

    if not 0 <= confidence_score <= 1:
        raise ValueError("Confidence score must be between 0 and 1")

    db_sentiment = NetworkSentiment(
        network_id=network_id,
        tweet_id=tweet_id,
        sentiment=sentiment,
        confidence_score=confidence_score,
        analyzed_at=datetime.utcnow()
    )

    db.add(db_sentiment)
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def create_influencer(
        db: Session,
        username: str,
        is_active: bool = True
) -> Influencer:

    db_influencer = Influencer(
        username=username,
        is_active=is_active,
        created_at=datetime.utcnow()
    )

    db.add(db_influencer)
    db.commit()
    db.refresh(db_influencer)

    return db_influencer
