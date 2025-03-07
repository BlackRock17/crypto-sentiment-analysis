from typing import List

from sqlalchemy.orm import Session
from src.data_processing.models.database import BlockchainToken, BlockchainNetwork, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def create_blockchain_token(
        db: Session,
        token_address: str,
        symbol: str,
        name: str = None,
        blockchain_network: str = None,
        network_confidence: float = 0.0,
        manually_verified: bool = False,
        needs_review: bool = False,
        blockchain_network_id: int = None
) -> BlockchainToken:

    db_token = BlockchainToken(
        token_address=token_address,
        symbol=symbol,
        name=name,
        blockchain_network=blockchain_network,
        network_confidence=network_confidence,
        manually_verified=manually_verified,
        needs_review=needs_review,
        blockchain_network_id=blockchain_network_id,
        created_at=datetime.utcnow()
    )

    db.add(db_token)
    db.commit()
    db.refresh(db_token)

    return db_token


def create_blockchain_network(
        db: Session,
        name: str,
        display_name: str = None,
        description: str = None,
        hashtags: List[str] = None,
        keywords: List[str] = None,
        icon_url: str = None,
        is_active: bool = True,
        website_url: str = None,
        explorer_url: str = None,
        launch_date: datetime = None
) -> BlockchainNetwork:
    """
    Създава нов запис за блокчейн мрежа

    Args:
        db: Database session
        name: Уникално име на мрежата (напр. 'solana', 'ethereum')
        display_name: Име за показване (напр. 'Solana', 'Ethereum')
        description: Описание на мрежата
        hashtags: Списък със свързани хештагове
        keywords: Списък с ключови думи
        icon_url: URL към иконата на мрежата
        is_active: Дали мрежата е активна
        website_url: URL към официалния сайт
        explorer_url: URL към blockchain explorer
        launch_date: Дата на стартиране на мрежата

    Returns:
        Създаден BlockchainNetwork инстанс
    """
    db_network = BlockchainNetwork(
        name=name,
        display_name=display_name or name.capitalize(),
        description=description,
        hashtags=hashtags or [],
        keywords=keywords or [],
        icon_url=icon_url,
        is_active=is_active,
        website_url=website_url,
        explorer_url=explorer_url,
        launch_date=launch_date,
        created_at=datetime.utcnow()
    )

    db.add(db_network)
    db.commit()
    db.refresh(db_network)

    return db_network


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


def create_sentiment_analysis(
        db: Session,
        tweet_id: int,
        sentiment: SentimentEnum,
        confidence_score: float
) -> SentimentAnalysis:
    """
    Create a new sentiment analysis record

    Args:
        db: Database session
        tweet_id: ID на tweet-а от базата данни (не Twitter ID)
        sentiment: POSITIVE, NEGATIVE, или NEUTRAL
        confidence_score: Число между 0 и 1, показващо увереността в анализа

    Returns:
        Създаден SentimentAnalysis запис
    """
    # check if the score is a valid number between 0 and 1
    if not 0 <= confidence_score <= 1:
        raise ValueError("Confidence score must be between 0 and 1")

    db_sentiment = SentimentAnalysis(
        tweet_id=tweet_id,
        sentiment=sentiment,
        confidence_score=confidence_score
    )

    db.add(db_sentiment)
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def create_token_mention(
        db: Session,
        tweet_id: int,
        token_id: int
) -> TokenMention:
    """
    Creates a link between tweet and token

    Args:
        db: Database session
        tweet_id: ID of the tweet from the base
        token_id: ID of the token from the base

    Returns:
        TokenMention record created
    """
    db_mention = TokenMention(
        tweet_id=tweet_id,
        token_id=token_id
    )

    db.add(db_mention)
    db.commit()
    db.refresh(db_mention)

    return db_mention