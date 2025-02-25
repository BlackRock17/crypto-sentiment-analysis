from sqlalchemy.orm import Session
from src.data_processing.models.database import SolanaToken, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
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