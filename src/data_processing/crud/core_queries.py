from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from src.data_processing.models.database import SolanaToken, Tweet, SentimentAnalysis, TokenMention, SentimentEnum


def get_token_sentiment_stats(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        days_back: int = 7
) -> Dict:
    """
    Retrieves sentiment statistics for a specific token

    Args:
        db: Database session
        token_symbol: Token symbol (eg 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        days_back: Number of days back to analyze

    Returns:
        Dictionary of sentiment statistics
    """
    if not token_symbol and not token_id:
        raise ValueError("token_symbol or token_id must be specified")

    # Calculate the end date for the period
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Creating the basic query with joins
    query = (
        db.query(
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        )
        .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Token filtering
    if token_symbol:
        query = query.filter(SolanaToken.symbol == token_symbol)
    else:
        query = query.filter(SolanaToken.id == token_id)

    # Grouping by mood and sorting by number
    query = query.group_by(SentimentAnalysis.sentiment).order_by(desc("count"))

    # Request execution
    sentiment_stats = query.all()

    # Calculating the total number
    total_mentions = sum(stat.count for stat in sentiment_stats)

    # Formatting the result
    result = {
        "token": token_symbol,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "total_mentions": total_mentions,
        "sentiment_breakdown": {
            sentiment.value: {
                "count": count,
                "percentage": round((count / total_mentions) * 100, 2) if total_mentions > 0 else 0,
                "avg_confidence": round(avg_confidence, 2)
            }
            for sentiment, count, avg_confidence in sentiment_stats
        }
    }

    return result