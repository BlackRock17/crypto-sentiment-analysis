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


def get_token_sentiment_timeline(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        days_back: int = 30,
        interval: str = "day"
) -> List[Dict]:
    """
    Get sentiment trend for a specific token over time

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        days_back: Number of days to look back
        interval: Time grouping interval ('day', 'week', 'hour')

    Returns:
        List of sentiment data points over time
    """
    if not token_symbol and not token_id:
        raise ValueError("Must provide either token_symbol or token_id")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Create base query with joins
    query = (
        db.query(
            func.date_trunc(interval, Tweet.created_at).label('time_bucket'),
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        )
        .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by token
    if token_symbol:
        query = query.filter(SolanaToken.symbol == token_symbol)
    else:
        query = query.filter(SolanaToken.id == token_id)

    # Group by time bucket and sentiment
    query = query.group_by('time_bucket', SentimentAnalysis.sentiment)

    # Order by time
    query = query.order_by('time_bucket')

    # Execute query
    sentiment_timeline = query.all()

    # Process results into timeline format
    timeline_data = []
    current_bucket = None
    current_data = None

    for bucket, sentiment, count, avg_confidence in sentiment_timeline:
        # If we're on a new time bucket, create a new data point
        if current_bucket != bucket:
            if current_data:
                timeline_data.append(current_data)

            current_bucket = bucket
            current_data = {
                "date": bucket.strftime('%Y-%m-%d') if interval == 'day' else (
                    bucket.strftime('%Y-%m-%d %H:00') if interval == 'hour' else
                    f"Week of {bucket.strftime('%Y-%m-%d')}"
                ),
                "total": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "positive_pct": 0,
                "negative_pct": 0,
                "neutral_pct": 0,
            }

        # Add sentiment counts to the current bucket
        current_data["total"] += count
        sentiment_key = sentiment.value.lower()
        current_data[sentiment_key] = count

    # Add the last data point if it exists
    if current_data:
        timeline_data.append(current_data)

    # Calculate percentages
    for data_point in timeline_data:
        if data_point["total"] > 0:
            data_point["positive_pct"] = round((data_point["positive"] / data_point["total"]) * 100, 2)
            data_point["negative_pct"] = round((data_point["negative"] / data_point["total"]) * 100, 2)
            data_point["neutral_pct"] = round((data_point["neutral"] / data_point["total"]) * 100, 2)

    return timeline_data


def compare_token_sentiments(
        db: Session,
        token_symbols: List[str] = None,
        token_ids: List[int] = None,
        days_back: int = 7
) -> Dict:
    """
    Compare sentiment analysis between multiple tokens

    Args:
        db: Database session
        token_symbols: List of token symbols (e.g. ['SOL', 'USDC'])
        token_ids: List of token IDs (alternative to symbols)
        days_back: Number of days to look back

    Returns:
        Dictionary with comparative sentiment data for each token
    """
    if not token_symbols and not token_ids:
        raise ValueError("Must provide either token_symbols or token_ids")

    if token_symbols and token_ids and len(token_symbols) != len(token_ids):
        raise ValueError("If both token_symbols and token_ids are provided, they must be the same length")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Container for results
    results = {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "tokens": {}
    }

    # If token_symbols is provided, use it, otherwise query token symbols
    if token_symbols:
        tokens_to_query = token_symbols
        token_filter = SolanaToken.symbol.in_(token_symbols)
    else:
        tokens_to_query = token_ids
        token_filter = SolanaToken.id.in_(token_ids)

        # Get token symbols for the provided IDs
        token_map = {
            t.id: t.symbol for t in db.query(SolanaToken.id, SolanaToken.symbol)
            .filter(SolanaToken.id.in_(token_ids)).all()
        }

    # Create base query with joins
    query = (
        db.query(
            SolanaToken.symbol,
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        )
        .join(TokenMention, TokenMention.token_id == SolanaToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .join(SentimentAnalysis, SentimentAnalysis.tweet_id == Tweet.id)
        .filter(token_filter)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(SolanaToken.symbol, SentimentAnalysis.sentiment)
    )

    # Execute query
    sentiment_data = query.all()

    # Process results
    token_mentions = {}
    for symbol, sentiment, count, avg_confidence in sentiment_data:
        if symbol not in token_mentions:
            token_mentions[symbol] = {
                "total": 0,
                "sentiments": {}
            }

        token_mentions[symbol]["total"] += count
        token_mentions[symbol]["sentiments"][sentiment.value] = {
            "count": count,
            "avg_confidence": round(avg_confidence, 2)
        }

    # Calculate percentages and prepare final format
    for symbol, data in token_mentions.items():
        total = data["total"]

        # Initialize with empty sentiment data in case some sentiments have no data
        sentiment_data = {
            "POSITIVE": {"count": 0, "percentage": 0, "avg_confidence": 0},
            "NEGATIVE": {"count": 0, "percentage": 0, "avg_confidence": 0},
            "NEUTRAL": {"count": 0, "percentage": 0, "avg_confidence": 0}
        }

        # Update with actual data
        for sentiment, sentiment_stats in data["sentiments"].items():
            sentiment_data[sentiment] = {
                "count": sentiment_stats["count"],
                "percentage": round((sentiment_stats["count"] / total) * 100, 2) if total > 0 else 0,
                "avg_confidence": sentiment_stats["avg_confidence"]
            }

        # Calculate sentiment score: (positive - negative) / total
        positive_count = sentiment_data["POSITIVE"]["count"]
        negative_count = sentiment_data["NEGATIVE"]["count"]
        sentiment_score = round((positive_count - negative_count) / total, 2) if total > 0 else 0

        # Add to results
        results["tokens"][symbol] = {
            "total_mentions": total,
            "sentiment_score": sentiment_score,  # Range from -1 to 1
            "sentiments": sentiment_data
        }

    return results