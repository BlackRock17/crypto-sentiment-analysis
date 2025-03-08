from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from src.data_processing.models.database import SolanaToken, Tweet, SentimentAnalysis, TokenMention, SentimentEnum


def get_token_sentiment_stats(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        days_back: int = 7
) -> Dict:
    """
    Extract sentiment statistics for a specific token

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        days_back: Number of days to analyze

    Returns:
        Dictionary with sentiment statistics
    """
    if not token_symbol and not token_id:
        raise ValueError("Must provide either token_symbol or token_id")

    # Check if token exists
    if token_symbol:
        token_exists = db.query(SolanaToken).filter(SolanaToken.symbol == token_symbol).first() is not None
        if not token_exists:
            raise ValueError(f"Token with symbol '{token_symbol}' not found")
    elif token_id:
        token_exists = db.query(SolanaToken).filter(SolanaToken.id == token_id).first() is not None
        if not token_exists:
            raise ValueError(f"Token with ID '{token_id}' not found")

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

    # Filter by token
    if token_symbol:
        query = query.filter(SolanaToken.symbol == token_symbol)
    else:
        query = query.filter(SolanaToken.id == token_id)

    # Grouping by mood and sorting by number
    query = query.group_by(SentimentAnalysis.sentiment).order_by(desc("count"))

    # Execution of the request
    sentiment_stats = query.all()

    # Calculating the total number
    total_mentions = sum(stat.count for stat in sentiment_stats)

    # Format the result
    result = {
        "token": token_symbol if token_symbol else f"ID: {token_id}",
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

    # Validate interval
    valid_intervals = ["hour", "day", "week", "month"]
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")

    # Validate days_back
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Check if token exists
    if token_symbol:
        token_exists = db.query(SolanaToken).filter(SolanaToken.symbol == token_symbol).first() is not None
        if not token_exists:
            raise ValueError(f"Token with symbol '{token_symbol}' not found")
    elif token_id:
        token_exists = db.query(SolanaToken).filter(SolanaToken.id == token_id).first() is not None
        if not token_exists:
            raise ValueError(f"Token with ID '{token_id}' not found")

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

    # Check if tokens exist
    if token_symbols:
        existing_tokens = db.query(SolanaToken.symbol).filter(SolanaToken.symbol.in_(token_symbols)).all()
        existing_symbols = [token[0] for token in existing_tokens]
        missing_symbols = [symbol for symbol in token_symbols if symbol not in existing_symbols]

        if missing_symbols:
            raise ValueError(f"Tokens with symbols {missing_symbols} not found")
    elif token_ids:
        existing_tokens = db.query(SolanaToken.id).filter(SolanaToken.id.in_(token_ids)).all()
        existing_ids = [token[0] for token in existing_tokens]
        missing_ids = [tid for tid in token_ids if tid not in existing_ids]

        if missing_ids:
            raise ValueError(f"Tokens with IDs {missing_ids} not found")

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
            "positive": {"count": 0, "percentage": 0, "avg_confidence": 0},
            "negative": {"count": 0, "percentage": 0, "avg_confidence": 0},
            "neutral": {"count": 0, "percentage": 0, "avg_confidence": 0}
        }

        # Update with actual data
        for sentiment, sentiment_stats in data["sentiments"].items():
            sentiment_data[sentiment] = {
                "count": sentiment_stats["count"],
                "percentage": round((sentiment_stats["count"] / total) * 100, 2) if total > 0 else 0,
                "avg_confidence": sentiment_stats["avg_confidence"]
            }

        # Calculate sentiment score: (positive - negative) / total
        positive_count = sentiment_data["positive"]["count"]
        negative_count = sentiment_data["negative"]["count"]
        sentiment_score = round((positive_count - negative_count) / total, 2) if total > 0 else 0

        # Add to results
        results["tokens"][symbol] = {
            "total_mentions": total,
            "sentiment_score": sentiment_score,  # Range from -1 to 1
            "sentiments": sentiment_data
        }

    return results


def get_most_discussed_tokens(
        db: Session,
        days_back: int = 7,
        limit: int = 10,
        min_mentions: int = 5
) -> List[Dict]:
    """
    Get the most discussed tokens in a specific period

    Args:
        db: Database session
        days_back: Number of days to look back
        limit: Maximum number of tokens to return
        min_mentions: Minimum number of mentions required

    Returns:
        List of tokens with their mention statistics
    """
    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if limit <= 0:
        raise ValueError("limit must be a positive integer")
    if min_mentions < 0:
        raise ValueError("min_mentions cannot be negative")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Create query to count mentions for each token
    query = (
        db.query(
            SolanaToken.id,
            SolanaToken.symbol,
            SolanaToken.name,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == SolanaToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(SolanaToken.id, SolanaToken.symbol, SolanaToken.name)
        .having(func.count(TokenMention.id) >= min_mentions)
        .order_by(desc("mention_count"))
        .limit(limit)
    )

    # Execute query
    token_mentions = query.all()

    # Format results
    results = []
    for token_id, symbol, name, mention_count in token_mentions:
        # Calculate sentiment distribution in a subquery
        sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("sentiment_count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id == token_id)
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        sentiment_counts = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in sentiment_query:
            sentiment_counts[sentiment.value] = count

        total_sentiment_count = sum(sentiment_counts.values())

        # Calculate sentiment score
        positive_pct = round((sentiment_counts["positive"] / total_sentiment_count) * 100,
                             2) if total_sentiment_count > 0 else 0
        negative_pct = round((sentiment_counts["negative"] / total_sentiment_count) * 100,
                             2) if total_sentiment_count > 0 else 0
        sentiment_score = round((positive_pct - negative_pct) / 100, 2)

        results.append({
            "token_id": token_id,
            "symbol": symbol,
            "name": name,
            "mention_count": mention_count,
            "sentiment_score": sentiment_score,
            "sentiment_breakdown": {
                sentiment: {
                    "count": count,
                    "percentage": round((count / total_sentiment_count) * 100, 2) if total_sentiment_count > 0 else 0
                }
                for sentiment, count in sentiment_counts.items()
            }
        })

    return results


def get_top_users_by_token(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        days_back: int = 30,
        limit: int = 10
) -> dict[str, str | list[dict[str, dict[Any, dict[str, int | float]] | Any]] | Any]:
    """
    Get top users discussing a specific token

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        days_back: Number of days to look back
        limit: Maximum number of users to return

    Returns:
        List of users with their activity and sentiment statistics
    """
    if not token_symbol and not token_id:
        raise ValueError("Must provide either token_symbol or token_id")

    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if limit <= 0:
        raise ValueError("limit must be a positive integer")

    # Check if token exists
    if token_symbol:
        token_exists = db.query(SolanaToken).filter(SolanaToken.symbol == token_symbol).first() is not None
        if not token_exists:
            raise ValueError(f"Token with symbol '{token_symbol}' not found")
    elif token_id:
        token_exists = db.query(SolanaToken).filter(SolanaToken.id == token_id).first() is not None
        if not token_exists:
            raise ValueError(f"Token with ID '{token_id}' not found")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Create base query
    query = (
        db.query(
            Tweet.author_id,
            Tweet.author_username,
            func.count(Tweet.id).label("tweet_count"),
            func.sum(Tweet.like_count).label("total_likes"),
            func.sum(Tweet.retweet_count).label("total_retweets")
        )
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by token
    if token_symbol:
        query = query.filter(SolanaToken.symbol == token_symbol)
    else:
        query = query.filter(SolanaToken.id == token_id)

    # Group by author and order by tweet count
    query = (
        query.group_by(Tweet.author_id, Tweet.author_username)
        .order_by(desc("tweet_count"), desc("total_likes"))
        .limit(limit)
    )

    # Execute query
    user_data = query.all()

    # Process results
    results = []
    token_name = token_symbol if token_symbol else db.query(SolanaToken.symbol).filter(
        SolanaToken.id == token_id).scalar()

    for author_id, username, tweet_count, likes, retweets in user_data:
        # Get sentiment distribution for this user and token
        sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
            .filter(Tweet.author_id == author_id)
            .filter(Tweet.created_at.between(start_date, end_date))
        )

        # Filter by token
        if token_symbol:
            sentiment_query = sentiment_query.filter(SolanaToken.symbol == token_symbol)
        else:
            sentiment_query = sentiment_query.filter(SolanaToken.id == token_id)

        sentiment_query = sentiment_query.group_by(SentimentAnalysis.sentiment)

        # Calculate sentiment stats
        sentiment_counts = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in sentiment_query:
            sentiment_counts[sentiment.value] = count

        total_analyzed = sum(sentiment_counts.values())

        # Calculate sentiment percentages
        sentiment_percentages = {}
        for sentiment, count in sentiment_counts.items():
            sentiment_percentages[sentiment] = round((count / total_analyzed) * 100, 1) if total_analyzed > 0 else 0

        # Calculate influence score (simplified)
        engagement_rate = (likes + retweets * 2) / tweet_count if tweet_count > 0 else 0
        influence_score = round(engagement_rate * tweet_count / 1000, 2)

        # Add to results
        results.append({
            "author_id": author_id,
            "username": username,
            "tweet_count": tweet_count,
            "total_likes": likes,
            "total_retweets": retweets,
            "engagement_rate": round(engagement_rate, 2),
            "influence_score": influence_score,
            "sentiment_distribution": {
                sentiment: {
                    "count": count,
                    "percentage": sentiment_percentages[sentiment]
                }
                for sentiment, count in sentiment_counts.items()
            }
        })

    return {
        "token": token_name,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "top_users": results
    }


def analyze_token_correlation(
        db: Session,
        primary_token_symbol: str,
        days_back: int = 30,
        min_co_mentions: int = 3,
        limit: int = 10
) -> Dict:
    """
    Analyze which tokens are frequently mentioned together with a primary token

    Args:
        db: Database session
        primary_token_symbol: Primary token to analyze correlations for
        days_back: Number of days to look back
        min_co_mentions: Minimum number of co-mentions required to include a token
        limit: Maximum number of correlated tokens to return

    Returns:
        Dictionary with correlation data between tokens
    """
    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if min_co_mentions <= 0:
        raise ValueError("min_co_mentions must be a positive integer")
    if limit <= 0:
        raise ValueError("limit must be a positive integer")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Get the primary token ID
    primary_token = db.query(SolanaToken).filter(SolanaToken.symbol == primary_token_symbol).first()
    if not primary_token:
        raise ValueError(f"Token with symbol '{primary_token_symbol}' not found")

    # Find tweets that mention the primary token
    primary_token_tweets = (
        db.query(Tweet.id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .filter(TokenMention.token_id == primary_token.id)
        .filter(Tweet.created_at.between(start_date, end_date))
        .subquery()
    )

    # Find co-mentioned tokens in those tweets
    co_mentioned_tokens = (
        db.query(
            SolanaToken.id,
            SolanaToken.symbol,
            SolanaToken.name,
            func.count(TokenMention.tweet_id.distinct()).label("co_mention_count")
        )
        .join(TokenMention, TokenMention.token_id == SolanaToken.id)
        .join(primary_token_tweets, TokenMention.tweet_id == primary_token_tweets.c.id)
        .filter(SolanaToken.id != primary_token.id)  # Exclude the primary token
        .group_by(SolanaToken.id, SolanaToken.symbol, SolanaToken.name)
        .having(func.count(TokenMention.tweet_id.distinct()) >= min_co_mentions)
        .order_by(desc("co_mention_count"))
        .limit(limit)
    )

    # Execute query
    co_mentioned_data = co_mentioned_tokens.all()

    # Get total mentions of primary token for reference
    primary_mentions_count = (
        db.query(func.count(TokenMention.id.distinct()))
        .filter(TokenMention.token_id == primary_token.id)
        .filter(db.query(Tweet).filter(Tweet.id == TokenMention.tweet_id)
                .filter(Tweet.created_at.between(start_date, end_date))
                .exists())
        .scalar()
    )

    # Process results
    correlated_tokens = []
    for token_id, symbol, name, co_mention_count in co_mentioned_data:
        # Calculate correlation strength (percentage of co-mentions)
        correlation_percentage = round((co_mention_count / primary_mentions_count) * 100,
                                       2) if primary_mentions_count > 0 else 0

        # Get combined sentiment for co-mentions
        combined_sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id.distinct()).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.tweet_id.in_(
                db.query(TokenMention.tweet_id)
                .filter(TokenMention.token_id == primary_token.id)
                .filter(db.query(TokenMention)
                        .filter(TokenMention.tweet_id == TokenMention.tweet_id)
                        .filter(TokenMention.token_id == token_id)
                        .exists())
            ))
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        sentiment_data = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in combined_sentiment_query:
            sentiment_data[sentiment.value] = count

        total_sentiment = sum(sentiment_data.values())

        # Add to results
        correlated_tokens.append({
            "token_id": token_id,
            "symbol": symbol,
            "name": name,
            "co_mention_count": co_mention_count,
            "correlation_percentage": correlation_percentage,
            "combined_sentiment": {
                sentiment: {
                    "count": count,
                    "percentage": round((count / total_sentiment) * 100, 2) if total_sentiment > 0 else 0
                }
                for sentiment, count in sentiment_data.items()
            }
        })

    return {
        "primary_token": {
            "symbol": primary_token.symbol,
            "name": primary_token.name,
            "total_mentions": primary_mentions_count
        },
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "correlated_tokens": correlated_tokens
    }


def get_sentiment_momentum(
        db: Session,
        token_symbols: List[str] = None,
        top_n: int = 5,
        days_back: int = 14,
        min_mentions: int = 10
) -> Dict:
    """
    Calculate sentiment momentum (change in sentiment over time) for tokens

    Args:
        db: Database session
        token_symbols: List of token symbols to analyze (if None, will analyze top tokens)
        top_n: If token_symbols is None, analyze this many top tokens
        days_back: Total number of days to analyze
        min_mentions: Minimum mentions required for a token to be included

    Returns:
        Dictionary with sentiment momentum data for tokens
    """
    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if min_mentions < 0:
        raise ValueError("min_mentions cannot be negative")
    if top_n <= 0:
        raise ValueError("top_n must be a positive integer")

    # Check if tokens exist
    if token_symbols:
        existing_tokens = db.query(SolanaToken.symbol).filter(SolanaToken.symbol.in_(token_symbols)).all()
        existing_symbols = [token[0] for token in existing_tokens]
        missing_symbols = [symbol for symbol in token_symbols if symbol not in existing_symbols]

        if missing_symbols:
            raise ValueError(f"Tokens with symbols {missing_symbols} not found")

    # Calculate date ranges for comparison
    end_date = datetime.utcnow()
    mid_date = end_date - timedelta(days=days_back // 2)
    start_date = end_date - timedelta(days=days_back)

    # First period: start_date to mid_date
    # Second period: mid_date to end_date

    # If token_symbols not provided, find top tokens by mention count
    if not token_symbols:
        top_tokens_query = (
            db.query(
                SolanaToken.symbol
            )
            .join(TokenMention, TokenMention.token_id == SolanaToken.id)
            .join(Tweet, Tweet.id == TokenMention.tweet_id)
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(SolanaToken.symbol)
            .having(func.count(TokenMention.id) >= min_mentions)
            .order_by(desc(func.count(TokenMention.id)))
            .limit(top_n)
        )

        token_symbols = [row[0] for row in top_tokens_query.all()]

    # Container for results
    results = {
        "period_1": f"{start_date.strftime('%Y-%m-%d')} to {mid_date.strftime('%Y-%m-%d')}",
        "period_2": f"{mid_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "tokens": {}
    }

    # Analyze each token
    for symbol in token_symbols:
        # Get sentiment for first period
        period_1_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
            .filter(SolanaToken.symbol == symbol)
            .filter(Tweet.created_at.between(start_date, mid_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        # Get sentiment for second period
        period_2_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .join(SolanaToken, SolanaToken.id == TokenMention.token_id)
            .filter(SolanaToken.symbol == symbol)
            .filter(Tweet.created_at.between(mid_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        # Process period 1 data
        period_1_data = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in period_1_query:
            period_1_data[sentiment.value] = count

        total_period_1 = sum(period_1_data.values())

        # Process period 2 data
        period_2_data = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in period_2_query:
            period_2_data[sentiment.value] = count

        total_period_2 = sum(period_2_data.values())

        # Skip tokens with not enough mentions
        if total_period_1 < min_mentions // 2 or total_period_2 < min_mentions // 2:
            continue

        # Calculate sentiment scores for each period
        # Formula: (positive - negative) / total
        period_1_score = (period_1_data["positive"] - period_1_data[
            "negative"]) / total_period_1 if total_period_1 > 0 else 0
        period_2_score = (period_2_data["positive"] - period_2_data[
            "negative"]) / total_period_2 if total_period_2 > 0 else 0

        # Calculate momentum (change in sentiment)
        momentum = round(period_2_score - period_1_score, 3)

        # Calculate mention growth
        mention_growth = round(((total_period_2 - total_period_1) / total_period_1) * 100,
                               1) if total_period_1 > 0 else float('inf')

        # Add to results
        results["tokens"][symbol] = {
            "period_1": {
                "total_mentions": total_period_1,
                "sentiment_score": round(period_1_score, 3),
                "sentiment_breakdown": {
                    sentiment: {
                        "count": count,
                        "percentage": round((count / total_period_1) * 100, 1) if total_period_1 > 0 else 0
                    }
                    for sentiment, count in period_1_data.items()
                }
            },
            "period_2": {
                "total_mentions": total_period_2,
                "sentiment_score": round(period_2_score, 3),
                "sentiment_breakdown": {
                    sentiment: {
                        "count": count,
                        "percentage": round((count / total_period_2) * 100, 1) if total_period_2 > 0 else 0
                    }
                    for sentiment, count in period_2_data.items()
                }
            },
            "momentum": momentum,
            "mention_growth_percentage": mention_growth
        }

    # Sort tokens by momentum for easier analysis
    sorted_tokens = dict(sorted(
        results["tokens"].items(),
        key=lambda item: item[1]["momentum"],
        reverse=True
    ))

    results["tokens"] = sorted_tokens

    return results


def get_token_mention_stats(
        db: Session,
        token_id: int
) -> Dict[str, Any]:
    """
    Get mention statistics for a specific token.

    Args:
        db: Database session
        token_id: Token ID

    Returns:
        Dictionary with token mention statistics
    """
    # Get the token
    token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()
    if not token:
        raise ValueError(f"Token with ID {token_id} not found")

    # Get mention count
    mention_count = db.query(func.count(TokenMention.id)).filter(
        TokenMention.token_id == token_id
    ).scalar()

    # Get first and last mention dates
    first_mention = db.query(func.min(TokenMention.mentioned_at)).filter(
        TokenMention.token_id == token_id
    ).scalar()

    last_mention = db.query(func.max(TokenMention.mentioned_at)).filter(
        TokenMention.token_id == token_id
    ).scalar()

    # Get sentiment distribution
    sentiment_query = db.query(
        SentimentAnalysis.sentiment,
        func.count(SentimentAnalysis.id).label("count")
    ).join(
        Tweet, Tweet.id == SentimentAnalysis.tweet_id
    ).join(
        TokenMention, TokenMention.tweet_id == Tweet.id
    ).filter(
        TokenMention.token_id == token_id
    ).group_by(
        SentimentAnalysis.sentiment
    ).all()

    sentiment_counts = {s.value: 0 for s in SentimentEnum}
    for sentiment, count in sentiment_query:
        sentiment_counts[sentiment.value] = count

    total_sentiment = sum(sentiment_counts.values())

    # Calculate sentiment score (-1 to 1)
    sentiment_score = 0
    if total_sentiment > 0:
        sentiment_score = (sentiment_counts["positive"] - sentiment_counts["negative"]) / total_sentiment

    # Prepare result
    return {
        "token_id": token.id,
        "token_symbol": token.symbol,
        "token_name": token.name,
        "blockchain_network": token.blockchain_network,
        "mention_count": mention_count,
        "first_seen": first_mention,
        "last_seen": last_mention,
        "sentiment_score": sentiment_score,
        "sentiment_distribution": {
            sentiment: {
                "count": count,
                "percentage": round((count / total_sentiment) * 100, 1) if total_sentiment > 0 else 0
            }
            for sentiment, count in sentiment_counts.items()
        }
    }


def find_similar_tokens(
        db: Session,
        token_symbol: str,
        min_similarity: float = 0.7,
        exclude_token_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Find tokens with similar symbols.

    Args:
        db: Database session
        token_symbol: Token symbol to compare with
        min_similarity: Minimum similarity score (0-1)
        exclude_token_id: Optional token ID to exclude

    Returns:
        List of similar tokens with similarity scores
    """
    # Get all tokens
    query = db.query(BlockchainToken)

    # Exclude the specified token if needed
    if exclude_token_id is not None:
        query = query.filter(BlockchainToken.id != exclude_token_id)

    all_tokens = query.all()

    # Calculate similarity for each token
    similar_tokens = []

    # Normalize the input symbol
    normalized_symbol = token_symbol.lower().strip()

    for token in all_tokens:
        # Normalize the token symbol
        token_normalized = token.symbol.lower().strip()

        # Calculate similarity (simple matching for now)
        # In a real implementation, you might want to use a more sophisticated similarity metric
        if normalized_symbol == token_normalized:
            similarity = 1.0
        elif normalized_symbol in token_normalized or token_normalized in normalized_symbol:
            # Partial match
            similarity = len(min(normalized_symbol, token_normalized)) / len(max(normalized_symbol, token_normalized))
        else:
            # Different symbols
            continue

        # Check if similarity meets the threshold
        if similarity >= min_similarity:
            token_dict = {
                "id": token.id,
                "symbol": token.symbol,
                "name": token.name,
                "blockchain_network": token.blockchain_network,
                "similarity": similarity
            }
            similar_tokens.append(token_dict)

    # Sort by similarity (descending)
    similar_tokens.sort(key=lambda t: t["similarity"], reverse=True)

    return similar_tokens
