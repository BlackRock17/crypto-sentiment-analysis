from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from scipy.stats import pearsonr
from src.data_processing.models.database import BlockchainToken, Tweet, SentimentAnalysis, TokenMention, SentimentEnum, \
    BlockchainNetwork


def get_token_sentiment_stats(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        blockchain_network: str = None,  # Add blockchain network parameter
        days_back: int = 7
) -> Dict:
    """
    Extract sentiment statistics for a specific token

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        blockchain_network: Optional blockchain network name to filter by
        days_back: Number of days to analyze

    Returns:
        Dictionary with sentiment statistics
    """
    if not token_symbol and not token_id:
        raise ValueError("Must provide either token_symbol or token_id")

    # Check if token exists with proper filtering by network
    if token_symbol:
        token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol)
        if blockchain_network:
            token_query = token_query.filter(BlockchainToken.blockchain_network == blockchain_network)
        token_exists = token_query.first() is not None

        if not token_exists:
            if blockchain_network:
                error_message = f"Token with symbol '{token_symbol}' on network '{blockchain_network}' not found"
            else:
                error_message = f"Token with symbol '{token_symbol}' not found"
            raise ValueError(error_message)
    elif token_id:
        token_exists = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first() is not None
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
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by token
    if token_symbol:
        query = query.filter(BlockchainToken.symbol == token_symbol)
        # Also filter by blockchain network if provided
        if blockchain_network:
            query = query.filter(BlockchainToken.blockchain_network == blockchain_network)
    else:
        query = query.filter(BlockchainToken.id == token_id)

    # Grouping by mood and sorting by number
    query = query.group_by(SentimentAnalysis.sentiment).order_by(desc("count"))

    # Execution of the request
    sentiment_stats = query.all()

    # Calculating the total number
    total_mentions = sum(stat.count for stat in sentiment_stats)

    # Get token information for the result
    token_info = token_symbol if token_symbol else f"ID: {token_id}"
    if blockchain_network and token_symbol:
        token_info = f"{token_symbol} ({blockchain_network})"

    # Format the result
    result = {
        "token": token_info,
        "blockchain_network": blockchain_network,  # Include network in result
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
        blockchain_network: str = None,  # Add blockchain network parameter
        days_back: int = 30,
        interval: str = "day"
) -> dict[str, list[dict[str | Any, str | int | Any]] | str | dict[str, str | None]]:
    """
    Get sentiment trend for a specific token over time

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        blockchain_network: Optional blockchain network name to filter by
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

    # Check if token exists with proper filtering by network
    if token_symbol:
        token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol)
        if blockchain_network:
            token_query = token_query.filter(BlockchainToken.blockchain_network == blockchain_network)
        token_exists = token_query.first() is not None

        if not token_exists:
            if blockchain_network:
                error_message = f"Token with symbol '{token_symbol}' on network '{blockchain_network}' not found"
            else:
                error_message = f"Token with symbol '{token_symbol}' not found"
            raise ValueError(error_message)
    elif token_id:
        token_exists = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first() is not None
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
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by token
    if token_symbol:
        query = query.filter(BlockchainToken.symbol == token_symbol)
        # Also filter by blockchain network if provided
        if blockchain_network:
            query = query.filter(BlockchainToken.blockchain_network == blockchain_network)
    else:
        query = query.filter(BlockchainToken.id == token_id)

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

    # Add metadata about the token and network
    token_info = {
        "symbol": token_symbol if token_symbol else f"ID: {token_id}",
        "blockchain_network": blockchain_network
    }

    # Wrap the result with metadata
    return {
        "token_info": token_info,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "timeline": timeline_data
    }


def compare_token_sentiments(
        db: Session,
        token_symbols: List[str] = None,
        token_ids: List[int] = None,
        blockchain_networks: List[str] = None,  # Add blockchain networks parameter
        days_back: int = 7
) -> Dict:
    """
    Compare sentiment analysis between multiple tokens

    Args:
        db: Database session
        token_symbols: List of token symbols (e.g. ['SOL', 'USDC'])
        token_ids: List of token IDs (alternative to symbols)
        blockchain_networks: Optional list of blockchain networks to filter by
        days_back: Number of days to look back

    Returns:
        Dictionary with comparative sentiment data for each token
    """
    if not token_symbols and not token_ids:
        raise ValueError("Must provide either token_symbols or token_ids")

    if token_symbols and token_ids and len(token_symbols) != len(token_ids):
        raise ValueError("If both token_symbols and token_ids are provided, they must be the same length")

    # Check if we have network info for each token or just a general list
    use_token_specific_networks = False
    if blockchain_networks and len(blockchain_networks) > 0:
        if token_symbols and len(blockchain_networks) == len(token_symbols):
            use_token_specific_networks = True
        elif token_ids and len(blockchain_networks) == len(token_ids):
            use_token_specific_networks = True

    # Check if tokens exist with filtering by networks
    if token_symbols:
        if use_token_specific_networks:
            # Check each token with its specific network
            for i, symbol in enumerate(token_symbols):
                network = blockchain_networks[i] if i < len(blockchain_networks) else None
                query = db.query(BlockchainToken).filter(BlockchainToken.symbol == symbol)
                if network:
                    query = query.filter(BlockchainToken.blockchain_network == network)
                if not query.first():
                    network_msg = f" on network '{network}'" if network else ""
                    raise ValueError(f"Token with symbol '{symbol}'{network_msg} not found")
        else:
            # Check all tokens exist, optionally filtering by networks
            existing_token_query = db.query(BlockchainToken.symbol).filter(BlockchainToken.symbol.in_(token_symbols))
            if blockchain_networks:
                existing_token_query = existing_token_query.filter(
                    BlockchainToken.blockchain_network.in_(blockchain_networks))

            existing_symbols = [token[0] for token in existing_token_query.all()]
            missing_symbols = [symbol for symbol in token_symbols if symbol not in existing_symbols]

            if missing_symbols:
                network_msg = f" for networks {blockchain_networks}" if blockchain_networks else ""
                raise ValueError(f"Tokens with symbols {missing_symbols} not found{network_msg}")
    elif token_ids:
        existing_token_query = db.query(BlockchainToken.id).filter(BlockchainToken.id.in_(token_ids))
        if blockchain_networks and not use_token_specific_networks:
            existing_token_query = existing_token_query.filter(
                BlockchainToken.blockchain_network.in_(blockchain_networks))

        existing_ids = [token[0] for token in existing_token_query.all()]
        missing_ids = [tid for tid in token_ids if tid not in existing_ids]

        if missing_ids:
            network_msg = f" for networks {blockchain_networks}" if blockchain_networks else ""
            raise ValueError(f"Tokens with IDs {missing_ids} not found{network_msg}")

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
        # Create base query
        query = db.query(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        ).join(
            TokenMention, TokenMention.token_id == BlockchainToken.id
        ).join(
            Tweet, Tweet.id == TokenMention.tweet_id
        ).join(
            SentimentAnalysis, SentimentAnalysis.tweet_id == Tweet.id
        ).filter(
            BlockchainToken.symbol.in_(token_symbols)
        ).filter(
            Tweet.created_at.between(start_date, end_date)
        )

        # Apply network filters
        if blockchain_networks and not use_token_specific_networks:
            query = query.filter(BlockchainToken.blockchain_network.in_(blockchain_networks))

        # Group by token and sentiment
        query = query.group_by(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            SentimentAnalysis.sentiment
        )

    else:
        tokens_to_query = token_ids
        # Create base query
        query = db.query(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        ).join(
            TokenMention, TokenMention.token_id == BlockchainToken.id
        ).join(
            Tweet, Tweet.id == TokenMention.tweet_id
        ).join(
            SentimentAnalysis, SentimentAnalysis.tweet_id == Tweet.id
        ).filter(
            BlockchainToken.id.in_(token_ids)
        ).filter(
            Tweet.created_at.between(start_date, end_date)
        )

        # Apply network filters
        if blockchain_networks and not use_token_specific_networks:
            query = query.filter(BlockchainToken.blockchain_network.in_(blockchain_networks))

        # Group by token and sentiment
        query = query.group_by(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            SentimentAnalysis.sentiment
        )

    # Execute query
    sentiment_data = query.all()

    # Process results
    token_mentions = {}

    if token_symbols:
        for symbol, network, sentiment, count, avg_confidence in sentiment_data:
            token_key = f"{symbol}_{network}" if network else symbol

            if token_key not in token_mentions:
                token_mentions[token_key] = {
                    "symbol": symbol,
                    "blockchain_network": network,
                    "total": 0,
                    "sentiments": {}
                }

            token_mentions[token_key]["total"] += count
            token_mentions[token_key]["sentiments"][sentiment.value] = {
                "count": count,
                "avg_confidence": round(avg_confidence, 2)
            }
    else:
        for token_id, symbol, network, sentiment, count, avg_confidence in sentiment_data:
            token_key = f"{symbol}_{network}" if network else symbol

            if token_key not in token_mentions:
                token_mentions[token_key] = {
                    "id": token_id,
                    "symbol": symbol,
                    "blockchain_network": network,
                    "total": 0,
                    "sentiments": {}
                }

            token_mentions[token_key]["total"] += count
            token_mentions[token_key]["sentiments"][sentiment.value] = {
                "count": count,
                "avg_confidence": round(avg_confidence, 2)
            }

    # Calculate percentages and prepare final format
    for token_key, data in token_mentions.items():
        total = data["total"]
        symbol = data["symbol"]
        network = data["blockchain_network"]

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

        # Create result key based on symbol and network
        result_key = f"{symbol} ({network})" if network else symbol

        # Add to results
        results["tokens"][result_key] = {
            "symbol": symbol,
            "blockchain_network": network,
            "total_mentions": total,
            "sentiment_score": sentiment_score,  # Range from -1 to 1
            "sentiments": sentiment_data
        }

    return results


def get_most_discussed_tokens(
        db: Session,
        days_back: int = 7,
        limit: int = 10,
        min_mentions: int = 5,
        blockchain_network: str = None  # Add blockchain network parameter
) -> dict[str, dict[str, int | str | None] | list[dict[str, float | dict[Any, dict[str, int | float]] | str | Any]]]:
    """
    Get the most discussed tokens in a specific period

    Args:
        db: Database session
        days_back: Number of days to look back
        limit: Maximum number of tokens to return
        min_mentions: Minimum number of mentions required
        blockchain_network: Optional blockchain network to filter by

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
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.name,
            BlockchainToken.blockchain_network,  # Include blockchain network
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by blockchain network if provided
    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    # Group by token and complete the query
    query = (
        query.group_by(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.name,
            BlockchainToken.blockchain_network  # Include in group by
        )
        .having(func.count(TokenMention.id) >= min_mentions)
        .order_by(desc("mention_count"))
        .limit(limit)
    )

    # Execute query
    token_mentions = query.all()

    # Format results
    results = []
    for token_id, symbol, name, network, mention_count in token_mentions:
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

        # Format token info with network
        token_display = f"{symbol}" if not network else f"{symbol} ({network})"

        results.append({
            "token_id": token_id,
            "symbol": symbol,
            "name": name,
            "blockchain_network": network,  # Include network in results
            "display_name": token_display,  # Add formatted display name
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

    # Add metadata about the query
    metadata = {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "blockchain_network": blockchain_network,
        "total_tokens": len(results)
    }

    return {
        "metadata": metadata,
        "tokens": results
    }


def get_top_users_by_token(
        db: Session,
        token_symbol: str = None,
        token_id: int = None,
        blockchain_network: str = None,  # Add blockchain network parameter
        days_back: int = 30,
        limit: int = 10
) -> dict[str, str | list[dict[str, dict[Any, dict[str, int | float]] | Any]] | Any]:
    """
    Get top users discussing a specific token

    Args:
        db: Database session
        token_symbol: Token symbol (e.g. 'SOL')
        token_id: Token ID in the database (alternative to symbol)
        blockchain_network: Optional blockchain network name to filter by
        days_back: Number of days to look back
        limit: Maximum number of users to return

    Returns:
        Dictionary with list of users and their activity statistics
    """
    if not token_symbol and not token_id:
        raise ValueError("Must provide either token_symbol or token_id")

    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if limit <= 0:
        raise ValueError("limit must be a positive integer")

    # Check if token exists with proper filtering by network
    if token_symbol:
        token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol)
        if blockchain_network:
            token_query = token_query.filter(BlockchainToken.blockchain_network == blockchain_network)
        token = token_query.first()

        if not token:
            if blockchain_network:
                error_message = f"Token with symbol '{token_symbol}' on network '{blockchain_network}' not found"
            else:
                error_message = f"Token with symbol '{token_symbol}' not found"
            raise ValueError(error_message)
    elif token_id:
        token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()
        if not token:
            raise ValueError(f"Token with ID '{token_id}' not found")
        # Extract network info from the token
        blockchain_network = token.blockchain_network

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
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Filter by token
    if token_symbol:
        query = query.filter(BlockchainToken.symbol == token_symbol)
        # Also filter by blockchain network if provided
        if blockchain_network:
            query = query.filter(BlockchainToken.blockchain_network == blockchain_network)
    else:
        query = query.filter(BlockchainToken.id == token_id)

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

    # Get token display name
    if token_symbol:
        token_name = token_symbol
        if blockchain_network:
            token_name = f"{token_symbol} ({blockchain_network})"
    else:
        token_info = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()
        token_name = token_info.symbol
        if token_info.blockchain_network:
            token_name = f"{token_info.symbol} ({token_info.blockchain_network})"

    for author_id, username, tweet_count, likes, retweets in user_data:
        # Get sentiment distribution for this user and token
        sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
            .filter(Tweet.author_id == author_id)
            .filter(Tweet.created_at.between(start_date, end_date))
        )

        # Filter by token
        if token_symbol:
            sentiment_query = sentiment_query.filter(BlockchainToken.symbol == token_symbol)
            if blockchain_network:
                sentiment_query = sentiment_query.filter(BlockchainToken.blockchain_network == blockchain_network)
        else:
            sentiment_query = sentiment_query.filter(BlockchainToken.id == token_id)

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
        "blockchain_network": blockchain_network,  # Include network in result
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "top_users": results
    }


def analyze_token_correlation(
        db: Session,
        primary_token_symbol: str,
        blockchain_network: str = None,  # Add blockchain network parameter
        days_back: int = 30,
        min_co_mentions: int = 3,
        limit: int = 10
) -> Dict:
    """
    Analyze which tokens are frequently mentioned together with a primary token

    Args:
        db: Database session
        primary_token_symbol: Primary token to analyze correlations for
        blockchain_network: Optional blockchain network for the primary token
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
    primary_token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == primary_token_symbol)
    if blockchain_network:
        primary_token_query = primary_token_query.filter(BlockchainToken.blockchain_network == blockchain_network)

    primary_token = primary_token_query.first()
    if not primary_token:
        error_message = f"Token with symbol '{primary_token_symbol}'"
        if blockchain_network:
            error_message += f" on network '{blockchain_network}'"
        error_message += " not found"
        raise ValueError(error_message)

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
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.name,
            BlockchainToken.blockchain_network,  # Include network
            func.count(TokenMention.tweet_id.distinct()).label("co_mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(primary_token_tweets, TokenMention.tweet_id == primary_token_tweets.c.id)
        .filter(BlockchainToken.id != primary_token.id)  # Exclude the primary token
        .group_by(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.name,
            BlockchainToken.blockchain_network  # Include in group by
        )
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
    for token_id, symbol, name, network, co_mention_count in co_mentioned_data:
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

        # Format token display name with network
        token_display = f"{symbol}" if not network else f"{symbol} ({network})"

        # Add to results
        correlated_tokens.append({
            "token_id": token_id,
            "symbol": symbol,
            "name": name,
            "blockchain_network": network,  # Include network in results
            "display_name": token_display,  # Add formatted display name
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

    # Format primary token display
    primary_token_display = primary_token_symbol
    if blockchain_network:
        primary_token_display = f"{primary_token_symbol} ({blockchain_network})"

    return {
        "primary_token": {
            "id": primary_token.id,
            "symbol": primary_token.symbol,
            "name": primary_token.name,
            "blockchain_network": primary_token.blockchain_network,
            "display_name": primary_token_display,
            "total_mentions": primary_mentions_count
        },
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "correlated_tokens": correlated_tokens
    }


def get_sentiment_momentum(
        db: Session,
        token_symbols: List[str] = None,
        blockchain_networks: List[str] = None,  # Add blockchain networks parameter
        top_n: int = 5,
        days_back: int = 14,
        min_mentions: int = 10
) -> Dict:
    """
    Calculate sentiment momentum (change in sentiment over time) for tokens

    Args:
        db: Database session
        token_symbols: List of token symbols to analyze (if None, will analyze top tokens)
        blockchain_networks: Optional list of blockchain networks to filter by
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

    # Check if we have network info for each token or just a general list
    use_token_specific_networks = False
    if token_symbols and blockchain_networks and len(blockchain_networks) == len(token_symbols):
        use_token_specific_networks = True

    # Check if tokens exist with proper filtering
    if token_symbols:
        if use_token_specific_networks:
            # Check each token with its specific network
            for i, symbol in enumerate(token_symbols):
                network = blockchain_networks[i] if i < len(blockchain_networks) else None
                query = db.query(BlockchainToken).filter(BlockchainToken.symbol == symbol)
                if network:
                    query = query.filter(BlockchainToken.blockchain_network == network)
                if not query.first():
                    network_msg = f" on network '{network}'" if network else ""
                    raise ValueError(f"Token with symbol '{symbol}'{network_msg} not found")
        elif blockchain_networks:
            # Check tokens exist with any of the provided networks
            for symbol in token_symbols:
                query = db.query(BlockchainToken).filter(
                    BlockchainToken.symbol == symbol,
                    BlockchainToken.blockchain_network.in_(blockchain_networks)
                )
                if not query.first():
                    raise ValueError(f"Token with symbol '{symbol}' not found in specified networks")

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
                BlockchainToken.symbol,
                BlockchainToken.blockchain_network
            )
            .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
            .join(Tweet, Tweet.id == TokenMention.tweet_id)
            .filter(Tweet.created_at.between(start_date, end_date))
        )

        # Apply network filter if provided
        if blockchain_networks:
            top_tokens_query = top_tokens_query.filter(BlockchainToken.blockchain_network.in_(blockchain_networks))

        # Complete query with group by, having, order by and limit
        top_tokens_query = (
            top_tokens_query.group_by(BlockchainToken.symbol, BlockchainToken.blockchain_network)
            .having(func.count(TokenMention.id) >= min_mentions)
            .order_by(desc(func.count(TokenMention.id)))
            .limit(top_n)
        )

        # Execute query and format results
        token_network_pairs = []
        for symbol, network in top_tokens_query.all():
            token_network_pairs.append((symbol, network))

        # If no tokens found, return empty result
        if not token_network_pairs:
            return {
                "period_1": f"{start_date.strftime('%Y-%m-%d')} to {mid_date.strftime('%Y-%m-%d')}",
                "period_2": f"{mid_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                "tokens": {}
            }
    else:
        # Use provided tokens and networks
        token_network_pairs = []
        if use_token_specific_networks:
            for i, symbol in enumerate(token_symbols):
                network = blockchain_networks[i] if i < len(blockchain_networks) else None
                token_network_pairs.append((symbol, network))
        else:
            # If we have networks but not per token, we need to get all combinations
            for symbol in token_symbols:
                if blockchain_networks:
                    # For each network, check if the token exists with that network
                    for network in blockchain_networks:
                        if db.query(BlockchainToken).filter(
                                BlockchainToken.symbol == symbol,
                                BlockchainToken.blockchain_network == network
                        ).first():
                            token_network_pairs.append((symbol, network))
                else:
                    # No networks specified, get all networks for this token
                    networks = db.query(BlockchainToken.blockchain_network).filter(
                        BlockchainToken.symbol == symbol
                    ).distinct().all()

                    for (network,) in networks:
                        token_network_pairs.append((symbol, network))

    # Container for results
    results = {
        "period_1": f"{start_date.strftime('%Y-%m-%d')} to {mid_date.strftime('%Y-%m-%d')}",
        "period_2": f"{mid_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "tokens": {}
    }

    # Analyze each token-network pair
    for symbol, network in token_network_pairs:
        # Create token key for results
        token_key = f"{symbol}" if not network else f"{symbol}_{network}"
        display_name = f"{symbol}" if not network else f"{symbol} ({network})"

        # Get token ID(s)
        token_query = db.query(BlockchainToken.id).filter(BlockchainToken.symbol == symbol)
        if network:
            token_query = token_query.filter(BlockchainToken.blockchain_network == network)
        token_ids = [id for (id,) in token_query.all()]

        if not token_ids:
            continue

        # Get sentiment for first period
        period_1_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id.in_(token_ids))
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
            .filter(TokenMention.token_id.in_(token_ids))
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
        results["tokens"][display_name] = {
            "symbol": symbol,
            "blockchain_network": network,
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


def compare_blockchain_networks_sentiment(
        db: Session,
        network_names: List[str],
        days_back: int = 30,
        min_tokens_per_network: int = 5,
        min_mentions_per_token: int = 3
) -> Dict:
    """
    Compare sentiment across different blockchain networks

    Args:
        db: Database session
        network_names: List of blockchain network names to compare
        days_back: Number of days to look back
        min_tokens_per_network: Minimum number of tokens required for a network to be included
        min_mentions_per_token: Minimum mentions required for a token to be included

    Returns:
        Dictionary with comparative sentiment data for each network
    """
    # Validate inputs
    if not network_names or len(network_names) == 0:
        raise ValueError("Must provide at least one network name")

    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Check if networks exist
    existing_networks = db.query(BlockchainNetwork.name).filter(
        BlockchainNetwork.name.in_(network_names)
    ).all()

    existing_network_names = [network[0] for network in existing_networks]
    missing_networks = [name for name in network_names if name not in existing_network_names]

    if missing_networks:
        raise ValueError(f"Blockchain networks with names {missing_networks} not found")

    # Container for results
    results = {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "networks": {}
    }

    # Process each network
    for network_name in network_names:
        # Get tokens for this network with enough mentions
        token_query = (
            db.query(
                BlockchainToken.id,
                BlockchainToken.symbol,
                func.count(TokenMention.id).label("mention_count")
            )
            .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
            .join(Tweet, Tweet.id == TokenMention.tweet_id)
            .filter(BlockchainToken.blockchain_network == network_name)
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(BlockchainToken.id, BlockchainToken.symbol)
            .having(func.count(TokenMention.id) >= min_mentions_per_token)
            .order_by(desc("mention_count"))
        )

        tokens = token_query.all()

        # Skip networks with not enough tokens
        if len(tokens) < min_tokens_per_network:
            continue

        # Get sentiment distribution for this network
        network_sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count"),
                func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
            .filter(BlockchainToken.blockchain_network == network_name)
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        # Process sentiment data
        sentiment_data = {sentiment.value: {"count": 0, "avg_confidence": 0} for sentiment in SentimentEnum}
        for sentiment, count, avg_confidence in network_sentiment_query:
            sentiment_data[sentiment.value] = {
                "count": count,
                "avg_confidence": round(avg_confidence, 2)
            }

        total_sentiment = sum(data["count"] for data in sentiment_data.values())

        # Calculate sentiment score: (positive - negative) / total
        positive_count = sentiment_data["positive"]["count"]
        negative_count = sentiment_data["negative"]["count"]
        sentiment_score = round((positive_count - negative_count) / total_sentiment, 2) if total_sentiment > 0 else 0

        # Calculate sentiment percentages
        for sentiment, data in sentiment_data.items():
            data["percentage"] = round((data["count"] / total_sentiment) * 100, 1) if total_sentiment > 0 else 0

        # Get top tokens by mention
        top_tokens = [
            {
                "token_id": token_id,
                "symbol": symbol,
                "mentions": mention_count
            }
            for token_id, symbol, mention_count in tokens[:10]  # Limit to top 10
        ]

        # Add network data to results
        results["networks"][network_name] = {
            "total_tokens": len(tokens),
            "total_mentions": total_sentiment,
            "sentiment_score": sentiment_score,
            "sentiment_breakdown": sentiment_data,
            "top_tokens": top_tokens
        }

    # Sort networks by total mentions
    sorted_networks = dict(sorted(
        results["networks"].items(),
        key=lambda item: item[1]["total_mentions"],
        reverse=True
    ))

    results["networks"] = sorted_networks

    return results


# New function for sentiment timeline by network
def get_network_sentiment_timeline(
        db: Session,
        blockchain_network: str,
        days_back: int = 30,
        interval: str = "day"
) -> Dict:
    """
    Get sentiment trend for a specific blockchain network over time

    Args:
        db: Database session
        blockchain_network: Blockchain network name
        days_back: Number of days to look back
        interval: Time grouping interval ('day', 'week', 'hour')

    Returns:
        Dictionary with sentiment timeline data for the network
    """
    # Validate inputs
    valid_intervals = ["hour", "day", "week", "month"]
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")

    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Check if network exists
    network = db.query(BlockchainNetwork).filter(BlockchainNetwork.name == blockchain_network).first()
    if not network:
        raise ValueError(f"Blockchain network '{blockchain_network}' not found")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Create query for sentiment timeline
    query = (
        db.query(
            func.date_trunc(interval, Tweet.created_at).label('time_bucket'),
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count"),
            func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
        )
        .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(BlockchainToken.blockchain_network == blockchain_network)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by('time_bucket', SentimentAnalysis.sentiment)
        .order_by('time_bucket')
    )

    # Execute query
    timeline_data = query.all()

    # Process results into timeline format
    processed_timeline = []
    current_bucket = None
    current_data = None

    for bucket, sentiment, count, avg_confidence in timeline_data:
        # If we're on a new time bucket, create a new data point
        if current_bucket != bucket:
            if current_data:
                # Calculate percentages
                if current_data["total"] > 0:
                    current_data["positive_pct"] = round((current_data["positive"] / current_data["total"]) * 100, 2)
                    current_data["negative_pct"] = round((current_data["negative"] / current_data["total"]) * 100, 2)
                    current_data["neutral_pct"] = round((current_data["neutral"] / current_data["total"]) * 100, 2)

                    # Calculate sentiment score (-1 to 1)
                    current_data["sentiment_score"] = round(
                        (current_data["positive"] - current_data["negative"]) / current_data["total"],
                        2
                    )

                processed_timeline.append(current_data)

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
                "sentiment_score": 0
            }

        # Add sentiment counts to the current bucket
        current_data["total"] += count
        sentiment_key = sentiment.value.lower()
        current_data[sentiment_key] = count

    # Add the last data point if it exists
    if current_data:
        # Calculate percentages for last data point
        if current_data["total"] > 0:
            current_data["positive_pct"] = round((current_data["positive"] / current_data["total"]) * 100, 2)
            current_data["negative_pct"] = round((current_data["negative"] / current_data["total"]) * 100, 2)
            current_data["neutral_pct"] = round((current_data["neutral"] / current_data["total"]) * 100, 2)

            # Calculate sentiment score (-1 to 1)
            current_data["sentiment_score"] = round(
                (current_data["positive"] - current_data["negative"]) / current_data["total"],
                2
            )

        processed_timeline.append(current_data)

    # Calculate overall sentiment stats
    total_mentions = sum(data["total"] for data in processed_timeline)
    total_positive = sum(data["positive"] for data in processed_timeline)
    total_negative = sum(data["negative"] for data in processed_timeline)
    total_neutral = sum(data["neutral"] for data in processed_timeline)

    overall_sentiment_score = round(
        (total_positive - total_negative) / total_mentions,
        2
    ) if total_mentions > 0 else 0

    # Get top tokens by mention count
    top_tokens_query = (
        db.query(
            BlockchainToken.symbol,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(BlockchainToken.blockchain_network == blockchain_network)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(BlockchainToken.symbol)
        .order_by(desc("mention_count"))
        .limit(5)  # Top 5 tokens
    )

    top_tokens = [
        {"symbol": symbol, "mentions": mentions}
        for symbol, mentions in top_tokens_query
    ]

    # Return result
    return {
        "network": {
            "name": blockchain_network,
            "display_name": network.display_name if network.display_name else blockchain_network
        },
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "total_mentions": total_mentions,
        "overall_sentiment": {
            "positive": total_positive,
            "negative": total_negative,
            "neutral": total_neutral,
            "positive_pct": round((total_positive / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "negative_pct": round((total_negative / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "neutral_pct": round((total_neutral / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "sentiment_score": overall_sentiment_score
        },
        "top_tokens": top_tokens,
        "timeline": processed_timeline
    }


# New function for cross-network token sentiment comparison
def compare_token_across_networks(
        db: Session,
        token_symbol: str,
        blockchain_networks: List[str] = None,
        days_back: int = 30
) -> Dict:
    """
    Compare sentiment for tokens with the same symbol across different blockchain networks

    Args:
        db: Database session
        token_symbol: Token symbol to analyze
        blockchain_networks: List of blockchain networks to include (if None, includes all networks)
        days_back: Number of days to look back

    Returns:
        Dictionary with comparative data across networks
    """
    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Check if token exists
    token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol)

    # If specific networks provided, filter by them
    if blockchain_networks:
        token_query = token_query.filter(BlockchainToken.blockchain_network.in_(blockchain_networks))

    tokens = token_query.all()

    if not tokens:
        error_msg = f"Token with symbol '{token_symbol}' not found"
        if blockchain_networks:
            error_msg += f" in specified networks {blockchain_networks}"
        raise ValueError(error_msg)

    # Get networks where this token exists
    networks = [token.blockchain_network for token in tokens if token.blockchain_network]

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Container for results
    results = {
        "token_symbol": token_symbol,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "networks": {}
    }

    # Process each network
    for network in networks:
        # Get token ID(s) for this symbol on this network
        token_ids = [
            token.id for token in tokens
            if token.blockchain_network == network
        ]

        if not token_ids:
            continue

        # Get sentiment distribution
        sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count"),
                func.avg(SentimentAnalysis.confidence_score).label("avg_confidence")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id.in_(token_ids))
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        # Process sentiment data
        sentiment_data = {sentiment.value: {"count": 0, "avg_confidence": 0} for sentiment in SentimentEnum}
        for sentiment, count, avg_confidence in sentiment_query:
            sentiment_data[sentiment.value] = {
                "count": count,
                "avg_confidence": round(avg_confidence, 2)
            }

        total_mentions = sum(data["count"] for data in sentiment_data.values())

        # Skip if no mentions
        if total_mentions == 0:
            continue

        # Calculate sentiment percentages
        for sentiment, data in sentiment_data.items():
            data["percentage"] = round((data["count"] / total_mentions) * 100, 1) if total_mentions > 0 else 0

        # Calculate sentiment score: (positive - negative) / total
        positive_count = sentiment_data["positive"]["count"]
        negative_count = sentiment_data["negative"]["count"]
        sentiment_score = round((positive_count - negative_count) / total_mentions, 2)

        # Get sentiment timeline for this token and network
        timeline_query = (
            db.query(
                func.date_trunc("day", Tweet.created_at).label('time_bucket'),
                func.count(TokenMention.id).label("mention_count")
            )
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id.in_(token_ids))
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by('time_bucket')
            .order_by('time_bucket')
        )

        # Process timeline data
        timeline = [
            {
                "date": bucket.strftime('%Y-%m-%d'),
                "mentions": count
            }
            for bucket, count in timeline_query
        ]

        # Get top users for this token and network
        users_query = (
            db.query(
                Tweet.author_username,
                func.count(Tweet.id).label("tweet_count")
            )
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id.in_(token_ids))
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(Tweet.author_username)
            .order_by(desc("tweet_count"))
            .limit(5)  # Top 5 users
        )

        top_users = [
            {"username": username, "tweet_count": count}
            for username, count in users_query
        ]

        # Add to results
        results["networks"][network] = {
            "total_mentions": total_mentions,
            "sentiment_score": sentiment_score,
            "sentiment_breakdown": sentiment_data,
            "timeline": timeline,
            "top_users": top_users
        }

    # Calculate relative popularity across networks
    total_all_networks = sum(data["total_mentions"] for data in results["networks"].values())

    if total_all_networks > 0:
        for network, data in results["networks"].items():
            data["popularity_percentage"] = round((data["total_mentions"] / total_all_networks) * 100, 1)

    # Sort networks by total mentions
    sorted_networks = dict(sorted(
        results["networks"].items(),
        key=lambda item: item[1]["total_mentions"],
        reverse=True
    ))

    results["networks"] = sorted_networks
    results["total_mentions_all_networks"] = total_all_networks
    results["network_count"] = len(sorted_networks)

    return results


# New function for network-token matrix analysis
def get_network_token_sentiment_matrix(
        db: Session,
        top_n_tokens: int = 10,
        top_n_networks: int = 5,
        days_back: int = 30,
        min_mentions: int = 5
) -> Dict:
    """
    Create a sentiment matrix between top tokens and networks

    Args:
        db: Database session
        top_n_tokens: Number of top tokens to include
        top_n_networks: Number of top networks to include
        days_back: Number of days to look back
        min_mentions: Minimum mentions required for inclusion

    Returns:
        Dictionary with matrix of sentiment data between tokens and networks
    """
    # Validate inputs
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")
    if top_n_tokens <= 0 or top_n_networks <= 0:
        raise ValueError("top_n_tokens and top_n_networks must be positive integers")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Get top networks by mention
    top_networks_query = (
        db.query(
            BlockchainToken.blockchain_network,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(BlockchainToken.blockchain_network != None)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(BlockchainToken.blockchain_network)
        .having(func.count(TokenMention.id) >= min_mentions)
        .order_by(desc("mention_count"))
        .limit(top_n_networks)
    )

    top_networks = [network for network, _ in top_networks_query]

    if not top_networks:
        return {
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "matrix": []
        }

    # Get top tokens across all networks
    top_tokens_query = (
        db.query(
            BlockchainToken.symbol,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(BlockchainToken.blockchain_network.in_(top_networks))
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(BlockchainToken.symbol)
        .having(func.count(TokenMention.id) >= min_mentions)
        .order_by(desc("mention_count"))
        .limit(top_n_tokens)
    )

    top_tokens = [symbol for symbol, _ in top_tokens_query]

    if not top_tokens:
        return {
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "matrix": []
        }

    # Create matrix
    matrix = []

    # Process each token across each network
    for symbol in top_tokens:
        row = {
            "token": symbol,
            "networks": {}
        }

        for network in top_networks:
            # Get sentiment for this token and network
            token_ids = [
                token.id for token in
                db.query(BlockchainToken.id).filter(
                    BlockchainToken.symbol == symbol,
                    BlockchainToken.blockchain_network == network
                ).all()
            ]

            if not token_ids:
                # Token doesn't exist on this network
                row["networks"][network] = None
                continue

            # Get sentiment data
            sentiment_query = (
                db.query(
                    SentimentAnalysis.sentiment,
                    func.count(SentimentAnalysis.id).label("count")
                )
                .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
                .join(TokenMention, TokenMention.tweet_id == Tweet.id)
                .filter(TokenMention.token_id.in_(token_ids))
                .filter(Tweet.created_at.between(start_date, end_date))
                .group_by(SentimentAnalysis.sentiment)
            )

            # Process sentiment data
            sentiment_counts = {sentiment.value: 0 for sentiment in SentimentEnum}
            for sentiment, count in sentiment_query:
                sentiment_counts[sentiment.value] = count

            mentions = sum(sentiment_counts.values())

            # Skip if not enough mentions
            if mentions < min_mentions:
                row["networks"][network] = None
                continue

            # Calculate sentiment score
            positive = sentiment_counts["positive"]
            negative = sentiment_counts["negative"]
            sentiment_score = round((positive - negative) / mentions, 2) if mentions > 0 else 0

            # Add to row
            row["networks"][network] = {
                "mentions": mentions,
                "sentiment_score": sentiment_score,
                "sentiment_counts": sentiment_counts
            }

        matrix.append(row)

    # Sort networks by total mentions across all tokens
    network_totals = {network: 0 for network in top_networks}
    for row in matrix:
        for network, data in row["networks"].items():
            if data is not None:
                network_totals[network] += data["mentions"]

    sorted_networks = sorted(
        top_networks,
        key=lambda network: network_totals.get(network, 0),
        reverse=True
    )

    # Return result
    return {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "networks": sorted_networks,
        "tokens": top_tokens,
        "matrix": matrix
    }


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


# Function to get sentiment trends across all networks
def get_global_sentiment_trends(
        db: Session,
        days_back: int = 30,
        interval: str = "day",
        top_networks: int = None
) -> Dict:
    """
    Get global sentiment trends across all blockchain networks

    Args:
        db: Database session
        days_back: Number of days to look back
        interval: Time grouping interval ('day', 'week', 'hour')
        top_networks: If provided, include only the top N networks by mention count

    Returns:
        Dictionary with global sentiment trends and breakdown by network
    """
    # Validate inputs
    valid_intervals = ["hour", "day", "week", "month"]
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")

    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Determine networks to include
    network_filter = None
    if top_networks:
        # Get top networks by mention
        top_networks_query = (
            db.query(
                BlockchainToken.blockchain_network
            )
            .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
            .join(Tweet, Tweet.id == TokenMention.tweet_id)
            .filter(BlockchainToken.blockchain_network != None)
            .filter(Tweet.created_at.between(start_date, end_date))
            .group_by(BlockchainToken.blockchain_network)
            .order_by(desc(func.count(TokenMention.id)))
            .limit(top_networks)
        )

        network_filter = [network[0] for network in top_networks_query]

    # Get overall sentiment timeline
    query = (
        db.query(
            func.date_trunc(interval, Tweet.created_at).label('time_bucket'),
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count")
        )
        .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Apply network filter if provided
    if network_filter:
        query = query.filter(BlockchainToken.blockchain_network.in_(network_filter))

    query = query.group_by('time_bucket', SentimentAnalysis.sentiment).order_by('time_bucket')

    # Execute query
    sentiment_timeline = query.all()

    # Process results into timeline format
    processed_timeline = []
    current_bucket = None
    current_data = None

    for bucket, sentiment, count in sentiment_timeline:
        # If we're on a new time bucket, create a new data point
        if current_bucket != bucket:
            if current_data:
                # Calculate percentages
                if current_data["total"] > 0:
                    current_data["positive_pct"] = round((current_data["positive"] / current_data["total"]) * 100, 2)
                    current_data["negative_pct"] = round((current_data["negative"] / current_data["total"]) * 100, 2)
                    current_data["neutral_pct"] = round((current_data["neutral"] / current_data["total"]) * 100, 2)

                    # Calculate sentiment score (-1 to 1)
                    current_data["sentiment_score"] = round(
                        (current_data["positive"] - current_data["negative"]) / current_data["total"],
                        2
                    )

                processed_timeline.append(current_data)

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
                "sentiment_score": 0
            }

        # Add sentiment counts to the current bucket
        current_data["total"] += count
        sentiment_key = sentiment.value.lower()
        current_data[sentiment_key] = count

    # Add the last data point if it exists
    if current_data:
        # Calculate percentages for last data point
        if current_data["total"] > 0:
            current_data["positive_pct"] = round((current_data["positive"] / current_data["total"]) * 100, 2)
            current_data["negative_pct"] = round((current_data["negative"] / current_data["total"]) * 100, 2)
            current_data["neutral_pct"] = round((current_data["neutral"] / current_data["total"]) * 100, 2)

            # Calculate sentiment score (-1 to 1)
            current_data["sentiment_score"] = round(
                (current_data["positive"] - current_data["negative"]) / current_data["total"],
                2
            )

        processed_timeline.append(current_data)

    # Get sentiment by network
    network_sentiment_query = (
        db.query(
            BlockchainToken.blockchain_network,
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .join(SentimentAnalysis, SentimentAnalysis.tweet_id == Tweet.id)
        .filter(BlockchainToken.blockchain_network != None)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Apply network filter if provided
    if network_filter:
        network_sentiment_query = network_sentiment_query.filter(
            BlockchainToken.blockchain_network.in_(network_filter)
        )

    network_sentiment_query = network_sentiment_query.group_by(
        BlockchainToken.blockchain_network, SentimentAnalysis.sentiment
    )

    # Process network sentiment data
    network_sentiment = {}
    for network, sentiment, count in network_sentiment_query:
        if network not in network_sentiment:
            network_sentiment[network] = {
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "total": 0
            }

        network_sentiment[network][sentiment.value.lower()] = count
        network_sentiment[network]["total"] += count

    # Calculate percentages and scores for networks
    for network, data in network_sentiment.items():
        if data["total"] > 0:
            data["positive_pct"] = round((data["positive"] / data["total"]) * 100, 2)
            data["negative_pct"] = round((data["negative"] / data["total"]) * 100, 2)
            data["neutral_pct"] = round((data["neutral"] / data["total"]) * 100, 2)
            data["sentiment_score"] = round((data["positive"] - data["negative"]) / data["total"], 2)

    # Sort networks by total mentions
    sorted_network_sentiment = dict(sorted(
        network_sentiment.items(),
        key=lambda item: item[1]["total"],
        reverse=True
    ))

    # Calculate overall sentiment stats
    total_mentions = sum(data["total"] for data in network_sentiment.values())
    total_positive = sum(data["positive"] for data in network_sentiment.values())
    total_negative = sum(data["negative"] for data in network_sentiment.values())
    total_neutral = sum(data["neutral"] for data in network_sentiment.values())

    overall_sentiment_score = round(
        (total_positive - total_negative) / total_mentions,
        2
    ) if total_mentions > 0 else 0

    # Return result
    return {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "total_mentions": total_mentions,
        "overall_sentiment": {
            "positive": total_positive,
            "negative": total_negative,
            "neutral": total_neutral,
            "positive_pct": round((total_positive / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "negative_pct": round((total_negative / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "neutral_pct": round((total_neutral / total_mentions) * 100, 2) if total_mentions > 0 else 0,
            "sentiment_score": overall_sentiment_score
        },
        "timeline": processed_timeline,
        "network_sentiment": sorted_network_sentiment,
        "networks_included": network_filter
    }


def get_token_categorization_stats(
        db: Session,
        days_back: int = 30
) -> Dict:
    """
    Get statistics about token categorization quality and coverage

    Args:
        db: Database session
        days_back: Number of days to analyze

    Returns:
        Dictionary with token categorization statistics
    """
    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Count tokens by categorization status
    categorization_query = (
        db.query(
            BlockchainToken.blockchain_network != None,
            BlockchainToken.manually_verified,
            BlockchainToken.needs_review,
            func.count(BlockchainToken.id).label("token_count")
        )
        .group_by(
            BlockchainToken.blockchain_network != None,
            BlockchainToken.manually_verified,
            BlockchainToken.needs_review
        )
    )

    # Process results
    categorization_stats = {
        "categorized": 0,
        "uncategorized": 0,
        "manually_verified": 0,
        "auto_categorized": 0,
        "needs_review": 0,
        "confidence_levels": {
            "high": 0,
            "medium": 0,
            "low": 0
        }
    }

    for has_network, is_verified, needs_review, count in categorization_query:
        if has_network:
            categorization_stats["categorized"] += count
            if is_verified:
                categorization_stats["manually_verified"] += count
            else:
                categorization_stats["auto_categorized"] += count
        else:
            categorization_stats["uncategorized"] += count

        if needs_review:
            categorization_stats["needs_review"] += count

    # Count tokens by confidence level
    confidence_query = (
        db.query(
            func.case(
                (BlockchainToken.network_confidence >= 0.8, "high"),
                (BlockchainToken.network_confidence >= 0.5, "medium"),
                else_="low"
            ).label("confidence_level"),
            func.count(BlockchainToken.id).label("token_count")
        )
        .filter(BlockchainToken.blockchain_network != None)
        .group_by("confidence_level")
    )

    for level, count in confidence_query:
        categorization_stats["confidence_levels"][level] = count

    # Calculate coverage stats
    total_tokens = categorization_stats["categorized"] + categorization_stats["uncategorized"]

    coverage_stats = {
        "total_tokens": total_tokens,
        "categorized_percentage": round((categorization_stats["categorized"] / total_tokens) * 100,
                                        2) if total_tokens > 0 else 0,
        "manually_verified_percentage": round((categorization_stats["manually_verified"] / total_tokens) * 100,
                                              2) if total_tokens > 0 else 0,
        "needs_review_percentage": round((categorization_stats["needs_review"] / total_tokens) * 100,
                                         2) if total_tokens > 0 else 0
    }

    # Get token mention stats by categorization status
    mention_query = (
        db.query(
            BlockchainToken.blockchain_network != None,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(BlockchainToken.blockchain_network != None)
    )

    mention_stats = {
        "categorized_mentions": 0,
        "uncategorized_mentions": 0
    }

    for has_network, count in mention_query:
        if has_network:
            mention_stats["categorized_mentions"] = count
        else:
            mention_stats["uncategorized_mentions"] = count

    total_mentions = mention_stats["categorized_mentions"] + mention_stats["uncategorized_mentions"]
    mention_stats["total_mentions"] = total_mentions
    mention_stats["categorized_percentage"] = round((mention_stats["categorized_mentions"] / total_mentions) * 100,
                                                    2) if total_mentions > 0 else 0

    # Return combined results
    return {
        "token_stats": categorization_stats,
        "coverage_stats": coverage_stats,
        "mention_stats": mention_stats,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    }


def detect_trending_tokens(
        db: Session,
        lookback_window: int = 7,
        comparison_window: int = 7,
        min_mentions: int = 10,
        limit: int = 20,
        blockchain_network: str = None
) -> Dict:
    """
    Detect trending tokens based on mention growth

    Args:
        db: Database session
        lookback_window: Number of days to look back for current period
        comparison_window: Number of days to look back for comparison period
        min_mentions: Minimum mentions in current period to be considered
        limit: Maximum number of tokens to return
        blockchain_network: Optional blockchain network to filter by

    Returns:
        Dictionary with trending token data
    """
    # Calculate date ranges
    end_date = datetime.utcnow()
    mid_date = end_date - timedelta(days=lookback_window)
    start_date = mid_date - timedelta(days=comparison_window)

    # Current period: mid_date to end_date
    # Comparison period: start_date to mid_date

    # Get token mentions for current period
    current_query = (
        db.query(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(Tweet.created_at.between(mid_date, end_date))
    )

    # Apply network filter if provided
    if blockchain_network:
        current_query = current_query.filter(BlockchainToken.blockchain_network == blockchain_network)

    # Group by and set minimum threshold
    current_query = (
        current_query.group_by(
            BlockchainToken.id,
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network
        )
        .having(func.count(TokenMention.id) >= min_mentions)
    )

    # Get current period data
    current_data = {
        (token_id, symbol, network): count
        for token_id, symbol, network, count in current_query
    }

    # If no tokens in current period, return empty result
    if not current_data:
        return {
            "current_period": f"{mid_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "comparison_period": f"{start_date.strftime('%Y-%m-%d')} to {mid_date.strftime('%Y-%m-%d')}",
            "network_filter": blockchain_network,
            "trending_tokens": []
        }

    # Get token IDs from current period
    token_ids = [token_id for (token_id, _, _) in current_data.keys()]

    # Get token mentions for comparison period (only for tokens in current period)
    previous_query = (
        db.query(
            BlockchainToken.id,
            func.count(TokenMention.id).label("mention_count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .filter(BlockchainToken.id.in_(token_ids))
        .filter(Tweet.created_at.between(start_date, mid_date))
        .group_by(BlockchainToken.id)
    )

    # Get previous period data
    previous_data = {
        token_id: count for token_id, count in previous_query
    }

    # Calculate growth for each token
    trending_tokens = []

    for (token_id, symbol, network), current_count in current_data.items():
        previous_count = previous_data.get(token_id, 0)

        # Calculate growth metrics
        if previous_count > 0:
            percentage_growth = ((current_count - previous_count) / previous_count) * 100
        else:
            # If no previous mentions, use a high value to indicate new tokens
            percentage_growth = 1000  # 1000% growth for new tokens

        absolute_growth = current_count - previous_count

        # Get sentiment data for current period
        sentiment_query = (
            db.query(
                SentimentAnalysis.sentiment,
                func.count(SentimentAnalysis.id).label("count")
            )
            .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
            .join(TokenMention, TokenMention.tweet_id == Tweet.id)
            .filter(TokenMention.token_id == token_id)
            .filter(Tweet.created_at.between(mid_date, end_date))
            .group_by(SentimentAnalysis.sentiment)
        )

        sentiment_counts = {sentiment.value: 0 for sentiment in SentimentEnum}
        for sentiment, count in sentiment_query:
            sentiment_counts[sentiment.value] = count

        total_sentiment = sum(sentiment_counts.values())

        # Calculate sentiment score
        if total_sentiment > 0:
            positive = sentiment_counts["positive"]
            negative = sentiment_counts["negative"]
            sentiment_score = round((positive - negative) / total_sentiment, 2)
        else:
            sentiment_score = 0

        # Add to trending tokens list
        trending_tokens.append({
            "token_id": token_id,
            "symbol": symbol,
            "blockchain_network": network,
            "display_name": f"{symbol} ({network})" if network else symbol,
            "current_mentions": current_count,
            "previous_mentions": previous_count,
            "absolute_growth": absolute_growth,
            "percentage_growth": round(percentage_growth, 2),
            "sentiment_score": sentiment_score,
            "sentiment_breakdown": {
                sentiment: {
                    "count": count,
                    "percentage": round((count / total_sentiment) * 100, 2) if total_sentiment > 0 else 0
                }
                for sentiment, count in sentiment_counts.items()
            }
        })

    # Sort tokens by growth percentage
    sorted_tokens = sorted(
        trending_tokens,
        key=lambda token: token["percentage_growth"],
        reverse=True
    )

    # Apply limit
    trending_tokens = sorted_tokens[:limit]

    # Return result
    return {
        "current_period": f"{mid_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "comparison_period": f"{start_date.strftime('%Y-%m-%d')} to {mid_date.strftime('%Y-%m-%d')}",
        "network_filter": blockchain_network,
        "trending_tokens": trending_tokens
    }


def find_correlated_network_sentiments(
        db: Session,
        days_back: int = 30,
        interval: str = "day",
        correlation_threshold: float = 0.5) -> Dict:
    """
    Find correlations between sentiment trends of different blockchain networks

    Args:
        db: Database session
        days_back: Number of days to look back
        interval: Time interval for correlation calculation ('day', 'week')
        correlation_threshold: Minimum correlation coefficient to include in results

    Returns:
        Dictionary with network correlation data
    """
    # Validate inputs
    valid_intervals = ["day", "week"]
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")

    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    if correlation_threshold < 0 or correlation_threshold > 1:
        raise ValueError("correlation_threshold must be between 0 and 1")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)

    # Get sentiment timeline for each network
    network_query = (
        db.query(
            BlockchainToken.blockchain_network,
            func.date_trunc(interval, Tweet.created_at).label('time_bucket'),
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count")
        )
        .join(TokenMention, TokenMention.token_id == BlockchainToken.id)
        .join(Tweet, Tweet.id == TokenMention.tweet_id)
        .join(SentimentAnalysis, SentimentAnalysis.tweet_id == Tweet.id)
        .filter(BlockchainToken.blockchain_network != None)
        .filter(Tweet.created_at.between(start_date, end_date))
        .group_by(
            BlockchainToken.blockchain_network,
            'time_bucket',
            SentimentAnalysis.sentiment
        )
        .order_by(
            BlockchainToken.blockchain_network,
            'time_bucket'
        )
    )

    # Calculate sentiment scores for each network and time bucket
    network_sentiment_scores = {}

    for network, time_bucket, sentiment, count in network_query:
        # Initialize network if not exists
        if network not in network_sentiment_scores:
            network_sentiment_scores[network] = {}

        # Initialize time bucket if not exists
        time_key = time_bucket.strftime('%Y-%m-%d')
        if time_key not in network_sentiment_scores[network]:
            network_sentiment_scores[network][time_key] = {
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "total": 0,
                "score": 0
            }

        # Add counts
        sentiment_key = sentiment.value.lower()
        network_sentiment_scores[network][time_key][sentiment_key] = count
        network_sentiment_scores[network][time_key]["total"] += count

    # Calculate sentiment score for each time bucket
    for network, timeline in network_sentiment_scores.items():
        for time_key, data in timeline.items():
            if data["total"] > 0:
                # Calculate sentiment score: (positive - negative) / total
                data["score"] = (data["positive"] - data["negative"]) / data["total"]

    # Filter networks with enough data points
    min_data_points = 5
    valid_networks = [
        network for network, timeline in network_sentiment_scores.items()
        if len(timeline) >= min_data_points
    ]

    # Calculate correlations between networks
    network_correlations = []

    if len(valid_networks) >= 2:

        for i in range(len(valid_networks)):
            for j in range(i + 1, len(valid_networks)):
                network1 = valid_networks[i]
                network2 = valid_networks[j]

                # Get common time points
                common_times = set(network_sentiment_scores[network1].keys()) & set(
                    network_sentiment_scores[network2].keys())

                if len(common_times) >= min_data_points:
                    # Create arrays of sentiment scores for common time points
                    scores1 = [network_sentiment_scores[network1][t]["score"] for t in common_times]
                    scores2 = [network_sentiment_scores[network2][t]["score"] for t in common_times]

                    try:
                        # Calculate Pearson correlation
                        correlation, p_value = pearsonr(scores1, scores2)

                        # Only include significant correlations above threshold
                        if abs(correlation) >= correlation_threshold and p_value < 0.05:
                            network_correlations.append({
                                "network1": network1,
                                "network2": network2,
                                "correlation": round(correlation, 3),
                                "p_value": round(p_value, 3),
                                "common_data_points": len(common_times),
                                "direction": "positive" if correlation > 0 else "negative"
                            })
                    except:
                        # Skip if correlation calculation fails
                        pass

    # Sort correlations by strength
    network_correlations = sorted(
        network_correlations,
        key=lambda x: abs(x["correlation"]),
        reverse=True
    )

    # Return result
    return {
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "interval": interval,
        "threshold": correlation_threshold,
        "networks_analyzed": valid_networks,
        "correlations": network_correlations
    }


def analyze_sentiment_seasonality(
        db: Session,
        token_symbol: str = None,
        blockchain_network: str = None,
        weeks_back: int = 12
) -> Dict:
    """
    Analyze sentiment seasonality patterns for a token or network

    Args:
        db: Database session
        token_symbol: Optional token symbol to analyze
        blockchain_network: Optional blockchain network to analyze
        weeks_back: Number of weeks to look back

    Returns:
        Dictionary with seasonality data
    """
    # Validate inputs
    if not token_symbol and not blockchain_network:
        raise ValueError("Must provide either token_symbol or blockchain_network")

    if weeks_back <= 0:
        raise ValueError("weeks_back must be a positive integer")

    # Check token exists if provided
    if token_symbol:
        token_query = db.query(BlockchainToken).filter(BlockchainToken.symbol == token_symbol)
        if blockchain_network:
            token_query = token_query.filter(BlockchainToken.blockchain_network == blockchain_network)

        if not token_query.first():
            error_msg = f"Token with symbol '{token_symbol}'"
            if blockchain_network:
                error_msg += f" on network '{blockchain_network}'"
            error_msg += " not found"
            raise ValueError(error_msg)

    # Check network exists if provided
    if blockchain_network and not token_symbol:
        network = db.query(BlockchainNetwork).filter(BlockchainNetwork.name == blockchain_network).first()
        if not network:
            raise ValueError(f"Blockchain network '{blockchain_network}' not found")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(weeks=weeks_back)

    # Create base query
    query = (
        db.query(
            func.extract('dow', Tweet.created_at).label('day_of_week'),
            func.extract('hour', Tweet.created_at).label('hour_of_day'),
            SentimentAnalysis.sentiment,
            func.count(SentimentAnalysis.id).label("count")
        )
        .join(Tweet, Tweet.id == SentimentAnalysis.tweet_id)
        .join(TokenMention, TokenMention.tweet_id == Tweet.id)
        .join(BlockchainToken, BlockchainToken.id == TokenMention.token_id)
        .filter(Tweet.created_at.between(start_date, end_date))
    )

    # Apply filters
    if token_symbol:
        query = query.filter(BlockchainToken.symbol == token_symbol)

    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    # Group by dimensions
    query = query.group_by(
        'day_of_week',
        'hour_of_day',
        SentimentAnalysis.sentiment
    )

    # Execute query
    results = query.all()

    # Process results
    hourly_patterns = {}
    daily_patterns = {}

    for day, hour, sentiment, count in results:
        day = int(day)
        hour = int(hour)
        sentiment_key = sentiment.value.lower()

        # Initialize day record if not exists
        if day not in daily_patterns:
            daily_patterns[day] = {
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "total": 0
            }

        # Initialize hour record if not exists
        hour_key = f"{day}_{hour}"
        if hour_key not in hourly_patterns:
            hourly_patterns[hour_key] = {
                "day": day,
                "hour": hour,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "total": 0
            }

        # Add counts
        daily_patterns[day][sentiment_key] += count
        daily_patterns[day]["total"] += count

        hourly_patterns[hour_key][sentiment_key] += count
        hourly_patterns[hour_key]["total"] += count

    # Calculate sentiment scores
    for day, data in daily_patterns.items():
        if data["total"] > 0:
            data["positive_pct"] = round((data["positive"] / data["total"]) * 100, 2)
            data["negative_pct"] = round((data["negative"] / data["total"]) * 100, 2)
            data["neutral_pct"] = round((data["neutral"] / data["total"]) * 100, 2)
            data["sentiment_score"] = round((data["positive"] - data["negative"]) / data["total"], 2)

    for hour_key, data in hourly_patterns.items():
        if data["total"] > 0:
            data["positive_pct"] = round((data["positive"] / data["total"]) * 100, 2)
            data["negative_pct"] = round((data["negative"] / data["total"]) * 100, 2)
            data["neutral_pct"] = round((data["neutral"] / data["total"]) * 100, 2)
            data["sentiment_score"] = round((data["positive"] - data["negative"]) / data["total"], 2)

    # Convert to lists and sort
    daily_data = [
        {
            "day": day,
            "day_name": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][day % 7],
            **data
        }
        for day, data in daily_patterns.items()
    ]
    daily_data.sort(key=lambda x: x["day"])

    hourly_data = list(hourly_patterns.values())
    hourly_data.sort(key=lambda x: (x["day"], x["hour"]))

    # Identify peak and low times
    if daily_data:
        peak_day = max(daily_data, key=lambda x: x["sentiment_score"])
        low_day = min(daily_data, key=lambda x: x["sentiment_score"])
    else:
        peak_day = low_day = None

    if hourly_data:
        peak_hour = max(hourly_data, key=lambda x: x["sentiment_score"])
        low_hour = min(hourly_data, key=lambda x: x["sentiment_score"])
    else:
        peak_hour = low_hour = None

    # Format target info
    target_info = {}
    if token_symbol:
        target_info["token_symbol"] = token_symbol
    if blockchain_network:
        target_info["blockchain_network"] = blockchain_network

    # Return seasonality data
    return {
        "target": target_info,
        "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        "daily_patterns": daily_data,
        "hourly_patterns": hourly_data,
        "peak_sentiment": {
            "day": peak_day,
            "hour": peak_hour
        },
        "low_sentiment": {
            "day": low_day,
            "hour": low_hour
        }
    }


def analyze_token_for_network_detection(
        db: Session,
        token_id: int,
        min_confidence: float = 0.5
) -> Dict[str, Any]:
    """
    Advanced analysis of a token to detect its most likely blockchain network.
    Uses context from tweets, common patterns, and historical data.

    Args:
        db: Database session
        token_id: ID of the token to analyze
        min_confidence: Minimum confidence threshold to suggest a network

    Returns:
        Dictionary with detected networks and confidence scores:
        {
            "token_id": int,
            "token_symbol": str,
            "detected_networks": [
                {"network": str, "confidence": float, "evidence": list}
            ],
            "recommended_network": {"id": int, "name": str},
            "confidence_score": float,
            "needs_manual_review": bool
        }
    """
    # Get token details
    token = get_blockchain_token_by_id(db, token_id)
    if not token:
        raise ValueError(f"Token with ID {token_id} not found")

    # Get mentions of this token
    mentions = db.query(TokenMention).filter(TokenMention.token_id == token_id).all()
    if not mentions:
        return {
            "token_id": token.id,
            "token_symbol": token.symbol,
            "detected_networks": [],
            "recommended_network": None,
            "confidence_score": 0.0,
            "needs_manual_review": True,
            "reason": "No mentions found for analysis"
        }

    # Get tweet IDs
    tweet_ids = [mention.tweet_id for mention in mentions]

    # Get tweets
    tweets = db.query(Tweet).filter(Tweet.id.in_(tweet_ids)).all()

    # Get all blockchain networks
    networks = db.query(BlockchainNetwork).filter(BlockchainNetwork.is_active == True).all()

    # Initialize network scores
    network_scores = {}
    evidence = {}

    for network in networks:
        network_scores[network.name] = 0.0
        evidence[network.name] = []

    # Analyze tweets for network indicators
    for tweet in tweets:
        tweet_text = tweet.text.lower()

        # Check tweet text for network mentions
        for network in networks:
            # Direct network name mention
            if network.name.lower() in tweet_text:
                network_scores[network.name] += 0.3
                evidence[network.name].append({
                    "tweet_id": tweet.id,
                    "type": "direct_mention",
                    "text": tweet.text,
                    "weight": 0.3
                })

            # Check for display name mention
            if network.display_name and network.display_name.lower() in tweet_text:
                network_scores[network.name] += 0.3
                evidence[network.name].append({
                    "tweet_id": tweet.id,
                    "type": "display_name_mention",
                    "text": tweet.text,
                    "weight": 0.3
                })

            # Check for hashtags
            if network.hashtags:
                for hashtag in network.hashtags:
                    if f"#{hashtag.lower()}" in tweet_text:
                        network_scores[network.name] += 0.4
                        evidence[network.name].append({
                            "tweet_id": tweet.id,
                            "type": "hashtag",
                            "text": f"#{hashtag}",
                            "weight": 0.4
                        })

            # Check for keywords
            if network.keywords:
                for keyword in network.keywords:
                    if keyword.lower() in tweet_text:
                        network_scores[network.name] += 0.2
                        evidence[network.name].append({
                            "tweet_id": tweet.id,
                            "type": "keyword",
                            "text": keyword,
                            "weight": 0.2
                        })

    # Analyze token symbol for network-specific patterns
    token_symbol = token.symbol.lower()
    token_address = token.token_address.lower()

    # Common Ethereum token patterns
    if token_address.startswith("0x") or "erc" in token_symbol:
        network_scores["ethereum"] = network_scores.get("ethereum", 0) + 0.4
        evidence["ethereum"].append({
            "type": "address_pattern",
            "text": f"Address starts with 0x: {token_address}",
            "weight": 0.4
        })

    # Common Solana token patterns
    if len(token_symbol) <= 5 and token_symbol.isupper():
        network_scores["solana"] = network_scores.get("solana", 0) + 0.2
        evidence["solana"].append({
            "type": "symbol_pattern",
            "text": f"Short uppercase symbol: {token.symbol}",
            "weight": 0.2
        })

    # Normalize scores based on evidence count
    for network_name in network_scores:
        if len(evidence[network_name]) > 0:
            # Average evidence weight
            avg_weight = sum(e["weight"] for e in evidence[network_name]) / len(evidence[network_name])
            # Apply diminishing returns for large numbers of evidence
            evidence_factor = min(1.0, math.log10(len(evidence[network_name]) + 1) / math.log10(11))
            # Calculate final score
            network_scores[network_name] = min(1.0, avg_weight * evidence_factor)

    # Find the highest scoring network
    best_network = None
    highest_score = 0.0

    for network_name, score in network_scores.items():
        if score > highest_score:
            highest_score = score
            # Get network ID for the recommendation
            network = next((n for n in networks if n.name == network_name), None)
            if network:
                best_network = {"id": network.id, "name": network_name}

    # Determine if manual review is needed
    needs_manual_review = highest_score < min_confidence

    # Return results
    detected_networks = [
        {"network": network_name, "confidence": score, "evidence": evidence[network_name]}
        for network_name, score in network_scores.items()
        if score > 0
    ]

    # Sort detected networks by confidence score (descending)
    detected_networks.sort(key=lambda x: x["confidence"], reverse=True)

    return {
        "token_id": token.id,
        "token_symbol": token.symbol,
        "detected_networks": detected_networks,
        "recommended_network": best_network,
        "confidence_score": highest_score,
        "needs_manual_review": needs_manual_review,
        "reason": "Low confidence score" if needs_manual_review and best_network else "No clear network detected"
    }
