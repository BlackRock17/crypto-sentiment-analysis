"""
Twitter API endpoints for Solana Sentiment Analysis with support for multiple blockchain networks.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date

from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks, Path, Body, status
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets, add_manual_tweet
from src.security.auth import get_current_superuser, get_current_active_user
from src.data_processing.models.auth import User
from src.data_processing.models.twitter import TwitterInfluencer, TwitterApiUsage
from src.data_processing.models.database import BlockchainNetwork, BlockchainToken
from src.data_collection.twitter.config import (
    twitter_config, CollectionFrequency, get_collection_frequency_hours
)
from src.data_collection.twitter.repository import TwitterRepository
from src.schemas.twitter import (
    InfluencerCreate, InfluencerUpdate, InfluencerResponse,
    ManualTweetCreate, TweetResponse, ApiUsageResponse,
    TwitterSettingsUpdate, TwitterSettingsResponse,
    # New schema imports for blockchain networks
    BlockchainNetworkCreate, BlockchainNetworkUpdate, BlockchainNetworkResponse,
    BlockchainTokenCreate, BlockchainTokenUpdate, BlockchainTokenResponse,
    TokenReviewAction
)
from src.data_processing.crud.twitter import (
    create_influencer, get_influencer, get_all_influencers,
    update_influencer, delete_influencer, toggle_influencer_automation,
    get_api_usage_stats, get_api_usage_history, get_influencer_by_username
)
from src.data_processing.crud.read import (
    get_blockchain_network_by_id, get_blockchain_network_by_name,
    get_all_blockchain_networks, get_blockchain_token_by_id,
    get_tokens_needing_review, get_all_blockchain_tokens
)
from src.data_processing.crud.create import (
    create_blockchain_network, create_blockchain_token
)
from src.data_processing.crud.update import (
    update_blockchain_network, update_blockchain_token,
    update_token_blockchain_network, mark_token_as_verified,
    merge_duplicate_tokens
)
from src.data_processing.crud.delete import (
    delete_blockchain_network, delete_blockchain_token,
    delete_blockchain_token_cascade
)
from src.exceptions import (
    BadRequestException, NotFoundException, ServerErrorException
)
from src.api.utils import enhance_token_response

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/twitter", tags=["Twitter"])


@router.post("/collect", status_code=202)
async def run_twitter_collection(
        background_tasks: BackgroundTasks,
        current_user: User = Depends(get_current_superuser)
):
    """
    Manually trigger collection of tweets from automated influencers.
    Requires superuser (admin) privileges.

    Args:
        background_tasks: FastAPI background tasks
        current_user: Current authenticated user (must be admin)

    Returns:
        Status message
    """
    logger.info(f"Manual tweet collection triggered by user {current_user.username}")

    # Run collection in background
    background_tasks.add_task(collect_automated_tweets)

    return {
        "status": "accepted",
        "message": "Tweet collection started in background"
    }


@router.get("/status")
async def get_twitter_status(
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get status of Twitter integration.

    Args:
        current_user: Current authenticated user
        db: Database session

    Returns:
        Status information
    """
    from src.data_collection.twitter.client import TwitterAPIClient
    from src.data_processing.models.database import Tweet, TokenMention

    # Create client to test connection
    client = TwitterAPIClient()
    connection_ok = client.test_connection()

    # Get statistics from database
    tweet_count = db.query(Tweet).count()
    mention_count = db.query(TokenMention).count()
    newest_tweet = db.query(Tweet).order_by(Tweet.created_at.desc()).first()

    # Get automated influencers count
    automated_count = db.query(TwitterInfluencer).filter(
        TwitterInfluencer.is_active == True,
        TwitterInfluencer.is_automated == True
    ).count()

    # Get influencer stats
    total_influencers = db.query(TwitterInfluencer).count()

    # Get timestamp of newest tweet
    newest_tweet_time = newest_tweet.created_at if newest_tweet else None

    # Get collection frequency
    hours = get_collection_frequency_hours(twitter_config.collection_frequency)

    return {
        "twitter_connection": "ok" if connection_ok else "error",
        "stored_tweets": tweet_count,
        "token_mentions": mention_count,
        "newest_tweet": newest_tweet_time,
        "collection_frequency": f"every {hours} hours",
        "influencers": {
            "total": total_influencers,
            "automated": automated_count,
            "max_automated": twitter_config.max_automated_influencers
        }
    }


@router.get("/api-usage", response_model=ApiUsageResponse)
async def get_api_usage(
        date: Optional[date] = None,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get Twitter API usage statistics.

    Args:
        date: Date to get usage for (defaults to today)
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        API usage statistics
    """
    stats = get_api_usage_stats(db, date)

    # Calculate remaining requests
    daily_limit = twitter_config.daily_request_limit
    monthly_limit = twitter_config.monthly_request_limit

    remaining_daily = max(0, daily_limit - stats["daily_usage"])
    remaining_monthly = max(0, monthly_limit - stats["monthly_usage"])

    return {
        "date": stats["date"],
        "daily_usage": stats["daily_usage"],
        "monthly_usage": stats["monthly_usage"],
        "monthly_limit": monthly_limit,
        "daily_limit": daily_limit,
        "remaining_daily": remaining_daily,
        "remaining_monthly": remaining_monthly,
        "influencer_usage": stats["influencer_usage"]
    }


@router.get("/api-usage/history")
async def get_api_usage_history_endpoint(
        days: int = Query(30, gt=0, lt=366),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get historical Twitter API usage data.

    Args:
        days: Number of days to look back
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Historical API usage data
    """
    return get_api_usage_history(db, days)


@router.get("/settings", response_model=TwitterSettingsResponse)
async def get_twitter_settings(
        current_user: User = Depends(get_current_superuser)
):
    """
    Get Twitter API settings.

    Args:
        current_user: Current authenticated user (must be admin)

    Returns:
        Twitter settings
    """
    return {
        "max_automated_influencers": twitter_config.max_automated_influencers,
        "collection_frequency": twitter_config.collection_frequency.value,
        "collection_interval_hours": get_collection_frequency_hours(twitter_config.collection_frequency),
        "max_tweets_per_user": twitter_config.max_tweets_per_user,
        "monthly_request_limit": twitter_config.monthly_request_limit,
        "daily_request_limit": twitter_config.daily_request_limit
    }


@router.put("/settings", response_model=TwitterSettingsResponse)
async def update_twitter_settings(
        settings: TwitterSettingsUpdate,
        current_user: User = Depends(get_current_superuser)
):
    """
    Update Twitter API settings.

    Args:
        settings: Settings to update
        current_user: Current authenticated user (must be admin)

    Returns:
        Updated Twitter settings
    """
    # Update settings
    if settings.max_automated_influencers is not None:
        twitter_config.max_automated_influencers = settings.max_automated_influencers

    if settings.collection_frequency is not None:
        twitter_config.collection_frequency = settings.collection_frequency

    if settings.max_tweets_per_user is not None:
        twitter_config.max_tweets_per_user = settings.max_tweets_per_user

    if settings.daily_request_limit is not None:
        twitter_config.daily_request_limit = settings.daily_request_limit

    # Return updated settings
    return {
        "max_automated_influencers": twitter_config.max_automated_influencers,
        "collection_frequency": twitter_config.collection_frequency.value,
        "collection_interval_hours": get_collection_frequency_hours(twitter_config.collection_frequency),
        "max_tweets_per_user": twitter_config.max_tweets_per_user,
        "monthly_request_limit": twitter_config.monthly_request_limit,
        "daily_request_limit": twitter_config.daily_request_limit
    }


@router.get("/influencers", response_model=List[InfluencerResponse])
async def get_influencers_list(
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False,
        automated_only: bool = False,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get list of Twitter influencers.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        active_only: Only return active influencers
        automated_only: Only return automated influencers
        current_user: Current authenticated user
        db: Database session

    Returns:
        List of influencers
    """
    return get_all_influencers(db, skip, limit, active_only, automated_only)


@router.post("/influencers", response_model=InfluencerResponse, status_code=status.HTTP_201_CREATED)
async def create_influencer_endpoint(
        influencer: InfluencerCreate,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Create a new Twitter influencer.

    Args:
        influencer: Influencer data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Created influencer
    """
    # Check if influencer with this username already exists
    existing = get_influencer_by_username(db, influencer.username)
    if existing:
        raise BadRequestException(f"Influencer with username '{influencer.username}' already exists")

    return create_influencer(
        db=db,
        username=influencer.username,
        name=influencer.name,
        description=influencer.description,
        follower_count=influencer.follower_count,
        is_active=influencer.is_active,
        is_automated=influencer.is_automated,
        priority=influencer.priority
    )


@router.get("/influencers/{influencer_id}", response_model=InfluencerResponse)
async def get_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get a Twitter influencer by ID.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Influencer data
    """
    influencer = get_influencer(db, influencer_id)
    if not influencer:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    return influencer


@router.put("/influencers/{influencer_id}", response_model=InfluencerResponse)
async def update_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        influencer_data: InfluencerUpdate = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Update a Twitter influencer.

    Args:
        influencer_id: Influencer ID
        influencer_data: Updated influencer data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated influencer
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # If username is being updated, check for uniqueness
    if influencer_data.username and influencer_data.username != existing.username:
        username_exists = get_influencer_by_username(db, influencer_data.username)
        if username_exists:
            raise BadRequestException(f"Influencer with username '{influencer_data.username}' already exists")

    # Update influencer
    updated = update_influencer(db, influencer_id, **influencer_data.model_dump(exclude_unset=True))
    if not updated:
        raise ServerErrorException("Failed to update influencer")

    return updated


@router.delete("/influencers/{influencer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Delete a Twitter influencer.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user (must be admin)
        db: Database session
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # Delete influencer
    success = delete_influencer(db, influencer_id)
    if not success:
        raise ServerErrorException("Failed to delete influencer")

    return None


@router.post("/influencers/{influencer_id}/toggle-automation", response_model=InfluencerResponse)
async def toggle_influencer_automation_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Toggle whether an influencer is automatically tracked.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated influencer
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # Toggle automation
    updated = toggle_influencer_automation(db, influencer_id)
    if not updated:
        raise ServerErrorException("Failed to toggle influencer automation")

    return updated


@router.post("/manual-tweets", response_model=TweetResponse)
async def add_manual_tweet_endpoint(
        tweet: ManualTweetCreate,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Manually add a tweet for analysis.

    Args:
        tweet: Tweet data
        background_tasks: FastAPI background tasks
        current_user: Current authenticated user
        db: Database session

    Returns:
        Status of the operation
    """
    logger.info(f"Manual tweet addition by {current_user.username} for influencer {tweet.influencer_username}")

    # Run in foreground for API response
    from src.data_collection.twitter.service import TwitterCollectionService

    service = TwitterCollectionService(db)
    stored_tweet, mentions_count = service.add_manual_tweet(
        influencer_username=tweet.influencer_username,
        tweet_text=tweet.text,
        created_at=tweet.created_at,
        tweet_id=tweet.tweet_id,
        retweet_count=tweet.retweet_count,
        like_count=tweet.like_count
    )

    if not stored_tweet:
        raise ServerErrorException("Failed to add manual tweet")

    # Get tweet with mentions
    repository = TwitterRepository(db)
    tweet_data = repository.get_tweet_with_mentions(stored_tweet.id)

    if not tweet_data:
        raise ServerErrorException("Failed to retrieve stored tweet")

    return tweet_data


# New endpoints for blockchain networks

@router.get("/networks", response_model=List[BlockchainNetworkResponse])
async def get_blockchain_networks(
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get list of blockchain networks.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        active_only: Only return active networks
        current_user: Current authenticated user
        db: Database session

    Returns:
        List of blockchain networks
    """
    return get_all_blockchain_networks(db, skip, limit, active_only)


@router.post("/networks", response_model=BlockchainNetworkResponse, status_code=status.HTTP_201_CREATED)
async def create_blockchain_network_endpoint(
        network: BlockchainNetworkCreate,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Create a new blockchain network.

    Args:
        network: Network data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Created blockchain network
    """
    # Check if network with this name already exists
    existing = get_blockchain_network_by_name(db, network.name)
    if existing:
        raise BadRequestException(f"Network with name '{network.name}' already exists")

    return create_blockchain_network(
        db=db,
        name=network.name,
        display_name=network.display_name,
        description=network.description,
        hashtags=network.hashtags,
        keywords=network.keywords,
        icon_url=network.icon_url,
        is_active=network.is_active,
        website_url=network.website_url,
        explorer_url=network.explorer_url,
        launch_date=network.launch_date
    )


@router.get("/networks/{network_id}", response_model=BlockchainNetworkResponse)
async def get_blockchain_network_endpoint(
        network_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get a blockchain network by ID.

    Args:
        network_id: Network ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Network data
    """
    network = get_blockchain_network_by_id(db, network_id)
    if not network:
        raise NotFoundException(f"Network with ID {network_id} not found")

    return network


@router.put("/networks/{network_id}", response_model=BlockchainNetworkResponse)
async def update_blockchain_network_endpoint(
        network_id: int = Path(..., gt=0),
        network_data: BlockchainNetworkUpdate = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Update a blockchain network.

    Args:
        network_id: Network ID
        network_data: Updated network data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated network
    """
    # Check if network exists
    existing = get_blockchain_network_by_id(db, network_id)
    if not existing:
        raise NotFoundException(f"Network with ID {network_id} not found")

    # If name is being updated, check for uniqueness
    if network_data.name and network_data.name != existing.name:
        name_exists = get_blockchain_network_by_name(db, network_data.name)
        if name_exists:
            raise BadRequestException(f"Network with name '{network_data.name}' already exists")

    # Update network
    updated = update_blockchain_network(
        db=db,
        network_id=network_id,
        **network_data.model_dump(exclude_unset=True)
    )

    if not updated:
        raise ServerErrorException("Failed to update network")

    return updated


@router.delete("/networks/{network_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blockchain_network_endpoint(
        network_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Delete a blockchain network.

    Args:
        network_id: Network ID
        current_user: Current authenticated user (must be admin)
        db: Database session
    """
    # Check if network exists
    existing = get_blockchain_network_by_id(db, network_id)
    if not existing:
        raise NotFoundException(f"Network with ID {network_id} not found")

    # Delete network
    try:
        success = delete_blockchain_network(db, network_id)
        if not success:
            raise ServerErrorException("Failed to delete network")
    except ValueError as e:
        raise BadRequestException(str(e))

    return None


# Endpoints for token management

@router.get("/tokens", response_model=List[BlockchainTokenResponse])
async def get_blockchain_tokens(
        skip: int = 0,
        limit: int = 100,
        symbol_filter: Optional[str] = None,
        network_filter: Optional[str] = None,
        needs_review: Optional[bool] = None,
        manually_verified: Optional[bool] = None,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get list of blockchain tokens with optional filtering.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        symbol_filter: Filter by token symbol
        network_filter: Filter by blockchain network
        needs_review: Filter by needs_review flag
        manually_verified: Filter by manually_verified flag
        current_user: Current authenticated user
        db: Database session

    Returns:
        List of blockchain tokens
    """
    tokens = get_all_blockchain_tokens(
        db=db,
        skip=skip,
        limit=limit,
        symbol_filter=symbol_filter,
        blockchain_network=network_filter,
        needs_review=needs_review,
        manually_verified=manually_verified
    )

    # Enhance all tokens with network information
    return [enhance_token_response(token, db) for token in tokens]


@router.post("/tokens", response_model=BlockchainTokenResponse, status_code=status.HTTP_201_CREATED)
async def create_blockchain_token_endpoint(
        token: BlockchainTokenCreate,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Create a new blockchain token.

    Args:
        token: Token data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Created blockchain token
    """
    from src.data_processing.crud.read import get_blockchain_token_by_address

    # Check if token with same address and network already exists
    existing = get_blockchain_token_by_address(db, token.token_address, token.blockchain_network)
    if existing:
        raise BadRequestException(
            f"Token with address '{token.token_address}' already exists on network '{token.blockchain_network}'")

    # Validate network if provided
    if token.blockchain_network:
        network = get_blockchain_network_by_name(db, token.blockchain_network)
        if not network:
            raise BadRequestException(f"Blockchain network '{token.blockchain_network}' does not exist")

    # Get blockchain_network_id if network is provided
    blockchain_network_id = None
    if token.blockchain_network:
        network = get_blockchain_network_by_name(db, token.blockchain_network)
        if network:
            blockchain_network_id = network.id

    created_token = create_blockchain_token(
        db=db,
        token_address=token.token_address,
        symbol=token.symbol,
        name=token.name,
        blockchain_network=token.blockchain_network,
        network_confidence=token.network_confidence or 1.0,  # Default to 1.0 for manually added tokens
        manually_verified=True,  # Manually added tokens are verified by default
        needs_review=False,  # Manually added tokens don't need review
        blockchain_network_id=blockchain_network_id
    )

    # Enhance token with network information
    return enhance_token_response(created_token, db)


@router.get("/tokens/{token_id}", response_model=BlockchainTokenResponse)
async def get_blockchain_token_endpoint(
        token_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get a blockchain token by ID.

    Args:
        token_id: Token ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Token data
    """
    token = get_blockchain_token_by_id(db, token_id)
    if not token:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Enhance token with network information
    return enhance_token_response(token, db)


@router.put("/tokens/{token_id}", response_model=BlockchainTokenResponse)
async def update_blockchain_token_endpoint(
        token_id: int = Path(..., gt=0),
        token_data: BlockchainTokenUpdate = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Update a blockchain token.

    Args:
        token_id: Token ID
        token_data: Updated token data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated token
    """
    # Check if token exists
    existing = get_blockchain_token_by_id(db, token_id)
    if not existing:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Update token
    updated = update_blockchain_token(
        db=db,
        token_id=token_id,
        **token_data.model_dump(exclude_unset=True)
    )

    if not updated:
        raise ServerErrorException("Failed to update token")

    # Enhance token with network information
    return enhance_token_response(updated, db)


@router.post("/tokens/{token_id}/verify", response_model=BlockchainTokenResponse)
async def verify_blockchain_token_endpoint(
        token_id: int = Path(..., gt=0),
        verified: bool = Query(True),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Mark a blockchain token as verified or unverified.

    Args:
        token_id: Token ID
        verified: Whether the token is verified
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated token
    """
    # Check if token exists
    existing = get_blockchain_token_by_id(db, token_id)
    if not existing:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Mark token as verified/unverified
    updated = mark_token_as_verified(
        db=db,
        token_id=token_id,
        verified=verified,
        needs_review=False  # Once reviewed, it no longer needs review
    )

    if not updated:
        raise ServerErrorException("Failed to update token verification status")

    # Enhance token with network information
    return enhance_token_response(updated, db)


@router.post("/tokens/{token_id}/set-network", response_model=BlockchainTokenResponse)
async def set_token_network_endpoint(
        token_id: int = Path(..., gt=0),
        network_id: int = Query(..., gt=0),
        confidence: float = Query(1.0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Set the blockchain network for a token.

    Args:
        token_id: Token ID
        network_id: Blockchain network ID
        confidence: Confidence level in network determination
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated token
    """
    # Check if token exists
    token = get_blockchain_token_by_id(db, token_id)
    if not token:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Check if network exists
    network = get_blockchain_network_by_id(db, network_id)
    if not network:
        raise NotFoundException(f"Network with ID {network_id} not found")

    # Update token network
    try:
        updated = update_token_blockchain_network(
            db=db,
            token_id=token_id,
            blockchain_network_id=network_id,
            confidence=confidence,
            manually_verified=True,
            needs_review=False
        )

        if not updated:
            raise ServerErrorException("Failed to update token network")

        # Enhance token with network information
        return enhance_token_response(updated, db)
    except ValueError as e:
        raise BadRequestException(str(e))


@router.delete("/tokens/{token_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blockchain_token_endpoint(
        token_id: int = Path(..., gt=0),
        force: bool = Query(False),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Delete a blockchain token.

    Args:
        token_id: Token ID
        force: Whether to force deletion even if token has mentions
        current_user: Current authenticated user (must be admin)
        db: Database session
    """
    # Check if token exists
    existing = get_blockchain_token_by_id(db, token_id)
    if not existing:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Delete token
    try:
        if force:
            # Use cascade delete if force is True
            success = delete_blockchain_token_cascade(db, token_id)
        else:
            # Normal delete with check_mentions=True
            success = delete_blockchain_token(db, token_id, check_mentions=True)

        if not success:
            raise ServerErrorException("Failed to delete token")
    except ValueError as e:
        raise BadRequestException(str(e))

    return None


@router.get("/tokens/review", response_model=List[BlockchainTokenResponse])
async def get_tokens_needing_review_endpoint(
        skip: int = 0,
        limit: int = 100,
        min_confidence: Optional[float] = None,
        max_confidence: Optional[float] = None,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get tokens that need review.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        min_confidence: Minimum confidence level
        max_confidence: Maximum confidence level
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        List of tokens needing review
    """
    tokens = get_tokens_needing_review(
        db=db,
        skip=skip,
        limit=limit,
        min_confidence=min_confidence,
        max_confidence=max_confidence
    )

    # Enhance all tokens with network information
    return [enhance_token_response(token, db) for token in tokens]


@router.post("/tokens/{token_id}/review", response_model=BlockchainTokenResponse)
async def review_token_endpoint(
        token_id: int = Path(..., gt=0),
        action: TokenReviewAction = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Review a token and take an action.

    Args:
        token_id: Token ID
        action: Review action to take
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated token
    """
    # Check if token exists
    token = get_blockchain_token_by_id(db, token_id)
    if not token:
        raise NotFoundException(f"Token with ID {token_id} not found")

    if action.action == "approve_network":
        # Approve the current network
        if not token.blockchain_network_id:
            raise BadRequestException("Token has no blockchain network to approve")

        updated = mark_token_as_verified(
            db=db,
            token_id=token_id,
            verified=True,
            needs_review=False
        )

    elif action.action == "set_network":
        # Set a new network
        if not action.network_id:
            raise BadRequestException("network_id is required for set_network action")

        # Check if network exists
        network = get_blockchain_network_by_id(db, action.network_id)
        if not network:
            raise NotFoundException(f"Network with ID {action.network_id} not found")

        updated = update_token_blockchain_network(
            db=db,
            token_id=token_id,
            blockchain_network_id=action.network_id,
            confidence=1.0,
            manually_verified=True,
            needs_review=False
        )

    elif action.action == "merge":
        # Merge with another token
        if not action.merge_with_id:
            raise BadRequestException("merge_with_id is required for merge action")

        # Check if the other token exists
        other_token = get_blockchain_token_by_id(db, action.merge_with_id)
        if not other_token:
            raise NotFoundException(f"Token with ID {action.merge_with_id} not found")

        try:
            # Merge tokens
            success = merge_duplicate_tokens(
                db=db,
                primary_token_id=action.merge_with_id,
                duplicate_token_id=token_id
            )

            if not success:
                raise ServerErrorException("Failed to merge tokens")

            # Return the primary token since the duplicate is now deleted
            return enhance_token_response(other_token, db)

        except Exception as e:
            raise ServerErrorException(f"Error merging tokens: {e}")

    elif action.action == "reject":
        # Mark for more review later
        updated = update_blockchain_token(
            db=db,
            token_id=token_id,
            needs_review=True
        )

    else:
        raise BadRequestException(f"Unknown action: {action.action}")

    # Enhance token with network information
    return enhance_token_response(updated, db)


@router.get("/tokens/uncategorized", response_model=List[BlockchainTokenResponse])
async def get_uncategorized_tokens(
        skip: int = 0,
        limit: int = 100,
        min_mentions: int = Query(1, description="Minimum number of mentions"),
        sort_by: str = Query("mentions", description="Sort by: mentions, date, confidence"),
        sort_order: str = Query("desc", description="Sort order: asc, desc"),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get tokens that need categorization (blockchain network assignment).
    These are tokens that have no assigned network or have low confidence scores.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        min_mentions: Minimum number of mentions to include a token
        sort_by: Field to sort by (mentions, date, confidence)
        sort_order: Sort order (asc, desc)
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        List of tokens needing categorization
    """
    # Create a query for tokens that need categorization
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
    )

    # Apply sorting
    if sort_by == "mentions":
        query = query.order_by(desc("mention_count") if sort_order == "desc" else "mention_count")
    elif sort_by == "date":
        query = query.order_by(desc(BlockchainToken.created_at) if sort_order == "desc" else BlockchainToken.created_at)
    elif sort_by == "confidence":
        query = query.order_by(
            desc(BlockchainToken.network_confidence) if sort_order == "desc" else BlockchainToken.network_confidence)

    # Apply pagination
    query = query.offset(skip).limit(limit)

    # Execute query
    results = query.all()

    # Format results
    tokens = []
    for token, mention_count in results:
        token_dict = enhance_token_response(token, db)
        token_dict["mention_count"] = mention_count
        tokens.append(token_dict)

    return tokens


@router.get("/tokens/duplicates", response_model=List[Dict[str, Any]])
async def get_potential_duplicate_tokens(
        min_similarity: float = Query(0.8, ge=0, le=1, description="Minimum similarity score"),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get potential duplicate tokens based on symbol similarity.

    Args:
        min_similarity: Minimum similarity score (0-1)
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        List of potential duplicate groups
    """
    # Get all tokens
    tokens = db.query(BlockchainToken).all()

    # Group tokens by normalized symbols
    symbol_groups = {}
    for token in tokens:
        # Normalize symbol (e.g., remove special characters, lowercase)
        normalized_symbol = token.symbol.lower().strip()

        if normalized_symbol not in symbol_groups:
            symbol_groups[normalized_symbol] = []

        symbol_groups[normalized_symbol].append(token)

    # Find groups with multiple tokens
    duplicate_groups = []
    for symbol, token_group in symbol_groups.items():
        if len(token_group) > 1:
            # Enhance tokens with network info and mention count
            enhanced_tokens = []
            for token in token_group:
                # Get mention count
                mention_count = db.query(func.count(TokenMention.id)).filter(
                    TokenMention.token_id == token.id
                ).scalar()

                # Enhance token
                enhanced_token = enhance_token_response(token, db)
                enhanced_token["mention_count"] = mention_count
                enhanced_tokens.append(enhanced_token)

            duplicate_groups.append({
                "symbol": symbol,
                "tokens": enhanced_tokens,
                "total_tokens": len(token_group)
            })

    # Sort groups by number of tokens (descending)
    duplicate_groups.sort(key=lambda g: g["total_tokens"], reverse=True)

    return duplicate_groups


@router.post("/tokens/{token_id}/categorize", response_model=BlockchainTokenResponse)
async def categorize_token(
        token_id: int = Path(..., gt=0),
        network_id: int = Body(..., gt=0),
        confidence: float = Body(1.0, ge=0, le=1),
        notes: Optional[str] = Body(None),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Categorize a token by assigning it to a blockchain network.

    Args:
        token_id: Token ID
        network_id: Blockchain network ID
        confidence: Confidence in the categorization (0-1)
        notes: Optional notes about the categorization
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated token
    """
    # Check if token exists
    token = get_blockchain_token_by_id(db, token_id)
    if not token:
        raise NotFoundException(f"Token with ID {token_id} not found")

    # Check if network exists
    network = get_blockchain_network_by_id(db, network_id)
    if not network:
        raise NotFoundException(f"Network with ID {network_id} not found")

    # Update token with the assigned network
    updated_token = update_token_blockchain_network(
        db=db,
        token_id=token_id,
        blockchain_network_id=network_id,
        confidence=confidence,
        manually_verified=True,  # It's manually verified since an admin is doing it
        needs_review=False  # No longer needs review after categorization
    )

    if not updated_token:
        raise ServerErrorException("Failed to update token")

    # TODO: If you want to keep track of who categorized the token and when,
    # you could add a TokenCategorizationHistory model and record it here

    # Enhance token with network information
    return enhance_token_response(updated_token, db)


@router.post("/tokens/merge", response_model=BlockchainTokenResponse)
async def merge_tokens(
        primary_token_id: int = Body(..., gt=0),
        duplicate_token_ids: List[int] = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Merge duplicate tokens by moving all mentions to the primary token.

    Args:
        primary_token_id: ID of the primary token to keep
        duplicate_token_ids: List of IDs of duplicate tokens to merge into the primary
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated primary token
    """
    # Check if primary token exists
    primary_token = get_blockchain_token_by_id(db, primary_token_id)
    if not primary_token:
        raise NotFoundException(f"Primary token with ID {primary_token_id} not found")

    # Check if all duplicate tokens exist
    for dup_id in duplicate_token_ids:
        if not get_blockchain_token_by_id(db, dup_id):
            raise NotFoundException(f"Duplicate token with ID {dup_id} not found")

    # Merge each duplicate token into the primary
    for dup_id in duplicate_token_ids:
        try:
            # Skip if it's the same as primary token
            if dup_id == primary_token_id:
                continue

            success = merge_duplicate_tokens(db, primary_token_id, dup_id)
            if not success:
                logger.warning(f"Failed to merge token {dup_id} into {primary_token_id}")
        except Exception as e:
            logger.error(f"Error merging token {dup_id}: {e}")

    # Refresh and return the updated primary token
    db.refresh(primary_token)
    return enhance_token_response(primary_token, db)
