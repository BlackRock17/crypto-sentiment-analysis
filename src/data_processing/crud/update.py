from typing import List

from sqlalchemy.orm import Session
from src.data_processing.models.database import BlockchainToken, BlockchainNetwork, Tweet, SentimentEnum, SentimentAnalysis, TokenMention
from datetime import datetime


def update_blockchain_token(
        db: Session,
        token_id: int,
        symbol: str = None,
        name: str = None,
        blockchain_network: str = None,
        blockchain_network_id: int = None,
        network_confidence: float = None,
        manually_verified: bool = None,
        needs_review: bool = None
) -> BlockchainToken:
    """
    Updates a blockchain token record

    Args:
        db: Database session
        token_id: ID of the token to update
        symbol: New token symbol (optional)
        name: New token name (optional)
        blockchain_network: New blockchain network name (optional)
        blockchain_network_id: New blockchain network ID (optional)
        network_confidence: New network confidence value (optional)
        manually_verified: New manual verification status (optional)
        needs_review: New needs review status (optional)

    Returns:
        Updated BlockchainToken record or None if not found
    """
    # Get the token by ID
    db_token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()

    # Return None if the token does not exist
    if db_token is None:
        return None

    # Update fields if provided
    if symbol is not None:
        db_token.symbol = symbol

    if name is not None:
        db_token.name = name

    if blockchain_network is not None:
        db_token.blockchain_network = blockchain_network

    if blockchain_network_id is not None:
        db_token.blockchain_network_id = blockchain_network_id

    if network_confidence is not None:
        # Checking if confidence_score is between 0 and 1
        if not 0 <= network_confidence <= 1:
            raise ValueError("network_confidence must be between 0 and 1")
        db_token.network_confidence = network_confidence

    if manually_verified is not None:
        db_token.manually_verified = manually_verified

    if needs_review is not None:
        db_token.needs_review = needs_review

    # Update timestamp
    db_token.updated_at = datetime.utcnow()

    # Save changes to the database
    db.commit()
    db.refresh(db_token)

    return db_token


def update_token_blockchain_network(
        db: Session,
        token_id: int,
        blockchain_network_id: int,
        confidence: float = 1.0,
        manually_verified: bool = True,
        needs_review: bool = False
) -> BlockchainToken:
    """
    Specialized function for updating the blockchain network of a token

    Args:
        db: Database session
        token_id: Token ID
        blockchain_network_id: Blockchain network ID
        confidence: Confidence level in the determination (default 1.0 for manually entered)
        manually_verified: Whether this is manually verified (default True)
        needs_review: Whether it needs subsequent review (default False)

    Returns:
        Updated BlockchainToken record or None if not found
    """
    # Getting the token
    db_token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()
    if not db_token:
        return None

    # Getting network information
    network = db.query(BlockchainNetwork).filter(BlockchainNetwork.id == blockchain_network_id).first()
    if not network:
        raise ValueError(f"Блокчейн мрежа с ID {blockchain_network_id} не съществува")

    # Token renewal
    db_token.blockchain_network_id = blockchain_network_id
    db_token.blockchain_network = network.name  # Also keep the symbolic identifier for compatibility
    db_token.network_confidence = confidence
    db_token.manually_verified = manually_verified
    db_token.needs_review = needs_review
    db_token.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(db_token)

    return db_token


def mark_token_as_verified(
        db: Session,
        token_id: int,
        verified: bool = True,
        needs_review: bool = False
) -> BlockchainToken:
    """
    Mark a token as manually verified

    Args:
        db: Database session
        token_id: Token ID
        verified: Whether it is verified
        needs_review: Whether it needs further review

    Returns:
        Updated BlockchainToken record or None if not found
    """
    return update_blockchain_token(
        db=db,
        token_id=token_id,
        manually_verified=verified,
        needs_review=needs_review
    )


def update_blockchain_network(
        db: Session,
        network_id: int,
        name: str = None,
        display_name: str = None,
        description: str = None,
        hashtags: List[str] = None,
        keywords: List[str] = None,
        icon_url: str = None,
        is_active: bool = None,
        website_url: str = None,
        explorer_url: str = None,
        launch_date: datetime = None
) -> BlockchainNetwork:
    """
    Updates a blockchain network entry

    Args:
        db: Database session
        network_id: Network ID
        name: New name (optional)
        display_name: New display name (optional)
        description: New description (optional)
        hashtags: New list of hashtags (optional)
        keywords: New list of keywords (optional)
        icon_url: New icon URL (optional)
        is_active: New activity status (optional)
        website_url: New website URL (optional)
        explorer_url: New blockchain explorer URL (optional)
        launch_date: New launch date (optional)

    Returns:
        Updated BlockchainNetwork entry or None if not found
    """
    # Get network by ID
    db_network = db.query(BlockchainNetwork).filter(BlockchainNetwork.id == network_id).first()

    # Return None if the network does not exist
    if db_network is None:
        return None

    # Update fields if provided
    if name is not None:
        # Check if the name is not in use by another network
        existing = db.query(BlockchainNetwork).filter(
            BlockchainNetwork.name == name,
            BlockchainNetwork.id != network_id
        ).first()
        if existing:
            raise ValueError(f"A blockchain network named '{name}' already exists")
        db_network.name = name

    if display_name is not None:
        db_network.display_name = display_name

    if description is not None:
        db_network.description = description

    if hashtags is not None:
        db_network.hashtags = hashtags

    if keywords is not None:
        db_network.keywords = keywords

    if icon_url is not None:
        db_network.icon_url = icon_url

    if is_active is not None:
        db_network.is_active = is_active

    if website_url is not None:
        db_network.website_url = website_url

    if explorer_url is not None:
        db_network.explorer_url = explorer_url

    if launch_date is not None:
        db_network.launch_date = launch_date

    # Update timestamp
    db_network.updated_at = datetime.utcnow()

    # Save changes to the database
    db.commit()
    db.refresh(db_network)

    return db_network


def merge_duplicate_tokens(
        db: Session,
        primary_token_id: int,
        duplicate_token_id: int
) -> bool:
    """
    Merges duplicate tokens by moving mentions to the primary token

    Args:
        db: Database session
        primary_token_id: ID of the primary token to keep
        duplicate_token_id: ID of the duplicate token to delete

    Returns:
        True if the merge succeeded, False otherwise
    """
    try:
        # Checking if both tokens exist
        primary_token = db.query(BlockchainToken).filter(BlockchainToken.id == primary_token_id).first()
        duplicate_token = db.query(BlockchainToken).filter(BlockchainToken.id == duplicate_token_id).first()

        if not primary_token or not duplicate_token:
            return False

        # Move mentions to the main token
        db.query(TokenMention).filter(
            TokenMention.token_id == duplicate_token_id
        ).update(
            {"token_id": primary_token_id}
        )

        # Delete duplicate token
        db.delete(duplicate_token)
        db.commit()

        return True

    except Exception as e:
        db.rollback()
        print(f"Token merging error: {e}")
        return False


def update_tweet(
        db: Session,
        tweet_id: int,
        text: str = None,
        author_username: str = None,
        retweet_count: int = None,
        like_count: int = None
) -> Tweet:
    """
    Update a tweet record

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet to update
        text: New text content of the tweet (optional)
        author_username: New author username (optional)
        retweet_count: New retweet count (optional)
        like_count: New like count (optional)

    Returns:
        Updated Tweet instance or None if not found
    """
    # Get the tweet by ID
    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    # Return None if tweet doesn't exist
    if db_tweet is None:
        return None

    # Update fields if provided
    if text is not None:
        db_tweet.text = text

    if author_username is not None:
        db_tweet.author_username = author_username

    if retweet_count is not None:
        db_tweet.retweet_count = retweet_count

    if like_count is not None:
        db_tweet.like_count = like_count

    # Commit changes to the database
    db.commit()
    db.refresh(db_tweet)

    return db_tweet


def update_sentiment_analysis(
        db: Session,
        sentiment_id: int,
        sentiment: SentimentEnum = None,
        confidence_score: float = None
) -> SentimentAnalysis:
    """
    Update a sentiment analysis record

    Args:
        db: Database session
        sentiment_id: The ID of the sentiment analysis record to update
        sentiment: New sentiment value (optional)
        confidence_score: New confidence score (optional)

    Returns:
        Updated SentimentAnalysis instance or None if not found
    """
    # Get the sentiment analysis by ID
    db_sentiment = db.query(SentimentAnalysis).filter(SentimentAnalysis.id == sentiment_id).first()

    # Return None if record doesn't exist
    if db_sentiment is None:
        return None

    # Update sentiment if provided
    if sentiment is not None:
        db_sentiment.sentiment = sentiment

    # Update confidence score if provided
    if confidence_score is not None:
        # Validate that confidence score is between 0 and 1
        if not 0 <= confidence_score <= 1:
            raise ValueError("Confidence score must be between 0 and 1")
        db_sentiment.confidence_score = confidence_score

    # Update the analyzed_at timestamp to current time
    db_sentiment.analyzed_at = datetime.utcnow()

    # Commit changes to the database
    db.commit()
    db.refresh(db_sentiment)

    return db_sentiment


def update_token_mention(
        db: Session,
        mention_id: int,
        tweet_id: int = None,
        token_id: int = None
) -> TokenMention:
    """
    Update a token mention record

    Args:
        db: Database session
        mention_id: The ID of the token mention record to update
        tweet_id: New tweet ID (optional)
        token_id: New token ID (optional)

    Returns:
        Updated TokenMention instance or None if not found
    """
    # Get the token mention by ID
    db_mention = db.query(TokenMention).filter(TokenMention.id == mention_id).first()

    # Return None if record doesn't exist
    if db_mention is None:
        return None

    # Update tweet_id if provided
    if tweet_id is not None:
        # Verify the tweet exists before updating
        tweet_exists = db.query(Tweet).filter(Tweet.id == tweet_id).first() is not None
        if not tweet_exists:
            raise ValueError(f"Tweet with ID {tweet_id} does not exist")
        db_mention.tweet_id = tweet_id

    # Update token_id if provided
    if token_id is not None:
        # Verify the token exists before updating
        token_exists = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first() is not None
        if not token_exists:
            raise ValueError(f"Token with ID {token_id} does not exist")
        db_mention.token_id = token_id

    # Update the mentioned_at timestamp
    db_mention.mentioned_at = datetime.utcnow()

    # Commit changes to the database
    db.commit()
    db.refresh(db_mention)

    return db_mention


def update_tweet_by_twitter_id(
        db: Session,
        twitter_id: str,
        text: str = None,
        author_username: str = None,
        retweet_count: int = None,
        like_count: int = None
) -> Tweet:
    """
    Update a tweet record using its Twitter ID

    Args:
        db: Database session
        twitter_id: The original Twitter ID (not the database ID)
        text: New text content of the tweet (optional)
        author_username: New author username (optional)
        retweet_count: New retweet count (optional)
        like_count: New like count (optional)

    Returns:
        Updated Tweet instance or None if not found
    """
    # Get the tweet by Twitter ID
    db_tweet = db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()

    # Return None if tweet doesn't exist
    if db_tweet is None:
        return None

    # Call the existing update function with the database ID
    return update_tweet(
        db=db,
        tweet_id=db_tweet.id,
        text=text,
        author_username=author_username,
        retweet_count=retweet_count,
        like_count=like_count
    )