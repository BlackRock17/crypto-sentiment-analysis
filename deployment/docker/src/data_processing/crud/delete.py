from sqlalchemy.orm import Session
from src.data_processing.models.database import BlockchainToken, Tweet, SentimentAnalysis, TokenMention, \
    BlockchainNetwork
from datetime import datetime


def delete_blockchain_token(db: Session, token_id: int, check_mentions: bool = True) -> bool:
    """
    Delete a blockchain token record

    Args:
        db: Database session
        token_id: The ID of the token to delete
        check_mentions: If True, will check if token has mentions and prevent deletion

    Returns:
        True if deletion was successful, False if token not found or has mentions
    """
    # Get the token by ID
    db_token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()

    # Return False if token doesn't exist
    if db_token is None:
        return False

    # Check if the token has mentions
    if check_mentions:
        mentions_count = db.query(TokenMention).filter(TokenMention.token_id == token_id).count()
        if mentions_count > 0:
            raise ValueError(
                f"Cannot delete token with ID {token_id} as it has {mentions_count} mentions. Remove mentions first or use cascade delete.")

    # Delete the token
    db.delete(db_token)
    db.commit()

    return True


def delete_tweet(db: Session, tweet_id: int, cascade: bool = True) -> bool:
    """
    Delete a tweet record

    Args:
        db: Database session
        tweet_id: The internal database ID of the tweet to delete
        cascade: If True, will also delete associated sentiment analysis and token mentions

    Returns:
        True if deletion was successful, False if tweet not found
    """
    # Get the tweet by ID
    db_tweet = db.query(Tweet).filter(Tweet.id == tweet_id).first()

    # Return False if tweet doesn't exist
    if db_tweet is None:
        return False

    # Handle cascade deletion
    if cascade:
        # Delete associated sentiment analysis
        db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).delete()

        # Delete associated token mentions
        db.query(TokenMention).filter(TokenMention.tweet_id == tweet_id).delete()
    else:
        # Check if tweet has associated records
        sentiment_count = db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).count()
        mentions_count = db.query(TokenMention).filter(TokenMention.tweet_id == tweet_id).count()

        if sentiment_count > 0 or mentions_count > 0:
            raise ValueError(
                f"Cannot delete tweet with ID {tweet_id} as it has {sentiment_count} sentiment analyses and {mentions_count} token mentions. Use cascade delete or remove associated records first.")

    # Delete the tweet
    db.delete(db_tweet)
    db.commit()

    return True


def delete_sentiment_analysis(db: Session, sentiment_id: int) -> bool:
    """
    Delete a sentiment analysis record

    Args:
        db: Database session
        sentiment_id: The ID of the sentiment analysis record to delete

    Returns:
        True if deletion was successful, False if record not found
    """
    # Get the sentiment analysis by ID
    db_sentiment = db.query(SentimentAnalysis).filter(SentimentAnalysis.id == sentiment_id).first()

    # Return False if record doesn't exist
    if db_sentiment is None:
        return False

    # Delete the sentiment analysis
    db.delete(db_sentiment)
    db.commit()

    return True


def delete_token_mention(db: Session, mention_id: int) -> bool:
    """
    Delete a token mention record

    Args:
        db: Database session
        mention_id: The ID of the token mention record to delete

    Returns:
        True if deletion was successful, False if record not found
    """
    # Get the token mention by ID
    db_mention = db.query(TokenMention).filter(TokenMention.id == mention_id).first()

    # Return False if record doesn't exist
    if db_mention is None:
        return False

    # Delete the token mention
    db.delete(db_mention)
    db.commit()

    return True


def delete_tweet_by_twitter_id(db: Session, twitter_id: str, cascade: bool = True) -> bool:
    """
    Delete a tweet record using its Twitter ID

    Args:
        db: Database session
        twitter_id: The original Twitter ID (not the database ID)
        cascade: If True, will also delete associated sentiment analysis and token mentions

    Returns:
        True if deletion was successful, False if tweet not found
    """
    # Get the tweet by Twitter ID
    db_tweet = db.query(Tweet).filter(Tweet.tweet_id == twitter_id).first()

    # Return False if tweet doesn't exist
    if db_tweet is None:
        return False

    # Call the existing delete function with the database ID
    return delete_tweet(db=db, tweet_id=db_tweet.id, cascade=cascade)


def delete_blockchain_token_by_address(db: Session, token_address: str, blockchain_network: str = None,
                                       check_mentions: bool = True) -> bool:
    """
    Delete a blockchain token record using its blockchain address

    Args:
        db: Database session
        token_address: The token's address on blockchain
        blockchain_network: Optional blockchain network name to specify which token to delete
        check_mentions: If True, will check if token has mentions and prevent deletion

    Returns:
        True if deletion was successful, False if token not found or has mentions
    """
    # Get the token by address
    query = db.query(BlockchainToken).filter(BlockchainToken.token_address == token_address)

    if blockchain_network:
        query = query.filter(BlockchainToken.blockchain_network == blockchain_network)

    db_token = query.first()

    # Return False if token doesn't exist
    if db_token is None:
        return False

    # Call the existing delete function with the database ID
    return delete_blockchain_token(db=db, token_id=db_token.id, check_mentions=check_mentions)


def delete_blockchain_token_cascade(db: Session, token_id: int) -> bool:
    """
    Delete a blockchain token record along with all its mentions

    Args:
        db: Database session
        token_id: The ID of the token to delete

    Returns:
        True if deletion was successful, False if token not found
    """
    # Get the token by ID
    db_token = db.query(BlockchainToken).filter(BlockchainToken.id == token_id).first()

    # Return False if token doesn't exist
    if db_token is None:
        return False

    # Delete all mentions of this token
    db.query(TokenMention).filter(TokenMention.token_id == token_id).delete()

    # Delete the token
    db.delete(db_token)
    db.commit()

    return True


def delete_blockchain_network(db: Session, network_id: int, check_tokens: bool = True) -> bool:
    """
    Delete a blockchain network record

    Args:
        db: Database session
        network_id: The ID of the network to delete
        check_tokens: If True, will check if network has associated tokens and prevent deletion

    Returns:
        True if deletion was successful, False if network not found or has tokens
    """
    # Get the network by ID
    db_network = db.query(BlockchainNetwork).filter(BlockchainNetwork.id == network_id).first()

    # Return False if network doesn't exist
    if db_network is None:
        return False

    # Check if the network has associated tokens
    if check_tokens:
        tokens_count = db.query(BlockchainToken).filter(BlockchainToken.blockchain_network_id == network_id).count()
        if tokens_count > 0:
            raise ValueError(
                f"Cannot delete network with ID {network_id} as it has {tokens_count} associated tokens. Remove tokens first or set check_tokens=False.")

    # Delete the network
    db.delete(db_network)
    db.commit()

    return True


def delete_blockchain_network_by_name(db: Session, network_name: str, check_tokens: bool = True) -> bool:
    """
    Delete a blockchain network record by its name

    Args:
        db: Database session
        network_name: The name of the network to delete
        check_tokens: If True, will check if network has associated tokens and prevent deletion

    Returns:
        True if deletion was successful, False if network not found or has tokens
    """
    # Get the network by name
    db_network = db.query(BlockchainNetwork).filter(BlockchainNetwork.name == network_name).first()

    # Return False if network doesn't exist
    if db_network is None:
        return False

    # Call the existing delete function with the network ID
    return delete_blockchain_network(db=db, network_id=db_network.id, check_tokens=check_tokens)
