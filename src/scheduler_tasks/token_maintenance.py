"""
Scheduled tasks for token maintenance.
Periodically checks for duplicate tokens and archives inactive ones.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.models.database import BlockchainToken, TokenMention
from src.data_processing.crud.update import merge_duplicate_tokens
from src.services.notification_service import NotificationService, NotificationPriority

# Configure logger
logger = logging.getLogger(__name__)


async def check_for_duplicate_tokens():
    """
    Scheduled task to check for potential duplicate tokens and notify administrators.
    """
    db = next(get_db())
    try:
        logger.info("Starting scheduled task: Checking for duplicate tokens")

        # Initialize notification service
        notification_service = NotificationService(db)

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
        duplicate_count = 0
        for symbol, token_group in symbol_groups.items():
            if len(token_group) > 1:
                duplicate_count += 1

                # Get the IDs for notification
                token_ids = [token.id for token in token_group]

                # Create notification
                notification_service.notify_duplicate_tokens(
                    primary_symbol=symbol,
                    duplicate_count=len(token_group),
                    token_ids=token_ids
                )

                logger.info(f"Found {len(token_group)} potential duplicates for symbol '{symbol}'")

        if duplicate_count == 0:
            logger.info("No duplicate tokens found")
        else:
            logger.info(f"Found {duplicate_count} groups of potential duplicate tokens")

        logger.info("Completed scheduled task: Checking for duplicate tokens")

    except Exception as e:
        logger.error(f"Error in scheduled task to check for duplicate tokens: {e}")
    finally:
        db.close()


async def auto_merge_exact_duplicates():
    """
    Scheduled task to automatically merge tokens with identical symbols and blockchain networks.
    """
    db = next(get_db())
    try:
        logger.info("Starting scheduled task: Auto-merging exact duplicate tokens")

        # Find tokens with same symbol and blockchain network
        # First, find symbols that appear multiple times
        duplicate_symbols = db.query(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            func.count(BlockchainToken.id).label("count")
        ).group_by(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network
        ).having(
            func.count(BlockchainToken.id) > 1
        ).all()

        merged_count = 0

        # For each potential duplicate set
        for symbol, network, count in duplicate_symbols:
            if not network:
                # Skip tokens without a blockchain network
                continue

            # Get all tokens with this symbol and network
            tokens = db.query(BlockchainToken).filter(
                BlockchainToken.symbol == symbol,
                BlockchainToken.blockchain_network == network
            ).all()

            if len(tokens) <= 1:
                continue

            # Find the primary token (manually verified or most mentions)
            primary_token = None
            for token in tokens:
                if token.manually_verified:
                    primary_token = token
                    break

            # If no manually verified token, use the one with most mentions
            if not primary_token:
                # For each token, count mentions
                token_mentions = {}
                for token in tokens:
                    mention_count = db.query(func.count(TokenMention.id)).filter(
                        TokenMention.token_id == token.id
                    ).scalar()
                    token_mentions[token.id] = mention_count

                # Use token with most mentions as primary
                if token_mentions:
                    primary_id = max(token_mentions.items(), key=lambda x: x[1])[0]
                    for token in tokens:
                        if token.id == primary_id:
                            primary_token = token
                            break

            # If we found a primary token, merge others into it
            if primary_token:
                for token in tokens:
                    if token.id != primary_token.id:
                        try:
                            success = merge_duplicate_tokens(
                                db=db,
                                primary_token_id=primary_token.id,
                                duplicate_token_id=token.id
                            )
                            if success:
                                merged_count += 1
                                logger.info(
                                    f"Merged duplicate token {token.symbol} (ID: {token.id}) into {primary_token.id}")
                        except Exception as e:
                            logger.error(f"Error merging token {token.id} into {primary_token.id}: {e}")

        logger.info(f"Auto-merged {merged_count} duplicate tokens")
        logger.info("Completed scheduled task: Auto-merging exact duplicate tokens")

    except Exception as e:
        logger.error(f"Error in scheduled task to auto-merge duplicate tokens: {e}")
    finally:
        db.close()


async def archive_inactive_tokens(days_inactive: int = 90):
    """
    Scheduled task to archive tokens that have had no activity for a specified period.

    Args:
        days_inactive: Number of days of inactivity to consider a token inactive
    """
    db = next(get_db())
    try:
        logger.info(f"Starting scheduled task: Archiving inactive tokens (>= {days_inactive} days inactive)")

        # Calculate the cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days_inactive)

        # Get all tokens
        tokens = db.query(BlockchainToken).all()

        archived_count = 0

        # For each token, check if it has had any recent activity
        for token in tokens:
            # Get the latest mention
            latest_mention = db.query(func.max(TokenMention.mentioned_at)).filter(
                TokenMention.token_id == token.id
            ).scalar()

            # If no mentions or latest mention is before cutoff date
            if not latest_mention or latest_mention < cutoff_date:
                # Archive the token (add an 'is_archived' flag to BlockchainToken model)
                # For now, let's just log it
                logger.info(
                    f"Token {token.symbol} (ID: {token.id}) has had no activity since {latest_mention or 'ever'}")

                # NOTE: To actually archive tokens, you would need to:
                # 1. Add an 'is_archived' column to the BlockchainToken model
                # 2. Set it to True for inactive tokens
                # token.is_archived = True
                # db.commit()

                archived_count += 1

        logger.info(f"Identified {archived_count} inactive tokens for archiving")
        logger.info("Completed scheduled task: Archiving inactive tokens")

    except Exception as e:
        logger.error(f"Error in scheduled task to archive inactive tokens: {e}")
    finally:
        db.close()
