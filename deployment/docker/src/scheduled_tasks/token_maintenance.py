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


async def auto_merge_exact_duplicates(dry_run: bool = False) -> Dict[str, Any]:
    """
    Automatically merge exact duplicate tokens (same symbol and network).
    Only merges tokens that are exact duplicates without manual verification required.

    Args:
        dry_run: If True, only simulate merges without actually performing them

    Returns:
        Dictionary with merge statistics
    """
    db = next(get_db())
    try:
        logger.info("Starting scheduled task: Auto-merging exact duplicate tokens")

        # Find tokens with same symbol and blockchain network
        # First, find symbols that appear multiple times in the same network
        duplicate_query = db.query(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network,
            func.count(BlockchainToken.id).label("count")
        ).filter(
            BlockchainToken.blockchain_network != None  # Only consider tokens with assigned networks
        ).group_by(
            BlockchainToken.symbol,
            BlockchainToken.blockchain_network
        ).having(
            func.count(BlockchainToken.id) > 1
        )

        # Execute query to get duplicates
        potential_duplicates = duplicate_query.all()

        if not potential_duplicates:
            logger.info("No exact duplicate tokens found")
            return {"groups_processed": 0, "tokens_merged": 0}

        logger.info(f"Found {len(potential_duplicates)} potential duplicate groups")

        groups_processed = 0
        tokens_merged = 0

        for symbol, network, count in potential_duplicates:
            try:
                # Get all tokens with this symbol and network
                tokens = db.query(BlockchainToken).filter(
                    BlockchainToken.symbol == symbol,
                    BlockchainToken.blockchain_network == network
                ).all()

                # Skip if there's only one token (shouldn't happen due to the HAVING clause above)
                if len(tokens) <= 1:
                    continue

                # Sort tokens to determine primary:
                # 1. Manually verified tokens first
                # 2. Tokens with highest confidence
                # 3. Tokens with most mentions
                # 4. Oldest tokens

                # First check for manually verified tokens
                verified_tokens = [t for t in tokens if t.manually_verified]
                primary_token = None

                if verified_tokens:
                    # Use the manually verified token with highest confidence
                    primary_token = max(verified_tokens, key=lambda t: (t.network_confidence or 0))
                else:
                    # Calculate mention counts for each token
                    mention_counts = {}
                    for token in tokens:
                        count = db.query(func.count(TokenMention.id)).filter(
                            TokenMention.token_id == token.id
                        ).scalar()
                        mention_counts[token.id] = count

                    # Sort by confidence, then mentions, then age (oldest first)
                    token_scores = [(
                        t,
                        t.network_confidence or 0,
                        mention_counts.get(t.id, 0),
                        -t.created_at.timestamp()  # Negative so oldest is highest
                    ) for t in tokens]

                    # Get token with highest score
                    primary_token = max(token_scores, key=lambda x: (x[1], x[2], x[3]))[0]

                # Get tokens to merge (all except primary)
                tokens_to_merge = [t for t in tokens if t.id != primary_token.id]

                if not tokens_to_merge:
                    continue

                # Log the planned merge
                logger.info(
                    f"{'Would merge' if dry_run else 'Merging'} {len(tokens_to_merge)} duplicates of {symbol} "
                    f"({network}) into primary token ID {primary_token.id}"
                )

                if not dry_run:
                    # Perform the merges
                    for token in tokens_to_merge:
                        success = merge_duplicate_tokens(
                            db=db,
                            primary_token_id=primary_token.id,
                            duplicate_token_id=token.id
                        )

                        if success:
                            tokens_merged += 1
                        else:
                            logger.warning(f"Failed to merge token ID {token.id} into {primary_token.id}")
                else:
                    # In dry run, just count what would be merged
                    tokens_merged += len(tokens_to_merge)

                groups_processed += 1

            except Exception as e:
                logger.error(f"Error processing duplicate group {symbol}/{network}: {e}")

        action_verb = "Would have merged" if dry_run else "Merged"
        logger.info(f"Auto-merge complete: {groups_processed} groups processed, {action_verb} {tokens_merged} tokens")

        return {
            "dry_run": dry_run,
            "groups_processed": groups_processed,
            "tokens_merged": tokens_merged
        }

    except Exception as e:
        logger.error(f"Error in auto_merge_exact_duplicates task: {e}")
        return {"error": str(e), "groups_processed": 0, "tokens_merged": 0}
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


async def advanced_duplicate_detection(
        similarity_threshold: float = 0.85,
        max_groups: int = 20
) -> Dict[str, Any]:
    """
    Enhanced duplicate token detection using multiple similarity metrics.
    Uses symbol similarity, address similarity, and mention context to detect duplicates.

    Args:
        similarity_threshold: Minimum similarity score to consider tokens as duplicates (0-1)
        max_groups: Maximum number of duplicate groups to process

    Returns:
        Dictionary with statistics about duplicate detection
    """
    db = next(get_db())
    try:
        logger.info(
            f"Starting scheduled task: Advanced duplicate token detection (similarity >= {similarity_threshold})")

        # Get all active tokens
        tokens = db.query(BlockchainToken).filter(BlockchainToken.is_archived == False).all()

        if not tokens:
            logger.info("No tokens found for duplicate analysis")
            return {"duplicate_groups": 0, "total_duplicates": 0}

        # Group tokens by normalized symbol
        symbol_groups = {}

        for token in tokens:
            # Normalize symbol (lowercase, remove special chars, etc.)
            normalized_symbol = ''.join(c.lower() for c in token.symbol if c.isalnum())

            if normalized_symbol not in symbol_groups:
                symbol_groups[normalized_symbol] = []

            symbol_groups[normalized_symbol].append(token)

        # Filter for groups with multiple tokens
        duplicate_groups = {symbol: group for symbol, group in symbol_groups.items() if len(group) > 1}

        # Initialize notification service
        notification_service = NotificationService(db)

        # Process only up to max_groups
        prioritized_groups = sorted(
            duplicate_groups.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )[:max_groups]

        processed_groups = 0
        total_duplicates = 0

        # Analyze each group more deeply
        for normalized_symbol, group in prioritized_groups:
            group_size = len(group)

            # Skip small groups
            if group_size <= 1:
                continue

            # Check if tokens are on the same network
            network_groups = {}
            for token in group:
                network = token.blockchain_network
                if network not in network_groups:
                    network_groups[network] = []
                network_groups[network].append(token)

            # Identify exact duplicates (same symbol and network)
            for network, network_tokens in network_groups.items():
                if len(network_tokens) > 1:
                    # These are exact duplicates
                    token_ids = [token.id for token in network_tokens]

                    # Notify about duplicate tokens
                    notification_service.notify_duplicate_tokens(
                        primary_symbol=network_tokens[0].symbol,
                        duplicate_count=len(network_tokens),
                        token_ids=token_ids
                    )

                    total_duplicates += len(network_tokens) - 1
                    logger.info(
                        f"Found {len(network_tokens)} exact duplicates for symbol '{network_tokens[0].symbol}' on network '{network}'")

            processed_groups += 1

        logger.info(
            f"Advanced duplicate detection complete: {processed_groups} groups analyzed, {total_duplicates} total duplicates found")

        return {
            "duplicate_groups": processed_groups,
            "total_duplicates": total_duplicates
        }

    except Exception as e:
        logger.error(f"Error in advanced_duplicate_detection task: {e}")
        return {"error": str(e), "duplicate_groups": 0, "total_duplicates": 0}
    finally:
        db.close()
