"""
Notification service for generating and managing notifications.
"""
import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

from sqlalchemy.orm import Session

from src.data_processing.models.notifications import NotificationType, NotificationPriority
from src.data_processing.models.database import BlockchainToken, BlockchainNetwork
from src.data_processing.crud.notifications import create_notification, get_notifications
from src.data_processing.crud.read import get_blockchain_token_by_id

# Configure logger
logger = logging.getLogger(__name__)


class NotificationService:
    """Service for managing system notifications."""

    def __init__(self, db: Session):
        """
        Initialize the notification service.

        Args:
            db: Database session
        """
        self.db = db

    def notify_new_token(self, token_id: int, confidence: float = 0.0) -> bool:
        """
        Create a notification for a new token detection.

        Args:
            token_id: Token ID
            confidence: Network confidence score (0-1)

        Returns:
            True if notification was created, False otherwise
        """
        try:
            # Get token details
            token = get_blockchain_token_by_id(self.db, token_id)
            if not token:
                logger.error(f"Cannot create notification: Token with ID {token_id} not found")
                return False

            # Determine priority based on confidence
            priority = NotificationPriority.LOW
            if confidence <= 0.3:
                priority = NotificationPriority.HIGH
            elif confidence <= 0.7:
                priority = NotificationPriority.MEDIUM

            # Create notification
            create_notification(
                db=self.db,
                type=NotificationType.NEW_TOKEN,
                title=f"New token detected: {token.symbol}",
                message=f"A new token '{token.symbol}' has been detected with {confidence:.1%} network confidence.",
                priority=priority,
                metadata={
                    "token_id": token.id,
                    "symbol": token.symbol,
                    "blockchain_network": token.blockchain_network,
                    "confidence": confidence
                }
            )

            return True

        except Exception as e:
            logger.error(f"Error creating new token notification: {e}")
            return False

    def notify_uncategorized_token(self, token_id: int, mention_count: int) -> bool:
        """
        Create a notification for a token that needs categorization.

        Args:
            token_id: Token ID
            mention_count: Number of mentions

        Returns:
            True if notification was created, False otherwise
        """
        try:
            # Get token details
            token = get_blockchain_token_by_id(self.db, token_id)
            if not token:
                logger.error(f"Cannot create notification: Token with ID {token_id} not found")
                return False

            # Determine priority based on mention count
            priority = NotificationPriority.LOW
            if mention_count >= 10:
                priority = NotificationPriority.HIGH
            elif mention_count >= 5:
                priority = NotificationPriority.MEDIUM

            # Create notification
            create_notification(
                db=self.db,
                type=NotificationType.UNCATEGORIZED_TOKEN,
                title=f"Token needs categorization: {token.symbol}",
                message=(
                    f"Token '{token.symbol}' has {mention_count} mentions but "
                    f"{'no assigned blockchain network' if not token.blockchain_network else 'low confidence score'}."
                ),
                priority=priority,
                metadata={
                    "token_id": token.id,
                    "symbol": token.symbol,
                    "mention_count": mention_count,
                    "blockchain_network": token.blockchain_network,
                    "confidence": token.network_confidence
                }
            )

            return True

        except Exception as e:
            logger.error(f"Error creating uncategorized token notification: {e}")
            return False

    def notify_duplicate_tokens(self, primary_symbol: str, duplicate_count: int, token_ids: List[int]) -> bool:
        """
        Create a notification for potential duplicate tokens.

        Args:
            primary_symbol: Token symbol
            duplicate_count: Number of potential duplicates
            token_ids: List of token IDs involved

        Returns:
            True if notification was created, False otherwise
        """
        try:
            # Determine priority based on duplicate count
            priority = NotificationPriority.MEDIUM
            if duplicate_count >= 3:
                priority = NotificationPriority.HIGH

            # Create notification
            create_notification(
                db=self.db,
                type=NotificationType.DUPLICATE_TOKEN,
                title=f"Potential duplicate tokens: {primary_symbol}",
                message=f"Found {duplicate_count} potential duplicate tokens for symbol '{primary_symbol}'.",
                priority=priority,
                metadata={
                    "symbol": primary_symbol,
                    "duplicate_count": duplicate_count,
                    "token_ids": token_ids
                }
            )

            return True

        except Exception as e:
            logger.error(f"Error creating duplicate tokens notification: {e}")
            return False

    def notify_high_activity(self, token_id: int, mention_count: int, time_period_hours: int) -> bool:
        """
        Create a notification for unusually high activity for a token.

        Args:
            token_id: Token ID
            mention_count: Number of mentions in the time period
            time_period_hours: Time period in hours

        Returns:
            True if notification was created, False otherwise
        """
        try:
            # Get token details
            token = get_blockchain_token_by_id(self.db, token_id)
            if not token:
                logger.error(f"Cannot create notification: Token with ID {token_id} not found")
                return False

            # Create notification
            create_notification(
                db=self.db,
                type=NotificationType.HIGH_ACTIVITY,
                title=f"High activity for {token.symbol}",
                message=(
                    f"Token '{token.symbol}' has unusually high activity with "
                    f"{mention_count} mentions in the last {time_period_hours} hours."
                ),
                priority=NotificationPriority.HIGH,
                metadata={
                    "token_id": token.id,
                    "symbol": token.symbol,
                    "blockchain_network": token.blockchain_network,
                    "mention_count": mention_count,
                    "time_period_hours": time_period_hours
                }
            )

            return True

        except Exception as e:
            logger.error(f"Error creating high activity notification: {e}")
            return False

    def create_system_notification(
            self,
            title: str,
            message: str,
            priority: NotificationPriority = NotificationPriority.MEDIUM,
            metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a general system notification.

        Args:
            title: Notification title
            message: Notification message
            priority: Priority level
            metadata: Additional metadata

        Returns:
            True if notification was created, False otherwise
        """
        try:
            create_notification(
                db=self.db,
                type=NotificationType.SYSTEM,
                title=title,
                message=message,
                priority=priority,
                metadata=metadata or {}
            )

            return True

        except Exception as e:
            logger.error(f"Error creating system notification: {e}")
            return False

    def get_recent_notifications(
            self,
            limit: int = 10,
            unread_only: bool = False,
            notification_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent notifications.

        Args:
            limit: Maximum number of notifications to return
            unread_only: Only return unread notifications
            notification_type: Filter by notification type

        Returns:
            List of notification dictionaries
        """
        notifications = get_notifications(
            db=self.db,
            limit=limit,
            unread_only=unread_only,
            notification_type=notification_type
        )

        # Convert to dictionaries for API response
        return [
            {
                "id": n.id,
                "type": n.type,
                "title": n.title,
                "message": n.message,
                "priority": n.priority,
                "created_at": n.created_at,
                "is_read": n.is_read,
                "metadata": n.metadata
            }
            for n in notifications
        ]
