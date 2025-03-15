"""
Kafka consumer for token categorization.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

from src.data_processing.kafka.consumer import KafkaConsumer
from src.data_processing.kafka.config import TOPICS
from src.data_processing.database import get_db
from src.data_processing.crud.core_queries import analyze_token_for_network_detection
from src.data_processing.crud.update import update_token_blockchain_network

logger = logging.getLogger(__name__)


class TokenCategorizationConsumer(KafkaConsumer):
    """Consumer for categorizing tokens based on network detection."""

    def __init__(self):
        """Initialize token categorization consumer."""
        super().__init__(
            topics=TOPICS["TOKEN_CATEGORIZATION"],
            group_id="token-categorizer",
            auto_commit=False
        )

    def handle_message(self, message):
        """
        Process a token categorization message from Kafka.

        Args:
            message: Kafka message containing token to categorize

        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Get database session
            db = next(get_db())

            try:
                # Deserialize the message
                task_data = self.deserialize_message(message)
                if not task_data:
                    logger.warning(f"Empty or invalid token categorization data received")
                    return False

                token_id = task_data.get('token_id')
                if not token_id:
                    logger.error("Missing token_id in categorization task")
                    return False

                logger.info(f"Categorizing token: {token_id}")

                # Analyze token to detect network
                analysis = analyze_token_for_network_detection(db, token_id)

                # Auto-categorize if confidence is high enough
                min_confidence = 0.7  # Threshold for auto-categorization
                if (not analysis["needs_manual_review"] and
                        analysis["recommended_network"] and
                        analysis["confidence_score"] >= min_confidence):

                    network_id = analysis["recommended_network"]["id"]
                    confidence = analysis["confidence_score"]

                    # Update token with detected network
                    updated_token = update_token_blockchain_network(
                        db=db,
                        token_id=token_id,
                        blockchain_network_id=network_id,
                        confidence=confidence,
                        manually_verified=False,  # Auto-detected
                        needs_review=False  # High confidence, doesn't need review
                    )

                    if updated_token:
                        logger.info(f"Auto-categorized token {token_id} to network "
                                    f"{analysis['recommended_network']['name']} with confidence {confidence:.2f}")
                        return True
                    else:
                        logger.error(f"Failed to update token {token_id}")
                        return False
                else:
                    # Flag for manual review if confidence is low
                    from src.data_processing.crud.update import update_blockchain_token
                    update_blockchain_token(
                        db=db,
                        token_id=token_id,
                        needs_review=True
                    )

                    # Create notification for uncategorized token
                    from src.services.notification_service import NotificationService
                    from src.data_processing.models.database import TokenMention

                    notification_service = NotificationService(db)
                    mention_count = db.query(TokenMention).filter(TokenMention.token_id == token_id).count()
                    notification_service.notify_uncategorized_token(token_id, mention_count)

                    logger.info(f"Flagged token {token_id} for review with confidence "
                                f"{analysis['confidence_score']:.2f}")
                    return True

            except Exception as e:
                logger.error(f"Error processing token categorization: {e}")
                return False

            finally:
                # Close database session
                db.close()

        except Exception as e:
            logger.error(f"Error in token categorization consumer: {e}")
            return False
