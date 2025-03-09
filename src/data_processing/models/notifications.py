"""
Models for notification system.
"""
from datetime import datetime
from enum import Enum
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship

from src.data_processing.models.database import Base


class NotificationType(str, Enum):
    """Types of notifications"""
    NEW_TOKEN = "new_token"  # New token detected
    UNCATEGORIZED_TOKEN = "uncategorized_token"  # Token that needs categorization
    DUPLICATE_TOKEN = "duplicate_token"  # Potential duplicate token detected
    HIGH_ACTIVITY = "high_activity"  # Unusual activity for a token
    SYSTEM = "system"  # System notification


class NotificationPriority(str, Enum):
    """Priority levels for notifications"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Notification(Base):
    """Notification model for storing system notifications"""
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)  # Type of notification
    title = Column(String(255), nullable=False)  # Notification title
    message = Column(Text, nullable=False)  # Notification message
    priority = Column(String(20), default="medium")  # Priority level
    created_at = Column(DateTime, default=datetime.utcnow)
    is_read = Column(Boolean, default=False)  # Whether the notification has been read
    additional_data = Column(JSON, default={})  # Additional metadata (e.g., token_id, symbol)

    # Optional link to a user if notification is user-specific
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    user = relationship("User", back_populates="notifications")

    def __repr__(self):
        return f"<Notification(id={self.id}, type='{self.type}', title='{self.title}')>"
