"""
CRUD operations for notifications.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union

from sqlalchemy import desc
from sqlalchemy.orm import Session

from src.data_processing.models.notifications import Notification, NotificationType, NotificationPriority


def create_notification(
        db: Session,
        type: Union[str, NotificationType],
        title: str,
        message: str,
        priority: Union[str, NotificationPriority] = NotificationPriority.MEDIUM,
        user_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
) -> Notification:
    """
    Create a new notification.

    Args:
        db: Database session
        type: Notification type
        title: Notification title
        message: Detailed notification message
        priority: Priority level (low, medium, high)
        user_id: Optional user ID if notification is for a specific user
        metadata: Additional metadata (token_id, symbol, etc.)

    Returns:
        Created Notification instance
    """
    if isinstance(type, NotificationType):
        type = type.value

    if isinstance(priority, NotificationPriority):
        priority = priority.value

    notification = Notification(
        type=type,
        title=title,
        message=message,
        priority=priority,
        user_id=user_id,
        metadata=metadata or {}
    )

    db.add(notification)
    db.commit()
    db.refresh(notification)

    return notification


def get_notification(db: Session, notification_id: int) -> Optional[Notification]:
    """
    Get a notification by ID.

    Args:
        db: Database session
        notification_id: Notification ID

    Returns:
        Notification instance or None if not found
    """
    return db.query(Notification).filter(Notification.id == notification_id).first()


def get_notifications(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        user_id: Optional[int] = None,
        unread_only: bool = False,
        notification_type: Optional[str] = None,
        priority: Optional[str] = None
) -> List[Notification]:
    """
    Get notifications with optional filtering.

    Args:
        db: Database session
        skip: Number of records to skip
        limit: Maximum number of records to return
        user_id: Optional user ID to filter notifications
        unread_only: Only return unread notifications
        notification_type: Filter by notification type
        priority: Filter by priority level

    Returns:
        List of Notification instances
    """
    query = db.query(Notification)

    # Apply filters
    if user_id is not None:
        query = query.filter(Notification.user_id == user_id)

    if unread_only:
        query = query.filter(Notification.is_read == False)

    if notification_type:
        query = query.filter(Notification.type == notification_type)

    if priority:
        query = query.filter(Notification.priority == priority)

    # Order by creation date (newest first)
    query = query.order_by(desc(Notification.created_at))

    # Apply pagination
    return query.offset(skip).limit(limit).all()


def mark_notification_as_read(db: Session, notification_id: int) -> bool:
    """
    Mark a notification as read.

    Args:
        db: Database session
        notification_id: Notification ID

    Returns:
        True if successful, False if notification not found
    """
    notification = get_notification(db, notification_id)
    if not notification:
        return False

    notification.is_read = True
    db.commit()
    return True


def mark_all_notifications_as_read(db: Session, user_id: Optional[int] = None) -> int:
    """
    Mark all notifications as read for a user or all notifications if no user specified.

    Args:
        db: Database session
        user_id: Optional user ID

    Returns:
        Number of notifications marked as read
    """
    query = db.query(Notification).filter(Notification.is_read == False)

    if user_id is not None:
        query = query.filter(Notification.user_id == user_id)

    # Update all matching notifications
    count = query.count()
    query.update({"is_read": True})
    db.commit()

    return count


def delete_notification(db: Session, notification_id: int) -> bool:
    """
    Delete a notification.

    Args:
        db: Database session
        notification_id: Notification ID

    Returns:
        True if successful, False if notification not found
    """
    notification = get_notification(db, notification_id)
    if not notification:
        return False

    db.delete(notification)
    db.commit()
    return True


def delete_old_notifications(db: Session, days: int = 30) -> int:
    """
    Delete notifications older than specified number of days.

    Args:
        db: Database session
        days: Number of days

    Returns:
        Number of notifications deleted
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days)

    # Count notifications to be deleted
    count = db.query(Notification).filter(Notification.created_at < cutoff_date).count()

    # Delete notifications
    db.query(Notification).filter(Notification.created_at < cutoff_date).delete()
    db.commit()

    return count
