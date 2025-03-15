"""
API endpoints for notifications.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Path, HTTPException, status
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.models.auth import User
from src.data_processing.models.notifications import NotificationType
from src.data_processing.crud.notifications import (
    get_notifications, get_notification, mark_notification_as_read,
    mark_all_notifications_as_read, delete_notification
)
from src.schemas.notifications import (
    NotificationResponse, NotificationCountResponse,
    NotificationMarkReadRequest, NotificationListParams
)
from src.security.auth import get_current_active_user, get_current_superuser
from src.services.notification_service import NotificationService
from src.exceptions import NotFoundException, ServerErrorException

router = APIRouter(prefix="/notifications", tags=["Notifications"])


@router.get("", response_model=List[NotificationResponse])
async def get_notifications_endpoint(
        params: NotificationListParams = Depends(),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get notifications with optional filtering.

    Args:
        params: Query parameters for filtering
        current_user: Current authenticated user
        db: Database session

    Returns:
        List of notifications
    """
    # Only superusers can see all notifications, regular users only see their own
    user_id = None if current_user.is_superuser else current_user.id

    notifications = get_notifications(
        db=db,
        skip=params.skip,
        limit=params.limit,
        user_id=user_id,
        unread_only=params.unread_only,
        notification_type=params.type,
        priority=params.priority
    )

    return notifications


@router.get("/count", response_model=NotificationCountResponse)
async def get_notifications_count(
        unread_only: bool = Query(True, description="Count only unread notifications"),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get count of notifications.

    Args:
        unread_only: Count only unread notifications
        current_user: Current authenticated user
        db: Database session

    Returns:
        Count of notifications
    """
    # Only superusers can see all notifications, regular users only see their own
    user_id = None if current_user.is_superuser else current_user.id

    # Get notifications with the specified filters but no pagination
    notifications = get_notifications(
        db=db,
        skip=0,
        limit=1000,  # Use a high limit
        user_id=user_id,
        unread_only=unread_only
    )

    # Group counts by type
    counts_by_type = {}
    for notification in notifications:
        if notification.type not in counts_by_type:
            counts_by_type[notification.type] = 0
        counts_by_type[notification.type] += 1

    return {
        "total": len(notifications),
        "counts_by_type": counts_by_type
    }


@router.get("/{notification_id}", response_model=NotificationResponse)
async def get_notification_by_id(
        notification_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get a notification by ID.

    Args:
        notification_id: Notification ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Notification details
    """
    notification = get_notification(db, notification_id)

    if not notification:
        raise NotFoundException(f"Notification with ID {notification_id} not found")

    # Check if user has access to this notification
    if not current_user.is_superuser and notification.user_id != current_user.id and notification.user_id is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have access to this notification"
        )

    return notification


@router.post("/{notification_id}/read", response_model=NotificationResponse)
async def mark_notification_read(
        notification_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Mark a notification as read.

    Args:
        notification_id: Notification ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Updated notification
    """
    notification = get_notification(db, notification_id)

    if not notification:
        raise NotFoundException(f"Notification with ID {notification_id} not found")

    # Check if user has access to this notification
    if not current_user.is_superuser and notification.user_id != current_user.id and notification.user_id is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have access to this notification"
        )

    success = mark_notification_as_read(db, notification_id)
    if not success:
        raise ServerErrorException("Failed to mark notification as read")

    # Refresh and return the updated notification
    db.refresh(notification)
    return notification


@router.post("/mark-all-read", response_model=dict)
async def mark_all_notifications_read(
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Mark all notifications as read for the current user.

    Args:
        current_user: Current authenticated user
        db: Database session

    Returns:
        Count of notifications marked as read
    """
    # Only superusers can mark all notifications as read, regular users only their own
    user_id = None if current_user.is_superuser else current_user.id

    count = mark_all_notifications_as_read(db, user_id)

    return {
        "success": True,
        "count": count,
        "message": f"Marked {count} notifications as read"
    }


@router.delete("/{notification_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_notification_endpoint(
        notification_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),  # Only admins can delete
        db: Session = Depends(get_db)
):
    """
    Delete a notification.

    Args:
        notification_id: Notification ID
        current_user: Current authenticated user (must be admin)
        db: Database session
    """
    notification = get_notification(db, notification_id)

    if not notification:
        raise NotFoundException(f"Notification with ID {notification_id} not found")

    success = delete_notification(db, notification_id)
    if not success:
        raise ServerErrorException("Failed to delete notification")
