"""
API schemas for notifications.
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field

from src.data_processing.models.notifications import NotificationType, NotificationPriority


class NotificationBase(BaseModel):
    """Base schema for notification data"""
    type: str
    title: str
    message: str
    priority: str = Field(default="medium")
    metadata: Dict[str, Any] = {}


class NotificationCreate(NotificationBase):
    """Schema for creating a new notification"""
    user_id: Optional[int] = None


class NotificationResponse(NotificationBase):
    """Schema for notification response"""
    id: int
    created_at: datetime
    is_read: bool
    user_id: Optional[int] = None

    model_config = {
        "from_attributes": True
    }


class NotificationCountResponse(BaseModel):
    """Schema for notification count response"""
    total: int
    counts_by_type: Dict[str, int]


class NotificationMarkReadRequest(BaseModel):
    """Schema for marking notifications as read"""
    notification_ids: List[int] = []
    mark_all: bool = False


class NotificationListParams(BaseModel):
    """Query parameters for listing notifications"""
    skip: int = Field(0, ge=0)
    limit: int = Field(20, ge=1, le=100)
    unread_only: bool = False
    type: Optional[str] = None
    priority: Optional[str] = None

    model_config = {
        "extra": "allow"
    }
