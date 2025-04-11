"""
Tweet data validation schemes.
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TweetCreate(BaseModel):
    """Scheme for manually adding a tweet."""
    tweet_id: str
    text: str
    created_at: Optional[datetime] = None

    class Config:
        schema_extra = {
            "example": {
                "tweet_id": "12345",
                "text": "tweet text",
                "created_at": "2025-04-11T12:00:00Z"
            }
        }
