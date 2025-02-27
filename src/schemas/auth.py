from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, EmailStr, Field


class Token(BaseModel):
    """Token response schema"""
    access_token: str
    token_type: str
    expires_at: datetime


class TokenData(BaseModel):
    """Token data schema"""
    user_id: Optional[int] = None
    username: Optional[str] = None


class UserBase(BaseModel):
    """Base user schema"""
    username: str
    email: EmailStr