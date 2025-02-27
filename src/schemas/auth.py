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


class UserCreate(UserBase):
    """User creation schema"""
    password: str = Field(..., min_length=8)


class UserResponse(UserBase):
    """User response schema"""
    id: int
    is_active: bool
    is_superuser: bool
    created_at: datetime

    class Config:
        orm_mode = True


class ApiKeyCreate(BaseModel):
    """API key creation schema"""
    name: str
    expiration_days: Optional[int] = None


class ApiKeyResponse(BaseModel):
    """API key response schema"""
    id: int
    name: str
    key: str
    created_at: datetime
    expiration_date: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    is_active: bool

    class Config:
        orm_mode = True
