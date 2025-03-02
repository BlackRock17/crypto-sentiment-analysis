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

    model_config = {
        "from_attributes": True
    }


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


class PasswordResetRequest(BaseModel):
    """Schema for requesting a password reset"""
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Schema for confirming a password reset"""
    reset_code: str
    new_password: str = Field(..., min_length=8)


class PasswordChange(BaseModel):
    """Schema for changing password (when user is logged in)"""
    current_password: str
    new_password: str = Field(..., min_length=8)


class UserUpdate(BaseModel):
    """Schema for updating user information"""
    email: Optional[EmailStr] = None
    username: Optional[str] = None


class UserProfileResponse(UserResponse):
    """Extended user response schema with profile information"""
    last_login: Optional[datetime] = None
    api_keys_count: int = 0
    account_created_at: datetime


class AccountDeactivateRequest(BaseModel):
    """Schema for account deactivation"""
    password: str
    reason: Optional[str] = None