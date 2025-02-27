from datetime import datetime, timedelta
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from config.settings import ACCESS_TOKEN_EXPIRE_MINUTES
from src.data_processing.crud.auth import (
    create_user, authenticate_user, create_api_key,
    get_user_by_username, get_user_by_email
)
from src.data_processing.database import get_db
from src.schemas.auth import (
    UserCreate, UserResponse, Token, ApiKeyCreate, ApiKeyResponse
)
from src.security.auth import get_current_active_user, get_current_superuser
from src.security.utils import create_user_token
from src.data_processing.models.auth import User

router = APIRouter(prefix="/auth", tags=["Authentication"])