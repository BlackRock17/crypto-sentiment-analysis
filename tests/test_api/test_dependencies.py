import pytest
from fastapi import HTTPException
from jose import jwt
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.crud.auth import create_user, create_api_key
from src.security.auth import get_current_user, get_current_active_user, get_user_by_api_key
from src.security.utils import create_access_token
from config.settings import JWT_SECRET_KEY, JWT_ALGORITHM


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()