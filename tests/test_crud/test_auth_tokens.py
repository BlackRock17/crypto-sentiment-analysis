import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import time

from src.data_processing.crud.auth import (
    create_user, get_active_token, revoke_token,
    create_api_key, get_api_key, get_active_api_key,
    update_api_key_usage, get_user_by_username
)
from src.data_processing.models.auth import User, Token, ApiKey
from src.data_processing.database import get_db
from src.security.utils import create_user_token


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_user(db: Session):
    """Create a test user"""
    # Check if test user already exists
    existing_user = get_user_by_username(db, "tokenuser")
    if existing_user:
        return existing_user

    # Create a new test user
    user = create_user(
        db=db,
        username="tokenuser",
        email="token@example.com",
        password="tokenpassword"
    )

    yield user

    # Clean up - we'll keep the test user for now as tokens reference it
    # In real applications, you might want to use a transaction that rolls back