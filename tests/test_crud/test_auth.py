import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.data_processing.crud.auth import (
    get_user_by_username, get_user_by_email, get_user_by_id,
    create_user, authenticate_user, get_active_token,
    revoke_token, create_api_key, get_api_key, get_active_api_key
)
from src.data_processing.models.auth import User, Token, ApiKey
from src.data_processing.database import get_db
from src.security.utils import get_password_hash, verify_password


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
    existing_user = get_user_by_username(db, "testuser")
    if existing_user:
        return existing_user

    # Create a new test user
    user = create_user(
        db=db,
        username="testuser",
        email="test@example.com",
        password="testpassword"
    )

    yield user

    # Clean up - remove test user
    db.delete(user)
    db.commit()