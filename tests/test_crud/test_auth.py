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


def test_create_user(db: Session):
    """Test creating a new user"""
    # Create a user with unique username/email
    username = f"testuser_{datetime.utcnow().timestamp()}"
    email = f"test_{datetime.utcnow().timestamp()}@example.com"

    user = create_user(
        db=db,
        username=username,
        email=email,
        password="testpassword"
    )

    # Verify the user was created correctly
    assert user.username == username
    assert user.email == email
    assert user.is_active == True
    assert user.is_superuser == False
    assert verify_password("testpassword", user.hashed_password)

    # Clean up
    db.delete(user)
    db.commit()

    print("✓ Successfully created and verified user")