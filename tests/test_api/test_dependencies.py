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


def test_get_current_user_valid_token(db: Session):
    """Test get_current_user with valid token"""
    # Create a test user
    timestamp = datetime.utcnow().timestamp()
    username = f"dep_test_user_{timestamp}"

    user = create_user(
        db=db,
        username=username,
        email=f"dep_test_{timestamp}@example.com",
        password="testpassword"
    )

    # Create a valid token
    token_data = {"sub": username, "user_id": user.id}
    token = create_access_token(token_data)

    # Test get_current_user
    current_user = get_current_user(token=token, db=db)

    assert current_user is not None
    assert current_user.id == user.id
    assert current_user.username == username

    # Clean up
    db.delete(user)
    db.commit()

    print("✓ Successfully tested get_current_user with valid token")


def test_get_current_user_invalid_token(db: Session):
    """Test get_current_user with invalid token"""
    # Create an invalid token with wrong signature
    payload = {
        "sub": "fake_user",
        "user_id": 999,
        "exp": datetime.utcnow() + timedelta(minutes=30)
    }
    invalid_token = jwt.encode(payload, "wrong_secret", algorithm=JWT_ALGORITHM)

    # Test get_current_user with invalid token
    with pytest.raises(HTTPException) as exc_info:
        get_current_user(token=invalid_token, db=db)

    assert exc_info.value.status_code == 401

    print("✓ Successfully tested get_current_user with invalid token")


def test_get_current_user_expired_token(db: Session):
    """Test get_current_user with expired token"""
    # Create a test user
    timestamp = datetime.utcnow().timestamp()
    username = f"exp_test_user_{timestamp}"

    user = create_user(
        db=db,
        username=username,
        email=f"exp_test_{timestamp}@example.com",
        password="testpassword"
    )

    # Create an expired token
    token_data = {"sub": username, "user_id": user.id}
    expired_delta = timedelta(minutes=-30)  # Negative delta for expiration in the past
    token = create_access_token(token_data, expired_delta)

    # Test get_current_user with expired token
    with pytest.raises(HTTPException) as exc_info:
        get_current_user(token=token, db=db)

    assert exc_info.value.status_code == 401

    # Clean up
    db.delete(user)
    db.commit()

    print("✓ Successfully tested get_current_user with expired token")