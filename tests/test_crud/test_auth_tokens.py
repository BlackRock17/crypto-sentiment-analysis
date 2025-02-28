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


@pytest.fixture
def test_token(db: Session, test_user: User):
    """Create a test token"""
    token = create_user_token(db, test_user)

    yield token

    # Clean up
    db.delete(token)
    db.commit()


def test_create_user_token(db: Session, test_user: User):
    """Test creating a user token"""
    token = create_user_token(db, test_user)

    assert token is not None
    assert token.user_id == test_user.id
    assert token.token_type == "bearer"
    assert token.is_revoked == False
    assert token.expires_at > datetime.utcnow()

    # Clean up
    db.delete(token)
    db.commit()

    print("✓ Successfully created and verified user token")


def test_get_active_token(db: Session, test_token: Token):
    """Test retrieving an active token"""
    token = get_active_token(db, test_token.token)

    assert token is not None
    assert token.id == test_token.id
    assert token.user_id == test_token.user_id

    print("✓ Successfully retrieved active token")


def test_revoke_token(db: Session, test_token: Token):
    """Test revoking a token"""
    result = revoke_token(db, test_token.token)

    assert result is True

    # Verify the token is now revoked
    token = db.query(Token).filter(Token.id == test_token.id).first()
    assert token.is_revoked == True

    print("✓ Successfully revoked token")


def test_expired_token_not_active(db: Session, test_user: User):
    """Test that an expired token is not considered active"""
    # Create a token that's already expired
    expiration = datetime.utcnow() - timedelta(minutes=5)

    expired_token = Token(
        token="expired_test_token",
        token_type="bearer",
        expires_at=expiration,
        user_id=test_user.id
    )

    db.add(expired_token)
    db.commit()

    # Test that get_active_token doesn't return the expired token
    active_token = get_active_token(db, "expired_test_token")
    assert active_token is None

    # Clean up
    db.delete(expired_token)
    db.commit()

    print("✓ Successfully verified expired token is not active")


def test_create_api_key(db: Session, test_user: User):
    """Test creating an API key"""
    api_key = create_api_key(
        db=db,
        user_id=test_user.id,
        name="Test API Key"
    )

    assert api_key is not None
    assert api_key.user_id == test_user.id
    assert api_key.name == "Test API Key"
    assert api_key.is_active == True
    assert api_key.key is not None
    assert len(api_key.key) == 64  # 64-character hexadecimal key

    # Clean up
    db.delete(api_key)
    db.commit()

    print("✓ Successfully created and verified API key")


def test_create_expiring_api_key(db: Session, test_user: User):
    """Test creating an API key with expiration"""
    api_key = create_api_key(
        db=db,
        user_id=test_user.id,
        name="Expiring API Key",
        expiration_days=7
    )

    assert api_key is not None
    assert api_key.user_id == test_user.id
    assert api_key.expiration_date is not None
    assert api_key.expiration_date > datetime.utcnow()
    assert api_key.expiration_date < (datetime.utcnow() + timedelta(days=8))

    # Clean up
    db.delete(api_key)
    db.commit()

    print("✓ Successfully created and verified expiring API key")


def test_get_api_key(db: Session, test_user: User):
    """Test retrieving an API key"""
    # Create a test API key
    api_key = create_api_key(
        db=db,
        user_id=test_user.id,
        name="Retrievable API Key"
    )

    # Retrieve the API key
    retrieved_key = get_api_key(db, api_key.key)

    assert retrieved_key is not None
    assert retrieved_key.id == api_key.id
    assert retrieved_key.user_id == test_user.id

    # Clean up
    db.delete(api_key)
    db.commit()

    print("✓ Successfully retrieved API key")


def test_get_active_api_key(db: Session, test_user: User):
    """Test retrieving an active API key"""
    # Create a test API key
    api_key = create_api_key(
        db=db,
        user_id=test_user.id,
        name="Active API Key"
    )

    # Retrieve the active API key
    active_key = get_active_api_key(db, api_key.key)

    assert active_key is not None
    assert active_key.id == api_key.id
    assert active_key.user_id == test_user.id

    # Clean up
    db.delete(api_key)
    db.commit()

    print("✓ Successfully retrieved active API key")


def test_update_api_key_usage(db: Session, test_user: User):
    """Test updating the last used time of an API key"""
    # Create a test API key
    api_key = create_api_key(
        db=db,
        user_id=test_user.id,
        name="Usage API Key"
    )

    # Record original last_used_at (should be None)
    assert api_key.last_used_at is None

    # Wait a moment to ensure there's a time difference
    time.sleep(0.1)

    # Update the usage
    update_api_key_usage(db, api_key)

    # Verify last_used_at was updated
    updated_key = get_api_key(db, api_key.key)
    assert updated_key.last_used_at is not None

    # Clean up
    db.delete(api_key)
    db.commit()

    print("✓ Successfully updated API key usage timestamp")


def test_expired_api_key_not_active(db: Session, test_user: User):
    """Test that an expired API key is not considered active"""
    # Create an API key that's already expired
    expired_api_key = ApiKey(
        key="expired_test_api_key",
        user_id=test_user.id,
        name="Expired API Key",
        expiration_date=datetime.utcnow() - timedelta(days=1),
        is_active=True
    )

    db.add(expired_api_key)
    db.commit()

    # Test that get_active_api_key doesn't return the expired key
    active_key = get_active_api_key(db, "expired_test_api_key")
    assert active_key is None

    # Clean up
    db.delete(expired_api_key)
    db.commit()

    print("✓ Successfully verified expired API key is not active")


def test_inactive_api_key_not_active(db: Session, test_user: User):
    """Test that an inactive API key is not considered active"""
    # Create an inactive API key
    inactive_api_key = ApiKey(
        key="inactive_test_api_key",
        user_id=test_user.id,
        name="Inactive API Key",
        is_active=False
    )

    db.add(inactive_api_key)
    db.commit()

    # Test that get_active_api_key doesn't return the inactive key
    active_key = get_active_api_key(db, "inactive_test_api_key")
    assert active_key is None

    # Clean up
    db.delete(inactive_api_key)
    db.commit()

    print("✓ Successfully verified inactive API key is not active")
