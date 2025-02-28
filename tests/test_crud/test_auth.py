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


def test_get_user_by_username(db: Session, test_user: User):
    """Test retrieving a user by username"""
    user = get_user_by_username(db, test_user.username)

    assert user is not None
    assert user.id == test_user.id
    assert user.email == test_user.email

    print("✓ Successfully retrieved user by username")


def test_get_user_by_email(db: Session, test_user: User):
    """Test retrieving a user by email"""
    user = get_user_by_email(db, test_user.email)

    assert user is not None
    assert user.id == test_user.id
    assert user.username == test_user.username

    print("✓ Successfully retrieved user by email")


def test_get_user_by_id(db: Session, test_user: User):
    """Test retrieving a user by ID"""
    user = get_user_by_id(db, test_user.id)

    assert user is not None
    assert user.username == test_user.username
    assert user.email == test_user.email

    print("✓ Successfully retrieved user by ID")


def test_authenticate_user(db: Session, test_user: User):
    """Test authenticating a user with correct credentials"""
    user = authenticate_user(db, test_user.username, "testpassword")

    assert user is not None
    assert user.id == test_user.id
    assert user.username == test_user.username

    print("✓ Successfully authenticated user with correct credentials")


def test_authenticate_user_wrong_password(db: Session, test_user: User):
    """Test authenticating a user with incorrect password"""
    user = authenticate_user(db, test_user.username, "wrongpassword")

    assert user is None

    print("✓ Successfully rejected authentication with wrong password")


def test_authenticate_user_wrong_username(db: Session):
    """Test authenticating a non-existent user"""
    user = authenticate_user(db, "nonexistentuser", "password")

    assert user is None

    print("✓ Successfully rejected authentication for non-existent user")