import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime

from src.main import app
from src.data_processing.database import get_db
from src.data_processing.crud.auth import (
    create_user, create_password_reset, get_valid_password_reset,
    get_user_by_id
)
from src.security.utils import verify_password

client = TestClient(app)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_user(db: Session):
    """Create a test user"""
    timestamp = datetime.utcnow().timestamp()
    username = f"api_account_user_{timestamp}"
    email = f"api_account_{timestamp}@example.com"
    password = "testpassword"

    user = create_user(
        db=db,
        username=username,
        email=email,
        password=password
    )

    yield user, username, email, password

    # Clean up
    db.delete(user)
    db.commit()


@pytest.fixture
def auth_headers(test_user):
    """Get auth headers for the test user"""
    _, username, _, password = test_user

    # Login to get token
    login_response = client.post(
        "/auth/token",
        data={"username": username, "password": password}
    )

    token_data = login_response.json()
    headers = {"Authorization": f"Bearer {token_data['access_token']}"}

    return headers


def test_password_reset_request(db: Session, test_user):
    """Test requesting a password reset"""
    _, _, email, _ = test_user

    # Request password reset
    response = client.post(
        "/auth/password-reset/request",
        json={"email": email}
    )

    # Check response
    assert response.status_code == 202
    data = response.json()
    assert "message" in data
    assert "reset_code" in data  # Note: only in development mode

    # Verify reset code was created in DB
    reset_code = data["reset_code"]
    reset = get_valid_password_reset(db, reset_code)

    assert reset is not None
    assert reset.user_id == test_user[0].id

    # Clean up
    db.delete(reset)
    db.commit()

    print("✓ Successfully tested password reset request")


def test_password_reset_confirm(db: Session, test_user):
    """Test confirming a password reset"""
    user, username, _, original_password = test_user

    # Create a password reset
    reset = create_password_reset(db, user.id)

    # Confirm password reset
    new_password = "newpassword123"
    response = client.post(
        "/auth/password-reset/confirm",
        json={
            "reset_code": reset.reset_code,
            "new_password": new_password
        }
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "message" in data

    # Verify password was changed
    db.refresh(user)
    assert verify_password(new_password, user.hashed_password)
    assert not verify_password(original_password, user.hashed_password)

    # Verify reset code is marked as used
    db.refresh(reset)
    assert reset.is_used == True

    # Verify login works with new password
    login_response = client.post(
        "/auth/token",
        data={"username": username, "password": new_password}
    )
    assert login_response.status_code == 200

    # Clean up
    db.delete(reset)
    db.commit()

    print("✓ Successfully tested password reset confirmation")


def test_password_change(test_user, auth_headers):
    """Test changing password when logged in"""
    user, username, _, current_password = test_user

    # Change password
    new_password = "updatedpassword123"
    response = client.post(
        "/auth/password-change",
        json={
            "current_password": current_password,
            "new_password": new_password
        },
        headers=auth_headers
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "message" in data

    # Verify login works with new password
    login_response = client.post(
        "/auth/token",
        data={"username": username, "password": new_password}
    )
    assert login_response.status_code == 200

    print("✓ Successfully tested password change")


def test_get_user_profile(auth_headers):
    """Test getting user profile"""
    response = client.get(
        "/auth/profile",
        headers=auth_headers
    )

    # Check response
    assert response.status_code == 200
    data = response.json()

    # Verify profile data
    assert "id" in data
    assert "username" in data
    assert "email" in data
    assert "is_active" in data
    assert "api_keys_count" in data
    assert "last_login" in data
    assert "account_created_at" in data

    print("✓ Successfully tested get user profile")


def test_update_user_profile(db: Session, test_user, auth_headers):
    """Test updating user profile"""
    user, _, _, _ = test_user

    # Update profile
    new_username = f"updated_{user.username}"
    response = client.put(
        "/auth/profile",
        json={"username": new_username},
        headers=auth_headers
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["username"] == new_username

    # Verify change in DB
    db.refresh(user)
    assert user.username == new_username

    print("✓ Successfully tested update user profile")


def test_deactivate_account(db: Session, test_user, auth_headers):
    """Test deactivating user account"""
    user, _, _, password = test_user

    # Deactivate account
    response = client.post(
        "/auth/deactivate",
        json={"password": password},
        headers=auth_headers
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "message" in data

    # Verify account is deactivated
    db.refresh(user)
    assert user.is_active == False

    print("✓ Successfully tested account deactivation")
