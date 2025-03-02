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
