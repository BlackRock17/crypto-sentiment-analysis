import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime

from src.main import app
from src.data_processing.database import get_db
from src.data_processing.crud.auth import create_user

client = TestClient(app)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def auth_headers(db: Session):
    """Create a user and get auth headers for tests"""
    timestamp = datetime.utcnow().timestamp()
    username = f"apikey_test_user_{timestamp}"
    password = "testpassword"

    # Create a test user
    user = create_user(
        db=db,
        username=username,
        email=f"apikey_test_{timestamp}@example.com",
        password=password
    )

    # Login to get token
    login_response = client.post(
        "/auth/token",
        data={"username": username, "password": password}
    )

    token_data = login_response.json()
    headers = {"Authorization": f"Bearer {token_data['access_token']}"}

    yield headers, user

    # Clean up
    db.delete(user)
    db.commit()


def test_create_api_key(auth_headers):
    """Test creating a new API key"""
    headers, user = auth_headers

    # Create API key data
    api_key_data = {
        "name": "Test API Key",
        "expiration_days": 30
    }

    # Make the request
    response = client.post(
        "/auth/api-keys",
        json=api_key_data,
        headers=headers
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test API Key"
    assert "key" in data
    assert "expiration_date" in data
    assert data["is_active"] == True

    print("✓ Successfully tested API key creation")