import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime

from src.main import app
from src.data_processing.database import get_db
from src.data_processing.crud.auth import create_user, get_user_by_username


client = TestClient(app)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


def test_login_endpoint_valid_credentials(db: Session):
    """Test login endpoint with valid credentials"""
    # Create a test user
    timestamp = datetime.utcnow().timestamp()
    username = f"api_test_user_{timestamp}"
    password = "testpassword"

    user = create_user(
        db=db,
        username=username,
        email=f"api_test_{timestamp}@example.com",
        password=password
    )

    # Try to log in
    response = client.post(
        "/auth/token",
        data={"username": username, "password": password}
    )

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert "expires_at" in data

    # Clean up
    db.delete(user)
    db.commit()

    print("✓ Successfully tested valid login")