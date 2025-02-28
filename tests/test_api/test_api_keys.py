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