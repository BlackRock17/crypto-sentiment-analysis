import pytest
from datetime import datetime
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.crud.auth import (
    create_user, update_user, deactivate_user,
    get_user_by_id, get_user_by_username, get_user_by_email,
    create_api_key, get_user_api_keys_count
)


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_user(db: Session):
    """Create a test user for account management tests"""
    timestamp = datetime.utcnow().timestamp()
    username = f"account_user_{timestamp}"
    email = f"account_{timestamp}@example.com"

    user = create_user(
        db=db,
        username=username,
        email=email,
        password="testpassword"
    )

    yield user

    # Clean up - delete the test user
    try:
        db.delete(user)
        db.commit()
    except:
        db.rollback()