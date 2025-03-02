import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_processing.crud.auth import (
    create_user, create_password_reset, get_valid_password_reset,
    mark_password_reset_used, update_user_password, get_user_by_id,
    authenticate_user
)
from src.data_processing.models.auth import User, PasswordReset
from src.security.utils import verify_password


@pytest.fixture
def db():
    """Database session fixture"""
    session = next(get_db())
    yield session
    session.close()


@pytest.fixture
def test_user(db: Session):
    """Create a test user for password reset tests"""
    # Generate a unique username to avoid conflicts
    timestamp = datetime.utcnow().timestamp()
    username = f"reset_user_{timestamp}"
    email = f"reset_{timestamp}@example.com"
    password = "originalpassword"

    # Create a new test user
    user = create_user(
        db=db,
        username=username,
        email=email,
        password=password
    )

    yield user, password

    # Clean up - delete the test user
    db.delete(user)
    db.commit()