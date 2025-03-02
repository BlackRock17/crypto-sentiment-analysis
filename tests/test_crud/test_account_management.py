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


def test_update_user(db: Session, test_user):
    """Test updating a user's information"""
    # Update username
    new_username = f"updated_{test_user.username}"
    updated_user = update_user(db, test_user.id, username=new_username)

    assert updated_user is not None
    assert updated_user.username == new_username
    assert updated_user.email == test_user.email

    # Update email
    new_email = f"updated_{test_user.email}"
    updated_user = update_user(db, test_user.id, email=new_email)

    assert updated_user is not None
    assert updated_user.username == new_username  # From previous update
    assert updated_user.email == new_email

    # Update both
    final_username = f"final_{test_user.username}"
    final_email = f"final_{test_user.email}"
    updated_user = update_user(db, test_user.id, username=final_username, email=final_email)

    assert updated_user is not None
    assert updated_user.username == final_username
    assert updated_user.email == final_email

    print("✓ Successfully updated user information")


def test_update_user_duplicate_username(db: Session, test_user):
    """Test updating a user with a duplicate username"""
    # Create another user
    timestamp = datetime.utcnow().timestamp()
    other_username = f"other_user_{timestamp}"
    other_email = f"other_{timestamp}@example.com"

    other_user = create_user(
        db=db,
        username=other_username,
        email=other_email,
        password="otherpassword"
    )

    # Try to update test_user with other_user's username
    with pytest.raises(ValueError) as exc_info:
        update_user(db, test_user.id, username=other_username)

    assert "Username already taken" in str(exc_info.value)

    # Clean up
    db.delete(other_user)
    db.commit()

    print("✓ Successfully prevented duplicate username update")


def test_update_user_duplicate_email(db: Session, test_user):
    """Test updating a user with a duplicate email"""
    # Create another user
    timestamp = datetime.utcnow().timestamp()
    other_username = f"other_user_{timestamp}"
    other_email = f"other_{timestamp}@example.com"

    other_user = create_user(
        db=db,
        username=other_username,
        email=other_email,
        password="otherpassword"
    )

    # Try to update test_user with other_user's email
    with pytest.raises(ValueError) as exc_info:
        update_user(db, test_user.id, email=other_email)

    assert "Email already taken" in str(exc_info.value)

    # Clean up
    db.delete(other_user)
    db.commit()

    print("✓ Successfully prevented duplicate email update")


def test_deactivate_user(db: Session, test_user):
    """Test deactivating a user account"""
    # Deactivate the user
    result = deactivate_user(db, test_user.id)

    assert result is True

    # Verify the user is now inactive
    user = get_user_by_id(db, test_user.id)
    assert user.is_active == False

    print("✓ Successfully deactivated user account")