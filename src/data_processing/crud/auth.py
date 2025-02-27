from datetime import datetime
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from src.data_processing.models.auth import User, Token, ApiKey
from src.security.utils import get_password_hash, verify_password


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """
    Returns a user by username

    Args:
        db: Database session
        username: Username

    Returns:
        User object or None if not found
    """
    return db.query(User).filter(User.username == username).first()


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    """
    Returns a user by email

    Args:
        db: Database session
        email: Email address

    Returns:
        User object or None if not found
    """
    return db.query(User).filter(User.email == email).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """
    Returns a user by ID

    Args:
        db: Database session
        user_id: User ID

    Returns:
        User object or None if not found
    """
    return db.query(User).filter(User.id == user_id).first()


def create_user(db: Session, username: str, email: str, password: str, is_superuser: bool = False) -> User:
    """
    Creates a new user

    Args:
        db: Database session
        username: Username
        email: Email address
        password: Password (will be hashed)
        is_superuser: Whether the user is an administrator

    Returns:
        The newly created user object
    """
    # Password hashing
    hashed_password = get_password_hash(password)

    # Create user
    db_user = User(
        username=username,
        email=email,
        hashed_password=hashed_password,
        is_superuser=is_superuser
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user