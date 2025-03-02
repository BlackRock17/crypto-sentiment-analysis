from datetime import datetime, timedelta
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from src.data_processing.models.auth import User, Token, ApiKey, PasswordReset
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


def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    """
    Authenticates a user via username and password

    Args:
        db: Database session
        username: Username
        password: Password

    Returns:
        User object if authentication is successful, otherwise None
    """
    user = get_user_by_username(db, username)

    if not user:
        return None

    if not verify_password(password, user.hashed_password):
        return None

    return user


def get_active_token(db: Session, token: str) -> Optional[Token]:
    """
    Returns an active token if it exists and has not expired.

    Args:
        db: Database session
        token: JWT token

    Returns:
        Token object if valid, otherwise None
    """
    return db.query(Token).filter(
        and_(
            Token.token == token,
            Token.is_revoked == False,
            or_(Token.expires_at > datetime.utcnow(), Token.expires_at == None)
        )
    ).first()


def revoke_token(db: Session, token: str) -> bool:
    """
    Revokes (deactivates) a token

    Args:
        db: Database session
        token: JWT token

    Returns:
        True if the token was successfully revoked, otherwise False
    """
    db_token = db.query(Token).filter(Token.token == token).first()

    if not db_token:
        return False

    db_token.is_revoked = True
    db.commit()

    return True


def create_api_key(db: Session, user_id: int, name: str, expiration_days: Optional[int] = None) -> ApiKey:
    """
    Creates a new API key for a user

    Args:
        db: Database session
        user_id: User ID
        name: Descriptive API key name
        expiration_days: Number of days until key expires (None for perpetual)

    Returns:
        Newly created ApiKey object
    """
    # Generate a random API key
    import secrets
    key = secrets.token_hex(32)  # 64-character hexadecimal key

    # Specify an expiration date, if provided
    expiration_date = None
    if expiration_days:
        expiration_date = datetime.utcnow() + timedelta(days=expiration_days)

    # Create API key
    db_api_key = ApiKey(
        key=key,
        user_id=user_id,
        name=name,
        expiration_date=expiration_date
    )

    db.add(db_api_key)
    db.commit()
    db.refresh(db_api_key)

    return db_api_key


def get_api_key(db: Session, key: str) -> Optional[ApiKey]:
    """
    Returns an API key by its value

    Args:
        db: Database session
        key: The value of the API key

    Returns:
        ApiKey object if found, otherwise None
    """
    return db.query(ApiKey).filter(ApiKey.key == key).first()


def get_active_api_key(db: Session, key: str) -> Optional[ApiKey]:
    """
    Returns an active API key if it exists and has not expired.

    Args:
        db: Database session
        key: The value of the API key

    Returns:
        ApiKey object if valid, otherwise None
    """
    return db.query(ApiKey).filter(
        and_(
            ApiKey.key == key,
            ApiKey.is_active == True,
            or_(ApiKey.expiration_date > datetime.utcnow(), ApiKey.expiration_date == None)
        )
    ).first()


def update_api_key_usage(db: Session, api_key: ApiKey) -> None:
    """
    Updates the last time the API key was used

    Args:
        db: Database session
        api_key: ApiKey object
    """
    api_key.last_used_at = datetime.utcnow()
    db.commit()


def create_password_reset(db: Session, user_id: int) -> PasswordReset:
    """
    Creates a password reset request

    Args:
        db: Database session
        user_id: The user ID for whom to create the reset

    Returns:
        Newly created PasswordReset object
    """
    # First, invalidate any existing reset codes for this user
    db.query(PasswordReset).filter(
        PasswordReset.user_id == user_id,
        PasswordReset.is_used == False
    ).update({"is_used": True})

    # Generate a unique reset code
    import secrets
    reset_code = secrets.token_urlsafe(32)

    # Set expiration (24 hours from now)
    expires_at = datetime.utcnow() + timedelta(hours=24)

    # Create new password reset record
    db_reset = PasswordReset(
        user_id=user_id,
        reset_code=reset_code,
        expires_at=expires_at
    )

    db.add(db_reset)
    db.commit()
    db.refresh(db_reset)

    return db_reset


def get_valid_password_reset(db: Session, reset_code: str) -> Optional[PasswordReset]:
    """
    Get a valid password reset request by its code

    Args:
        db: Database session
        reset_code: The unique reset code

    Returns:
        PasswordReset object if valid and not expired, otherwise None
    """
    return db.query(PasswordReset).filter(
        PasswordReset.reset_code == reset_code,
        PasswordReset.is_used == False,
        PasswordReset.expires_at > datetime.utcnow()
    ).first()