from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from config.settings import JWT_SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from src.data_processing.models.auth import User, Token

# Initialize a password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Checks if the password matches the hashed password"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generates a hashed password"""
    return pwd_context.hash(password)


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Creates a JWT access token

    Args:
        data: The data that will be encoded in the token
        expires_delta: Token validity time

    Returns:
        Encrypted JWT token
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

    return encoded_jwt


def create_user_token(db: Session, user: User) -> Token:
    """
    Creates a token for the user and saves it to the database

    Args:
        db: Database session
        user: The user for whom the token is being created

    Returns:
        Token with the newly created token
    """
    # Determining the validity period
    expiration = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    # Creating a JWT token
    token_data = {"sub": user.username, "user_id": user.id}
    access_token = create_access_token(token_data)

    # Saving the token to the database
    db_token = Token(
        token=access_token,
        token_type="bearer",
        expires_at=expiration,
        user_id=user.id
    )

    db.add(db_token)
    db.commit()
    db.refresh(db_token)

    return db_token