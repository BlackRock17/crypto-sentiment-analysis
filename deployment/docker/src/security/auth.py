from datetime import datetime
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from config.settings import JWT_SECRET_KEY, JWT_ALGORITHM
from src.data_processing.crud.auth import get_active_token, get_user_by_id, get_active_api_key, update_api_key_usage
from src.data_processing.database import get_db
from src.data_processing.models.auth import User

# OAuth2 scheme for retrieving the token from the Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

# API Key schema for extracting the API key from the X-API-Key header
api_key_header = APIKeyHeader(name="X-API-Key")


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    """
    Retrieves the current user based on a JWT token

    Args:
        token: JWT token from Authorization header
        db: Database session

    Returns:
        The user object

    Raises:
        HTTPException: If the token is invalid or expired
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        # Decoding the JWT token
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        user_id: str = payload.get("user_id")
        if user_id is None:
            raise credentials_exception

        # Checking if the token is in the database and active
        db_token = get_active_token(db, token)
        if not db_token:
            raise credentials_exception

    except JWTError:
        raise credentials_exception

    # Retrieving the user from the database
    user = get_user_by_id(db, int(user_id))
    if user is None or not user.is_active:
        raise credentials_exception

    return user


def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """
    Checks if the current user is active

    Args:
        current_user: User object

    Returns:
        The user object

    Raises:
        HTTPException: If the user is not active
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user"
        )
    return current_user


def get_current_superuser(current_user: User = Depends(get_current_active_user)) -> User:
    """
    Checks if the current user is a superuser (admin)

    Args:
        current_user: User object

    Returns:
        The user object

    Raises:
        HTTPException: If the user is not a superuser
    """
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    return current_user


def get_user_by_api_key(api_key: str = Depends(api_key_header), db: Session = Depends(get_db)) -> User:
    """
    Retrieves a user based on an API key

    Args:
        api_key: API key from X-API-Key header
        db: Database session

    Returns:
        The user object

    Raises:
        HTTPException: If the API key is invalid
    """
    api_key_obj = get_active_api_key(db, api_key)

    if not api_key_obj:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
            headers={"WWW-Authenticate": "APIKey"},
        )

    # Update last used time
    update_api_key_usage(db, api_key_obj)

    # Retrieve user
    user = get_user_by_id(db, api_key_obj.user_id)

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key associated with inactive user",
            headers={"WWW-Authenticate": "APIKey"},
        )

    return user
