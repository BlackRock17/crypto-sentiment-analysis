from datetime import datetime, timedelta
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from config.settings import ACCESS_TOKEN_EXPIRE_MINUTES
from src.data_processing.crud.auth import (
    create_user, authenticate_user, create_api_key,
    get_user_by_username, get_user_by_email, create_password_reset,
    get_valid_password_reset, mark_password_reset_used, update_user_password
)
from src.data_processing.database import get_db
from src.schemas.auth import (
    UserCreate, UserResponse, Token, ApiKeyCreate, ApiKeyResponse,
    PasswordResetRequest, PasswordResetConfirm, PasswordChange
)
from src.security.auth import get_current_active_user, get_current_superuser
from src.security.utils import create_user_token
from src.data_processing.models.auth import User

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/token", response_model=Token)
async def login_for_access_token(
        form_data: OAuth2PasswordRequestForm = Depends(),
        db: Session = Depends(get_db)
):
    """
    OAuth2 compatible token login endpoint
    """
    # Authenticate user
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create access token
    token = create_user_token(db, user)

    return {
        "access_token": token.token,
        "token_type": token.token_type,
        "expires_at": token.expires_at
    }


@router.post("/signup", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(
        user_data: UserCreate,
        db: Session = Depends(get_db)
):
    """
    Register a new user
    """
    # Check if username already exists
    existing_user = get_user_by_username(db, user_data.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    # Check if email already exists
    existing_email = get_user_by_email(db, user_data.email)
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Create new user
    user = create_user(
        db=db,
        username=user_data.username,
        email=user_data.email,
        password=user_data.password
    )

    return user


@router.get("/me", response_model=UserResponse)
async def read_users_me(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get current user information
    """
    return current_user


@router.post("/api-keys", response_model=ApiKeyResponse)
async def create_new_api_key(
        api_key_data: ApiKeyCreate,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Create a new API key for the current user
    """
    api_key = create_api_key(
        db=db,
        user_id=current_user.id,
        name=api_key_data.name,
        expiration_days=api_key_data.expiration_days
    )

    return api_key


@router.get("/users", response_model=List[UserResponse])
async def read_users(
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_superuser),
    db: Session = Depends(get_db)
):
    """
    Get all users (admin only)
    """
    users = db.query(User).offset(skip).limit(limit).all()
    return users


@router.post("/password-reset/request", status_code=status.HTTP_202_ACCEPTED)
async def request_password_reset(
        reset_request: PasswordResetRequest,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db)
):
    """
    Request a password reset link

    This endpoint will send a password reset email with a unique reset code
    """
    # Find user by email
    user = get_user_by_email(db, reset_request.email)

    # Always return success even if email doesn't exist (security best practice)
    if not user:
        return {"message": "If this email exists in our system, you will receive a password reset link"}

    # Create password reset request
    reset = create_password_reset(db, user.id)

    # In a real-world application, here you would send an email with the reset code
    # For this example, we'll just return the reset code (in a real app, don't do this!)
    # background_tasks.add_task(send_password_reset_email, user.email, reset.reset_code)

    return {
        "message": "If this email exists in our system, you will receive a password reset link",
        "reset_code": reset.reset_code  # Remove this in production!
    }


@router.post("/password-reset/confirm", status_code=status.HTTP_200_OK)
async def confirm_password_reset(
        reset_confirm: PasswordResetConfirm,
        db: Session = Depends(get_db)
):
    """
    Confirm a password reset with the reset code and set a new password
    """
    # Find valid reset request
    reset = get_valid_password_reset(db, reset_confirm.reset_code)

    if not reset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset code"
        )

    # Update user's password
    success = update_user_password(db, reset.user_id, reset_confirm.new_password)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password"
        )

    # Mark reset code as used
    mark_password_reset_used(db, reset_confirm.reset_code)

    return {"message": "Password has been reset successfully"}


