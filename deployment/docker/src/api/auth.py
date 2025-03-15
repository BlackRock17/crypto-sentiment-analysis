from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from config.settings import ACCESS_TOKEN_EXPIRE_MINUTES
from src.data_processing.crud.auth import (
    create_user, authenticate_user, create_api_key,
    get_user_by_username, get_user_by_email, create_password_reset,
    get_valid_password_reset, mark_password_reset_used, update_user_password,
    update_user, deactivate_user, get_user_api_keys_count, get_user_last_login
)
from src.data_processing.database import get_db
from src.schemas.auth import (
    UserCreate, UserResponse, Token, ApiKeyCreate, ApiKeyResponse,
    PasswordResetRequest, PasswordResetConfirm, PasswordChange,
    UserUpdate, UserProfileResponse, AccountDeactivateRequest
)
from src.exceptions import (
    BadRequestException, UnauthorizedException, ForbiddenException,
    NotFoundException, ConflictException, ServerErrorException
)
from src.security.auth import get_current_active_user, get_current_superuser
from src.security.utils import create_user_token, verify_password, get_password_hash
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
        raise UnauthorizedException(detail="Incorrect username or password")

    # Check if user is active
    if not user.is_active:
        raise ForbiddenException(detail="User account is disabled")

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
        raise BadRequestException(detail="Username already registered")

    # Check if email already exists
    existing_email = get_user_by_email(db, user_data.email)
    if existing_email:
        raise BadRequestException(detail="Email already registered")

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
        raise BadRequestException(detail="Invalid or expired reset code")

    # Update user's password
    success = update_user_password(db, reset.user_id, reset_confirm.new_password)

    if not success:
        raise ServerErrorException(detail="Failed to update password")

    # Mark reset code as used
    mark_password_reset_used(db, reset_confirm.reset_code)

    return {"message": "Password has been reset successfully"}


@router.post("/password-change", status_code=status.HTTP_200_OK)
async def change_password(
        password_change: PasswordChange,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Change password for the currently logged in user
    """
    # Verify current password
    if not verify_password(password_change.current_password, current_user.hashed_password):
        raise BadRequestException(detail="Current password is incorrect")

    # Update password
    current_user.hashed_password = get_password_hash(password_change.new_password)
    db.commit()

    return {"message": "Password changed successfully"}


@router.get("/profile", response_model=UserProfileResponse)
async def get_user_profile(
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get detailed profile information for the current user
    """
    # Get additional user information
    api_keys_count = get_user_api_keys_count(db, current_user.id)
    last_login = get_user_last_login(db, current_user.id)

    # Create response with extended information
    return {
        "id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "is_active": current_user.is_active,
        "is_superuser": current_user.is_superuser,
        "created_at": current_user.created_at,
        "last_login": last_login,
        "api_keys_count": api_keys_count,
        "account_created_at": current_user.created_at
    }


@router.put("/profile", response_model=UserResponse)
async def update_user_profile(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update profile information for the current user
    """
    try:
        updated_user = update_user(
            db=db,
            user_id=current_user.id,
            username=user_update.username,
            email=user_update.email
        )
        return updated_user
    except ValueError as e:
        raise BadRequestException()


@router.post("/deactivate", status_code=status.HTTP_200_OK)
async def deactivate_account(
        deactivate_request: AccountDeactivateRequest,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Deactivate the current user's account
    """
    # Verify password
    if not verify_password(deactivate_request.password, current_user.hashed_password):
        raise BadRequestException(detail="Current password is incorrect")

    # Deactivate account
    success = deactivate_user(db, current_user.id)

    if not success:
        raise ServerErrorException(detail="Failed to deactivate account")

    # In a real application, you might want to log the reason for deactivation
    # if deactivate_request.reason:
    #     log_account_deactivation(user_id=current_user.id, reason=deactivate_request.reason)

    return {"message": "Account deactivated successfully"}
