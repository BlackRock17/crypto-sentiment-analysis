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