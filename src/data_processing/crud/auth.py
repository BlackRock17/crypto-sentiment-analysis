from datetime import datetime
from typing import Optional, List

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from src.data_processing.models.auth import User, Token, ApiKey
from src.security.utils import get_password_hash, verify_password