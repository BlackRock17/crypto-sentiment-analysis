"""
Twitter API endpoints for Solana Sentiment Analysis.
"""

import logging
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any, List

from src.data_processing.database import get_db
from src.data_collection.tasks.twitter_tasks import collect_influencer_tweets
from src.security.auth import get_current_superuser
from src.data_processing.models.auth import User

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/twitter", tags=["Twitter"])