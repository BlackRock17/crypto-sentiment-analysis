import pytest
import uuid
from datetime import datetime, timedelta
from src.data_processing.database import get_db
from src.data_processing.crud.create import (
    create_solana_token,
    create_tweet,
    create_sentiment_analysis,
    create_token_mention
)
from src.data_processing.crud.read import (
    get_solana_token_by_id,
    get_tweet_by_id,
    get_sentiment_analysis_by_id,
    get_token_mention_by_id
)
from src.data_processing.crud.update import (
    update_solana_token,
    update_tweet,
    update_sentiment_analysis,
    update_token_mention,
    update_tweet_by_twitter_id
)
from src.data_processing.models.database import SentimentEnum