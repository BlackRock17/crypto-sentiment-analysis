import pytest
import uuid
from datetime import datetime
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
from src.data_processing.crud.delete import (
    delete_solana_token,
    delete_tweet,
    delete_sentiment_analysis,
    delete_token_mention,
    delete_tweet_by_twitter_id,
    delete_solana_token_by_address,
    delete_solana_token_cascade
)
from src.data_processing.models.database import SentimentEnum