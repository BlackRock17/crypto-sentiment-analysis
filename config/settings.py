import os
import secrets
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Проверка дали сме в Docker среда
IN_DOCKER = os.environ.get('IN_DOCKER', 'false').lower() == 'true'

# Database Configuration
if IN_DOCKER:
    # В Docker среда, използваме host.docker.internal за достъп до хост машината
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@host.docker.internal:5432/crypto_sentiment')
else:
    # Локална разработка
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/crypto_sentiment')

# Twitter API Configuration
TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')

# # Database Configuration
# DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://username:password@localhost:5432/crypto_sentiment')

# Solana-related hashtags to track
SOLANA_HASHTAGS = [
    'solana',
    'sol',
    'solanasummer',
    'solananft',
    'solanafamous',
]

# JWT Settings
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", secrets.token_hex(32))  # Generates a random key if none exists
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # 30 minutes token validity

RATE_LIMIT_PER_MINUTE = 30