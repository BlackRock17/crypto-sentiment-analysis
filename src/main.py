import logging
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Настройка на логера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Импортирай твоя RateLimiter
from src.middleware.rate_limiter import RateLimiter

# Импортирай другите необходими модули
from src.monitoring.setup import setup_kafka_monitoring, shutdown_kafka_monitoring
from src.api.auth import router as auth_router
from src.api.twitter import router as twitter_router
from src.scheduler import setup_scheduler, shutdown_scheduler
from src.api.notifications import router as notifications_router
from src.data_processing.kafka.setup import create_topics

# Определяне на базовата директория на проекта
BASE_DIR = Path(__file__).parent.parent

# Логване на пътищата за дебъг
logger.info(f"Base directory: {BASE_DIR}")
logger.info(f"Static files: {os.path.join(BASE_DIR, 'src', 'static')}")
logger.info(f"Templates: {os.path.join(BASE_DIR, 'src', 'templates')}")

# Глобална променлива за Kafka мониторинг
kafka_monitoring = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    global kafka_monitoring

    from src.data_collection.twitter.config import twitter_config
    twitter_config.is_test_mode = False
    logger.info(f"Twitter API в реален режим: {twitter_config.is_test_mode}")

    # Startup: Set up Kafka topics and start scheduler
    try:
        # Create Kafka topics if they don't exist
        topics_created = create_topics()
        if topics_created:
            logger.info("Kafka topics created successfully")
        else:
            logger.warning("Failed to create Kafka topics - services may not function correctly")

        # Set up Kafka monitoring
        monitoring = setup_kafka_monitoring()
        kafka_monitoring = monitoring

    except Exception as e:
        logger.error(f"Error creating Kafka topics: {e}")

    try:
        from scripts.seed_blockchain_networks import seed_blockchain_networks
        seed_blockchain_networks()
        logger.info("Blockchain networks checked")
    except Exception as e:
        logger.error(f"Error checking blockchain networks: {e}")

    # Set up and start scheduler
    scheduler = setup_scheduler()

    yield

    # Shutdown: Stop scheduler
    shutdown_scheduler()
    shutdown_kafka_monitoring()


# Create FastAPI application
app = FastAPI(
    title="Solana Sentiment Analysis",
    description="API for analyzing sentiment of Solana tokens on social media",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
app.add_middleware(RateLimiter)

# Настройка за шаблони и статични файлове с абсолютни пътища
static_path = os.path.join(BASE_DIR, "src", "static")
app.mount("/static", StaticFiles(directory=static_path), name="static")

templates_path = os.path.join(BASE_DIR, "src", "templates")
templates = Jinja2Templates(directory=templates_path)

# Include API routers
app.include_router(auth_router)
app.include_router(twitter_router)
app.include_router(notifications_router)


# Ендпойнти за UI
@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/add-tweet", response_class=HTMLResponse)
async def read_add_tweet(request: Request):
    return templates.TemplateResponse("tweet_form.html", {"request": request})


@app.get("/monitoring", response_class=HTMLResponse)
async def read_monitoring(request: Request):
    return templates.TemplateResponse("monitoring.html", {"request": request})


# Добавете това в края на файла main.py
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)