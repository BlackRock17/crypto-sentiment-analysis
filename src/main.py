import logging
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

# Глобална променлива за Kafka мониторинг
kafka_monitoring = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    global kafka_monitoring

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

# Настройка за шаблони и статични файлове
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/templates")

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