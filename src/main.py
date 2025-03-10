import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.api.auth import router as auth_router
from src.api.twitter import router as twitter_router
from src.middleware.rate_limiter import RateLimiter
from src.scheduler import setup_scheduler, shutdown_scheduler
from src.api.notifications import router as notifications_router
from src.data_processing.kafka.setup import create_topics  # Add this import


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    # Startup: Set up Kafka topics and start scheduler
    try:
        # Create Kafka topics if they don't exist
        topics_created = create_topics()
        if topics_created:
            app.logger.info("Kafka topics created successfully")
        else:
            app.logger.warning("Failed to create Kafka topics - services may not function correctly")
    except Exception as e:
        app.logger.error(f"Error creating Kafka topics: {e}")

    # Set up and start scheduler
    scheduler = setup_scheduler()

    yield

    # Shutdown: Stop scheduler
    shutdown_scheduler()


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
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
app.add_middleware(RateLimiter)

# Include routers
app.include_router(auth_router)
app.include_router(twitter_router)
app.include_router(notifications_router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Solana Sentiment Analysis API",
        "version": "0.1.0",
        "status": "active"
    }


if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=True)
