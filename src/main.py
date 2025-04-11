"""
Main application module for Twitter Sentiment Analysis.
"""
import logging
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.api.twitter import router as twitter_router
from src.data_processing.kafka.setup import ensure_topics_exist

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine the base directory of the project
BASE_DIR = Path(__file__).parent.parent

# Log paths for debugging
logger.info(f"Base directory: {BASE_DIR}")
logger.info(f"Static files: {os.path.join(BASE_DIR, 'src', 'static')}")
logger.info(f"Templates: {os.path.join(BASE_DIR, 'src', 'templates')}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    # Startup: create Kafka topics if they don't exist
    try:
        logger.info("Ensuring Kafka topics exist...")
        ensure_topics_exist()
    except Exception as e:
        logger.warning(f"Failed to ensure Kafka topics: {e}")

    yield

    # Shutdown: perform any cleanup here
    logger.info("Shutting down application...")


# Create the FastAPI application
app = FastAPI(
    title="Twitter Sentiment Analysis",
    description="API for analyzing sentiment in tweets",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(BASE_DIR, "src", "static")),
    name="static"
)

# Set up Jinja2 templates
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "src", "templates"))

# Include routers
app.include_router(twitter_router)


# Root route redirects to the homepage
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Render the index page."""
    return templates.TemplateResponse("index.html", {"request": request})


# Route for adding tweets manually
@app.get("/add-tweet", response_class=HTMLResponse)
async def add_tweet(request: Request):
    """Render the tweet form page."""
    return templates.TemplateResponse("tweet_form.html", {"request": request})


# Route for monitoring
@app.get("/monitoring", response_class=HTMLResponse)
async def monitoring(request: Request):
    """Render the monitoring page."""
    return templates.TemplateResponse("monitoring.html", {"request": request})


# Add this in the end of the file
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
