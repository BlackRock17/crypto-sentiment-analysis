import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.api.auth import router as auth_router
from src.middleware.rate_limiter import RateLimiter
from src.scheduler import setup_scheduler, shutdown_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    # Startup: Set up and start scheduler
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
