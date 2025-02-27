import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.api.auth import router as auth_router
from src.middleware.rate_limiter import RateLimiter

# Create FastAPI application
app = FastAPI(
    title="Solana Sentiment Analysis",
    description="API for analyzing sentiment of Solana tokens on social media",
    version="0.1.0"
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
