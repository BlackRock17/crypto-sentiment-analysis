import time
from typing import Dict, Tuple, Optional, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

from config.settings import RATE_LIMIT_PER_MINUTE


class RateLimiter(BaseHTTPMiddleware):
    """
    Middleware for API rate limiting

    Limits the number of requests from a single IP address
    within a specified time window
    """

    def __init__(self, app, rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE):
        super().__init__(app)
        self.rate_limit = rate_limit_per_minute
        self.window = 60  # 60 seconds (1 minute)
        self.requests: Dict[str, Dict[float, int]] = {}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract client IP
        client_ip = self._get_client_ip(request)

        # Check if client exceeded rate limit
        if self._is_rate_limited(client_ip):
            return Response(
                content="Rate limit exceeded. Please try again later.",
                status_code=HTTP_429_TOO_MANY_REQUESTS,
                headers={
                    "Retry-After": str(self.window),
                    "X-Rate-Limit-Limit": str(self.rate_limit),
                    "X-Rate-Limit-Window": f"{self.window}s"
                }
            )

        # Process request
        response = await call_next(request)

        # Add rate limit headers to response
        remaining, reset = self._get_rate_limit_info(client_ip)
        response.headers["X-Rate-Limit-Limit"] = str(self.rate_limit)
        response.headers["X-Rate-Limit-Remaining"] = str(remaining)
        response.headers["X-Rate-Limit-Reset"] = str(int(reset))

        return response

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request headers or connection info"""
        # Check for X-Forwarded-For header (used by proxies)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Get the first IP in the chain (client IP)
            return forwarded_for.split(",")[0].strip()

        # Otherwise use direct client IP
        return request.client.host if request.client else "unknown"

    def _is_rate_limited(self, client_ip: str) -> bool:
        """
        Check if client IP has exceeded rate limit

        Returns True if rate limited, False otherwise
        """
        current_time = time.time()

        # Initialize client entry if not exists
        if client_ip not in self.requests:
            self.requests[client_ip] = {}

        # Remove old request records (outside current window)
        self.requests[client_ip] = {
            ts: count for ts, count in self.requests[client_ip].items()
            if current_time - ts < self.window
        }

        # Get current request count in window
        request_count = sum(self.requests[client_ip].values())

        # Check if exceeds limit
        if request_count >= self.rate_limit:
            return True

        # Record current request
        self.requests[client_ip][current_time] = self.requests[client_ip].get(current_time, 0) + 1
        return False

    def _get_rate_limit_info(self, client_ip: str) -> Tuple[int, float]:
        """
        Get remaining requests and window reset time

        Returns (remaining_requests, reset_time_seconds)
        """
        current_time = time.time()

        # Get requests in current window
        if client_ip in self.requests:
            requests_in_window = sum(self.requests[client_ip].values())
        else:
            requests_in_window = 0

        # Calculate remaining requests
        remaining = max(0, self.rate_limit - requests_in_window)

        # Calculate window reset time
        if requests_in_window > 0 and client_ip in self.requests:
            oldest_timestamp = min(self.requests[client_ip].keys())
            reset_time = oldest_timestamp + self.window
        else:
            reset_time = current_time + self.window

        return remaining, reset_time
