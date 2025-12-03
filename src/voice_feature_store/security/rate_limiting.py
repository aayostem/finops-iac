import time
from typing import Dict, Tuple
import redis
from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Redis-based rate limiting implementation."""

    def __init__(self, redis_client: redis.Redis, prefix: str = "rate_limit"):
        self.redis = redis_client
        self.prefix = prefix

    def _get_key(self, identifier: str, window: int) -> str:
        """Generate Redis key for rate limiting."""
        return f"{self.prefix}:{identifier}:{window}"

    def is_rate_limited(
        self, identifier: str, max_requests: int, window_seconds: int
    ) -> Tuple[bool, Dict]:
        """Check if request is rate limited."""
        current_window = int(time.time() // window_seconds)
        key = self._get_key(identifier, current_window)

        try:
            # Use pipeline for atomic operations
            pipeline = self.redis.pipeline()
            pipeline.incr(key)
            pipeline.expire(key, window_seconds)
            results = pipeline.execute()

            current_requests = results[0]

            remaining = max(0, max_requests - current_requests)
            reset_time = (current_window + 1) * window_seconds

            headers = {
                "X-RateLimit-Limit": str(max_requests),
                "X-RateLimit-Remaining": str(remaining),
                "X-RateLimit-Reset": str(reset_time),
            }

            is_limited = current_requests > max_requests

            if is_limited:
                logger.warning(
                    f"Rate limit exceeded for {identifier}: {current_requests}/{max_requests}"
                )

            return is_limited, headers

        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            # Fail open - don't block requests if Redis is down
            return False, {}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware for rate limiting requests."""

    def __init__(self, app, rate_limiter: RateLimiter, limits: Dict):
        super().__init__(app)
        self.rate_limiter = rate_limiter
        self.limits = limits  # {endpoint_pattern: (max_requests, window_seconds)}

    async def dispatch(self, request: Request, call_next):
        # Get client identifier (IP or user ID)
        client_ip = request.client.host
        user_agent = request.headers.get("user-agent", "unknown")

        # Use API key if available, otherwise use IP + user agent
        identifier = self._get_identifier(request, client_ip, user_agent)

        # Find matching rate limit for this endpoint
        endpoint_limit = self._get_endpoint_limit(request.url.path, request.method)

        if endpoint_limit:
            max_requests, window_seconds = endpoint_limit
            is_limited, headers = self.rate_limiter.is_rate_limited(
                identifier, max_requests, window_seconds
            )

            if is_limited:
                raise HTTPException(
                    status_code=429, detail="Rate limit exceeded", headers=headers
                )

            # Add rate limit headers to response
            response = await call_next(request)
            for key, value in headers.items():
                response.headers[key] = value
            return response

        return await call_next(request)

    def _get_identifier(self, request: Request, client_ip: str, user_agent: str) -> str:
        """Get unique identifier for rate limiting."""
        # Try to get API key from header
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return f"token_{hash(token) % 10000:04d}"

        # Fall back to IP + user agent
        return f"ip_{client_ip}_{hash(user_agent) % 1000:03d}"

    def _get_endpoint_limit(self, path: str, method: str) -> Tuple[int, int]:
        """Get rate limit for specific endpoint."""
        for pattern, limit in self.limits.items():
            if self._matches_pattern(pattern, path, method):
                return limit
        return None

    def _matches_pattern(self, pattern: str, path: str, method: str) -> bool:
        """Check if path and method match the pattern."""
        if ":" in pattern:
            pattern_method, pattern_path = pattern.split(":", 1)
            return (
                pattern_method == method or pattern_method == "*"
            ) and path.startswith(pattern_path)
        else:
            return path.startswith(pattern)


# Rate limit configurations
DEFAULT_RATE_LIMITS = {
    "GET:/features": (100, 60),  # 100 requests per minute for feature reads
    "POST:/features": (50, 60),  # 50 requests per minute for feature writes
    "GET:/training-data": (20, 60),  # 20 requests per minute for training data
    "GET:/metrics": (10, 60),  # 10 requests per minute for metrics
    "*:/health": (1000, 60),  # 1000 requests per minute for health checks
}
