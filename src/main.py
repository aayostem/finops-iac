#!/usr/bin/env python3
"""
Main application entry point for Voice Feature Store.
Orchestrates all components and provides unified interface.
"""
import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Depends
import uvicorn

from config.settings import settings
from api.server import app as api_app
from api.ml_server import ml_router
from security.authentication import authenticator
from security.rate_limiting import RateLimitMiddleware, RateLimiter, DEFAULT_RATE_LIMITS
from storage.online_store import OnlineFeatureStore
from storage.offline_store import OfflineFeatureStore
from ml.model_serving import ModelRegistry, ModelServer
from monitoring.alerting import AlertManager
from streaming.flink_processor import setup_flink_job
import redis

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("voice_feature_store.log")],
)

logger = logging.getLogger(__name__)


class VoiceFeatureStore:
    """Main application class orchestrating all components."""

    def __init__(self):
        self.online_store = None
        self.offline_store = None
        self.model_registry = None
        self.model_server = None
        self.alert_manager = None
        self.rate_limiter = None
        self.flink_job = None
        self.is_running = False

    async def initialize(self):
        """Initialize all components."""
        logger.info("Initializing Voice Feature Store...")

        try:
            # Initialize storage
            self.online_store = OnlineFeatureStore()
            self.offline_store = OfflineFeatureStore()

            # Initialize ML components
            self.model_registry = ModelRegistry()
            self.model_server = ModelServer(self.online_store, self.model_registry)

            # Initialize monitoring and alerting
            alert_config = {
                "slack_webhook": settings.slack_webhook_url,
                "email_config": settings.email_config,
                "pagerduty_integration_key": settings.pagerduty_key,
                "latency_threshold": 1.0,
                "error_rate_threshold": 0.05,
                "computation_delay_threshold": 10.0,
            }
            self.alert_manager = AlertManager(alert_config)

            # Initialize rate limiting
            redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password,
                decode_responses=True,
            )
            self.rate_limiter = RateLimiter(redis_client)

            # Health check
            if not await self.health_check():
                raise Exception("Health check failed during initialization")

            logger.info("Voice Feature Store initialized successfully")
            self.is_running = True

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            await self.shutdown()
            raise

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of all components."""
        health_status = {
            "online_store": False,
            "offline_store": False,
            "model_server": False,
            "overall": False,
            "timestamp": None,
        }

        try:
            # Check online store
            if self.online_store:
                health_status["online_store"] = self.online_store.health_check()

            # Check offline store (basic connectivity)
            if self.offline_store:
                health_status["offline_store"] = True  # Simplified check

            # Check model server
            if self.model_server and self.model_registry:
                health_status["model_server"] = len(self.model_server.loaded_models) > 0

            # Overall status
            health_status["overall"] = all(
                [
                    health_status["online_store"],
                    health_status["offline_store"],
                    health_status["model_server"],
                ]
            )

            health_status["timestamp"] = datetime.utcnow().isoformat()

        except Exception as e:
            logger.error(f"Health check error: {e}")
            health_status["overall"] = False

        return health_status

    async def start_streaming_processor(self):
        """Start the Flink streaming job."""
        try:
            logger.info("Starting Flink streaming processor...")
            self.flink_job = setup_flink_job()
            # In production, this would submit the job to a Flink cluster
            logger.info("Flink streaming processor started")
        except Exception as e:
            logger.error(f"Failed to start streaming processor: {e}")

    async def shutdown(self):
        """Graceful shutdown of all components."""
        logger.info("Shutting down Voice Feature Store...")
        self.is_running = False

        # Stop streaming processor
        if self.flink_job:
            try:
                self.flink_job.cancel()
                logger.info("Flink job cancelled")
            except Exception as e:
                logger.error(f"Error cancelling Flink job: {e}")

        # Close connections
        if self.online_store:
            try:
                self.online_store.redis_client.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        logger.info("Voice Feature Store shutdown complete")


# Global application instance
app_instance = VoiceFeatureStore()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    # Startup
    await app_instance.initialize()

    # Start background tasks
    asyncio.create_task(background_monitoring())
    asyncio.create_task(app_instance.start_streaming_processor())

    yield

    # Shutdown
    await app_instance.shutdown()


# Create main FastAPI application
app = FastAPI(
    title="Voice Feature Store",
    description="Enterprise Real-time Feature Store for Voice Analytics",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add rate limiting middleware
app.add_middleware(
    RateLimitMiddleware,
    rate_limiter=app_instance.rate_limiter,
    limits=DEFAULT_RATE_LIMITS,
)

# Include routers
app.include_router(api_app)
app.include_router(ml_router)


# Add startup event
@app.on_event("startup")
async def startup_event():
    """Additional startup tasks."""
    logger.info("Voice Feature Store API starting up...")


# Add shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Additional shutdown tasks."""
    logger.info("Voice Feature Store API shutting down...")


async def background_monitoring():
    """Background task for continuous monitoring."""
    while app_instance.is_running:
        try:
            # Check system health
            health_status = await app_instance.health_check()

            # Trigger alerts if needed
            if not health_status["overall"]:
                await app_instance.alert_manager.check_metrics(
                    {
                        "system_health": 0.0,
                        "unhealthy_components": [
                            comp
                            for comp, healthy in health_status.items()
                            if not healthy and comp != "overall"
                        ],
                    }
                )

            # Wait before next check
            await asyncio.sleep(60)  # Check every minute

        except Exception as e:
            logger.error(f"Background monitoring error: {e}")
            await asyncio.sleep(30)  # Wait shorter period on error


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    asyncio.create_task(app_instance.shutdown())
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main entry point."""
    try:
        # Start the API server
        uvicorn.run(
            "voice_feature_store.main:app",
            host=settings.api_host,
            port=settings.api_port,
            workers=settings.api_workers,
            log_level=settings.log_level.lower(),
            reload=settings.environment == "development",
        )
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
