from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import prometheus_client
from prometheus_client import Counter, Histogram, generate_latest

from ..storage.online_store import OnlineFeatureStore
from ..storage.offline_store import OfflineFeatureStore
from ..features.feature_registry import FeatureRegistry
from config.settings import settings


# Metrics
REQUEST_COUNT = Counter(
    "api_requests_total", "Total API requests", ["endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds", "API request latency", ["endpoint"]
)

# Logging
logging.basicConfig(level=getattr(logging, settings.log_level))
logger = logging.getLogger(__name__)

# FastAPI App
app = FastAPI(
    title="Voice Feature Store API",
    description="Real-time feature store for voice analytics",
    version="1.0.0",
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependencies
def get_online_store():
    return OnlineFeatureStore()


def get_offline_store():
    return OfflineFeatureStore()


def get_feature_registry():
    return FeatureRegistry()


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("Starting Voice Feature Store API")

    # Health check dependencies
    online_store = OnlineFeatureStore()
    if not online_store.health_check():
        logger.error("Online store health check failed on startup")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Voice Feature Store API",
        "version": "1.0.0",
        "status": "healthy",
    }


@app.get("/health")
async def health_check(online_store: OnlineFeatureStore = Depends(get_online_store)):
    """Comprehensive health check."""
    health_status = {
        "api": "healthy",
        "online_store": "healthy" if online_store.health_check() else "unhealthy",
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Overall status
    overall_healthy = all(
        status == "healthy"
        for service, status in health_status.items()
        if service != "timestamp"
    )

    status_code = 200 if overall_healthy else 503
    health_status["overall"] = "healthy" if overall_healthy else "unhealthy"

    return JSONResponse(content=health_status, status_code=status_code)


@app.get("/features/{call_id}/{speaker_id}")
async def get_features(
    call_id: str,
    speaker_id: str,
    feature_names: Optional[List[str]] = Query(None),
    online_store: OnlineFeatureStore = Depends(get_online_store),
):
    """Get latest features for a specific call and speaker."""
    with REQUEST_LATENCY.labels(endpoint="get_features").time():
        try:
            if feature_names:
                # Get specific feature vector
                features = online_store.get_feature_vector(
                    call_id, speaker_id, feature_names
                )
            else:
                # Get all features
                voice_features = online_store.get_latest_features(call_id, speaker_id)
                features = voice_features.to_dict() if voice_features else {}

            if not features:
                raise HTTPException(status_code=404, detail="Features not found")

            REQUEST_COUNT.labels(endpoint="get_features", status="success").inc()
            return {"call_id": call_id, "speaker_id": speaker_id, "features": features}

        except HTTPException:
            REQUEST_COUNT.labels(endpoint="get_features", status="error").inc()
            raise
        except Exception as e:
            logger.error(f"Error getting features for {call_id}:{speaker_id}: {e}")
            REQUEST_COUNT.labels(endpoint="get_features", status="error").inc()
            raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/features/batch")
async def get_features_batch(
    call_speaker_pairs: List[str] = Query(
        ..., description="List of call_id:speaker_id pairs"
    ),
    feature_names: Optional[List[str]] = Query(None),
    online_store: OnlineFeatureStore = Depends(get_online_store),
):
    """Get features for multiple call/speaker pairs in batch."""
    with REQUEST_LATENCY.labels(endpoint="get_features_batch").time():
        try:
            results = {}

            for pair in call_speaker_pairs:
                if ":" not in pair:
                    continue

                call_id, speaker_id = pair.split(":", 1)
                if feature_names:
                    features = online_store.get_feature_vector(
                        call_id, speaker_id, feature_names
                    )
                else:
                    voice_features = online_store.get_latest_features(
                        call_id, speaker_id
                    )
                    features = voice_features.to_dict() if voice_features else {}

                results[pair] = features

            REQUEST_COUNT.labels(endpoint="get_features_batch", status="success").inc()
            return {"results": results}

        except Exception as e:
            logger.error(f"Error in batch features request: {e}")
            REQUEST_COUNT.labels(endpoint="get_features_batch", status="error").inc()
            raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/training-data")
async def get_training_data(
    start_date: datetime,
    end_date: datetime,
    call_ids: Optional[List[str]] = Query(None),
    offline_store: OfflineFeatureStore = Depends(get_offline_store),
):
    """Get historical features for model training."""
    with REQUEST_LATENCY.labels(endpoint="get_training_data").time():
        try:
            df = offline_store.get_training_data(start_date, end_date, call_ids)

            if df.empty:
                return {"message": "No data found for the given criteria", "data": []}

            # Convert DataFrame to JSON records
            records = df.to_dict("records")

            REQUEST_COUNT.labels(endpoint="get_training_data", status="success").inc()
            return {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "record_count": len(records),
                "data": records,
            }

        except Exception as e:
            logger.error(f"Error getting training data: {e}")
            REQUEST_COUNT.labels(endpoint="get_training_data", status="error").inc()
            raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/point-in-time-data")
async def get_point_in_time_data(
    prediction_times: List[datetime] = Query(
        ..., description="List of prediction timestamps"
    ),
    lookback_hours: int = Query(1, description="Lookback window in hours"),
    offline_store: OfflineFeatureStore = Depends(get_offline_store),
):
    """Get point-in-time correct dataset for training."""
    with REQUEST_LATENCY.labels(endpoint="get_point_in_time_data").time():
        try:
            lookback = timedelta(hours=lookback_hours)
            df = offline_store.create_point_in_time_correct_dataset(
                prediction_times, lookback
            )

            if df.empty:
                return {"message": "No data found", "data": []}

            records = df.to_dict("records")

            REQUEST_COUNT.labels(
                endpoint="get_point_in_time_data", status="success"
            ).inc()
            return {
                "prediction_times": [t.isoformat() for t in prediction_times],
                "lookback_hours": lookback_hours,
                "record_count": len(records),
                "data": records,
            }

        except Exception as e:
            logger.error(f"Error getting point-in-time data: {e}")
            REQUEST_COUNT.labels(
                endpoint="get_point_in_time_data", status="error"
            ).inc()
            raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()


@app.get("/feature-registry")
async def get_feature_registry(
    registry: FeatureRegistry = Depends(get_feature_registry),
):
    """Get all registered features with metadata."""
    with REQUEST_LATENCY.labels(endpoint="feature_registry").time():
        try:
            features = registry.get_all_features()
            REQUEST_COUNT.labels(endpoint="feature_registry", status="success").inc()
            return {"features": features}
        except Exception as e:
            logger.error(f"Error getting feature registry: {e}")
            REQUEST_COUNT.labels(endpoint="feature_registry", status="error").inc()
            raise HTTPException(status_code=500, detail="Internal server error")


def main():
    """Main entry point for the API server."""
    uvicorn.run(
        "voice_feature_store.api.server:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        log_level=settings.log_level.lower(),
        reload=settings.environment == "development",
    )


if __name__ == "__main__":
    main()
