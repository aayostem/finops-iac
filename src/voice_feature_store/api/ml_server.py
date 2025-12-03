from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from ..security.authentication import authenticator, require_scope
from ..ml.model_serving import ModelServer, ModelRegistry, ModelMonitoring
from ..storage.online_store import OnlineFeatureStore
from ..storage.offline_store import OfflineFeatureStore

logger = logging.getLogger(__name__)

# Create router
ml_router = APIRouter(prefix="/ml", tags=["Machine Learning"])

# Global instances
model_registry = ModelRegistry()
online_store = OnlineFeatureStore()
offline_store = OfflineFeatureStore()
model_server = ModelServer(online_store, model_registry)
model_monitoring = ModelMonitoring(online_store, offline_store)


@ml_router.post("/predict/{model_name}")
async def predict(
    model_name: str,
    call_id: str = Query(..., description="Call identifier"),
    speaker_id: str = Query(..., description="Speaker identifier"),
    version: Optional[str] = Query(None, description="Model version"),
    current_user: Any = Depends(authenticator.require_scope("read:predictions")),
):
    """Make a prediction using a trained model."""
    try:
        prediction = await model_server.predict(
            model_name, call_id, speaker_id, version
        )

        if not prediction:
            raise HTTPException(
                status_code=404, detail=f"Prediction failed or features not available"
            )

        return {
            "model_name": prediction.model_name,
            "model_version": prediction.model_version,
            "prediction": prediction.prediction,
            "confidence": prediction.confidence,
            "features_used": prediction.features_used,
            "execution_time_ms": prediction.execution_time_ms,
            "timestamp": prediction.timestamp.isoformat(),
        }

    except Exception as e:
        logger.error(f"Prediction endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")


@ml_router.post("/predict/batch/{model_name}")
async def batch_predict(
    model_name: str,
    call_speaker_pairs: List[str] = Query(
        ..., description="List of call_id:speaker_id pairs"
    ),
    version: Optional[str] = Query(None, description="Model version"),
    current_user: Any = Depends(authenticator.require_scope("read:predictions")),
):
    """Make batch predictions for multiple calls."""
    try:
        predictions = await model_server.batch_predict(
            model_name, call_speaker_pairs, version
        )

        results = {}
        for pair, prediction in predictions.items():
            if prediction:
                results[pair] = {
                    "prediction": prediction.prediction,
                    "confidence": prediction.confidence,
                    "execution_time_ms": prediction.execution_time_ms,
                    "timestamp": prediction.timestamp.isoformat(),
                }

        return {
            "model_name": model_name,
            "model_version": version
            or model_registry.get_model(model_name).get("current_version"),
            "total_requests": len(call_speaker_pairs),
            "successful_predictions": len(results),
            "predictions": results,
        }

    except Exception as e:
        logger.error(f"Batch prediction endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Batch prediction failed")


@ml_router.get("/models")
async def list_models(
    current_user: Any = Depends(authenticator.require_scope("read:models")),
):
    """List all available models."""
    try:
        models = model_registry.list_models()
        return {"models": models}
    except Exception as e:
        logger.error(f"List models endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to list models")


@ml_router.get("/models/{model_name}")
async def get_model_info(
    model_name: str,
    version: Optional[str] = Query(None, description="Model version"),
    current_user: Any = Depends(authenticator.require_scope("read:models")),
):
    """Get detailed information about a model."""
    try:
        model_info = model_registry.get_model(model_name, version)
        if not model_info:
            raise HTTPException(status_code=404, detail="Model not found")

        return {
            "model_name": model_name,
            "version": version
            or model_registry.models[model_name].get("current_version"),
            "info": model_info,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get model info endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get model info")


@ml_router.get("/monitoring/drift/{model_name}")
async def check_feature_drift(
    model_name: str,
    days: int = Query(7, description="Number of days for reference data"),
    current_user: Any = Depends(authenticator.require_scope("read:monitoring")),
):
    """Check feature drift for a model."""
    try:
        model_info = model_registry.get_model(model_name)
        if not model_info:
            raise HTTPException(status_code=404, detail="Model not found")

        # Get reference data (older data)
        end_date = datetime.utcnow() - timedelta(days=days)
        start_date = end_date - timedelta(days=30)  # 30-day reference period
        reference_data = offline_store.get_training_data(start_date, end_date)

        if reference_data.empty:
            return {"message": "No reference data available", "drift_scores": {}}

        feature_set = model_info.get("feature_set", "default")
        drift_scores = await model_monitoring.check_feature_drift(
            model_name, feature_set, reference_data
        )

        # Determine drift status
        drift_status = {}
        for feature, score in drift_scores.items():
            if score > 0.25:
                drift_status[feature] = "high_drift"
            elif score > 0.1:
                drift_status[feature] = "medium_drift"
            else:
                drift_status[feature] = "low_drift"

        return {
            "model_name": model_name,
            "reference_period": f"{start_date.date()} to {end_date.date()}",
            "drift_scores": drift_scores,
            "drift_status": drift_status,
            "overall_drift": max(drift_scores.values()) if drift_scores else 0.0,
        }

    except Exception as e:
        logger.error(f"Feature drift check endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to check feature drift")


@ml_router.get("/monitoring/performance")
async def get_model_performance(
    model_name: Optional[str] = Query(None, description="Filter by model name"),
    current_user: Any = Depends(authenticator.require_scope("read:monitoring")),
):
    """Get model performance metrics."""
    try:
        # In production, this would query your metrics database
        # For now, return mock data
        performance_data = {
            "sentiment_analysis": {
                "accuracy": 0.89,
                "precision": 0.87,
                "recall": 0.85,
                "f1_score": 0.86,
                "inference_latency_p95": 45.2,
                "throughput_rps": 1250,
            },
            "call_quality_scoring": {
                "accuracy": 0.92,
                "precision": 0.91,
                "recall": 0.90,
                "f1_score": 0.905,
                "inference_latency_p95": 28.7,
                "throughput_rps": 1800,
            },
        }

        if model_name:
            if model_name in performance_data:
                return {model_name: performance_data[model_name]}
            else:
                raise HTTPException(
                    status_code=404, detail="Model performance data not found"
                )
        else:
            return performance_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Model performance endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get performance metrics")


@ml_router.post("/models/{model_name}/register")
async def register_model(
    model_name: str,
    version: str,
    model_info: Dict[str, Any],
    current_user: Any = Depends(authenticator.require_scope("write:models")),
):
    """Register a new model version."""
    try:
        model_registry.register_model(model_name, version, model_info)
        return {
            "message": f"Model {model_name} version {version} registered successfully",
            "model_name": model_name,
            "version": version,
        }
    except Exception as e:
        logger.error(f"Model registration endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to register model")
