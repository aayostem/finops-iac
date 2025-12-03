import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any
import pickle
import json
from datetime import datetime
import logging
from prometheus_client import Counter, Histogram, Gauge
import asyncio
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ML Model Metrics
MODEL_PREDICTION_COUNT = Counter(
    "model_predictions_total", "Total model predictions", ["model_name", "status"]
)
MODEL_PREDICTION_LATENCY = Histogram(
    "model_prediction_latency_seconds", "Model prediction latency", ["model_name"]
)
MODEL_FEATURE_DRIFT = Gauge(
    "model_feature_drift", "Feature drift detection", ["model_name", "feature_name"]
)


@dataclass
class ModelPrediction:
    """Model prediction result with metadata."""

    model_name: str
    prediction: Any
    confidence: float
    features_used: List[str]
    timestamp: datetime
    model_version: str
    execution_time_ms: float


class ModelRegistry:
    """Registry for managing ML models and their versions."""

    def __init__(self, registry_path: str = "models/"):
        self.registry_path = registry_path
        self.models: Dict[str, Dict] = {}
        self._load_model_registry()

    def _load_model_registry(self):
        """Load model registry from storage."""
        try:
            # In production, this would load from a database or model registry service
            self.models = {
                "sentiment_analysis": {
                    "current_version": "v1.2.0",
                    "versions": {
                        "v1.2.0": {
                            "path": "models/sentiment/v1.2.0/model.pkl",
                            "created_at": "2023-10-27T10:00:00Z",
                            "metrics": {
                                "accuracy": 0.89,
                                "precision": 0.87,
                                "recall": 0.85,
                                "f1_score": 0.86,
                            },
                            "feature_set": "voice_sentiment_v1",
                            "target_latency_ms": 50,
                        }
                    },
                    "description": "Real-time customer sentiment analysis from voice features",
                },
                "call_quality_scoring": {
                    "current_version": "v1.1.0",
                    "versions": {
                        "v1.1.0": {
                            "path": "models/quality/v1.1.0/model.pkl",
                            "created_at": "2023-10-20T15:30:00Z",
                            "metrics": {
                                "accuracy": 0.92,
                                "precision": 0.91,
                                "recall": 0.90,
                                "f1_score": 0.905,
                            },
                            "feature_set": "voice_quality_v1",
                            "target_latency_ms": 30,
                        }
                    },
                    "description": "Call quality and agent performance scoring",
                },
            }
        except Exception as e:
            logger.error(f"Failed to load model registry: {e}")
            self.models = {}

    def register_model(self, model_name: str, version: str, model_info: Dict):
        """Register a new model version."""
        if model_name not in self.models:
            self.models[model_name] = {"versions": {}}

        self.models[model_name]["versions"][version] = model_info

        # Auto-promote to current if this is the first version or if specified
        if "current_version" not in self.models[model_name] or model_info.get(
            "promote_to_current", False
        ):
            self.models[model_name]["current_version"] = version

        logger.info(f"Registered model {model_name} version {version}")

    def get_model(self, model_name: str, version: str = None) -> Optional[Dict]:
        """Get model information."""
        if model_name not in self.models:
            return None

        if version is None:
            version = self.models[model_name].get("current_version")

        return self.models[model_name]["versions"].get(version)

    def list_models(self) -> List[Dict]:
        """List all registered models."""
        return [
            {
                "name": name,
                "current_version": info.get("current_version"),
                "description": info.get("description", ""),
                "version_count": len(info.get("versions", {})),
            }
            for name, info in self.models.items()
        ]


class ModelServer:
    """Server for real-time model inference using voice features."""

    def __init__(self, online_store, model_registry: ModelRegistry):
        self.online_store = online_store
        self.model_registry = model_registry
        self.loaded_models: Dict[str, Any] = {}
        self._load_models()

    def _load_models(self):
        """Load models into memory."""
        for model_name, model_info in self.model_registry.models.items():
            current_version = model_info.get("current_version")
            if current_version:
                try:
                    model_path = model_info["versions"][current_version]["path"]
                    self.loaded_models[model_name] = self._load_single_model(model_path)
                    logger.info(f"Loaded model {model_name} version {current_version}")
                except Exception as e:
                    logger.error(f"Failed to load model {model_name}: {e}")

    def _load_single_model(self, model_path: str) -> Any:
        """Load a single model from storage."""

        # In production, this would load from model storage (S3, model registry, etc.)
        # For now, return a mock model
        class MockModel:
            def predict(self, features):
                # Mock sentiment prediction based on voice features
                if features.get("talk_to_listen_ratio", 0) > 0.7:
                    return (
                        "positive"
                        if features.get("energy_db", -50) > -30
                        else "neutral"
                    )
                else:
                    return (
                        "negative"
                        if features.get("silence_ratio", 0) > 0.3
                        else "neutral"
                    )

            def predict_proba(self, features):
                # Mock confidence scores
                prediction = self.predict(features)
                if prediction == "positive":
                    return {"positive": 0.8, "neutral": 0.15, "negative": 0.05}
                elif prediction == "neutral":
                    return {"positive": 0.2, "neutral": 0.7, "negative": 0.1}
                else:
                    return {"positive": 0.1, "neutral": 0.2, "negative": 0.7}

        return MockModel()

    async def predict(
        self, model_name: str, call_id: str, speaker_id: str, version: str = None
    ) -> Optional[ModelPrediction]:
        """Make prediction using voice features."""
        start_time = datetime.utcnow()

        try:
            # Get model info
            model_info = self.model_registry.get_model(model_name, version)
            if not model_info:
                logger.error(f"Model {model_name} version {version} not found")
                MODEL_PREDICTION_COUNT.labels(
                    model_name=model_name, status="error"
                ).inc()
                return None

            # Get required features
            feature_set = model_info.get("feature_set", "default")
            required_features = self._get_required_features(feature_set)

            # Retrieve features from online store
            features = self.online_store.get_feature_vector(
                call_id, speaker_id, required_features
            )

            if not features:
                logger.warning(f"No features found for {call_id}:{speaker_id}")
                MODEL_PREDICTION_COUNT.labels(
                    model_name=model_name, status="error"
                ).inc()
                return None

            # Validate features
            if not self._validate_features(features, required_features):
                logger.warning(f"Incomplete features for prediction: {features}")
                MODEL_PREDICTION_COUNT.labels(
                    model_name=model_name, status="error"
                ).inc()
                return None

            # Make prediction
            model = self.loaded_models.get(model_name)
            if not model:
                logger.error(f"Model {model_name} not loaded")
                MODEL_PREDICTION_COUNT.labels(
                    model_name=model_name, status="error"
                ).inc()
                return None

            # Time the prediction
            prediction_start = datetime.utcnow()
            prediction = model.predict(features)
            confidence_scores = model.predict_proba(features)
            prediction_time = (
                datetime.utcnow() - prediction_start
            ).total_seconds() * 1000

            # Calculate overall confidence
            confidence = max(confidence_scores.values()) if confidence_scores else 0.0

            # Create prediction result
            result = ModelPrediction(
                model_name=model_name,
                prediction=prediction,
                confidence=confidence,
                features_used=required_features,
                timestamp=datetime.utcnow(),
                model_version=version or model_info.get("current_version"),
                execution_time_ms=prediction_time,
            )

            # Record metrics
            total_time = (datetime.utcnow() - start_time).total_seconds()
            MODEL_PREDICTION_LATENCY.labels(model_name=model_name).observe(total_time)
            MODEL_PREDICTION_COUNT.labels(model_name=model_name, status="success").inc()

            logger.info(
                f"Prediction completed for {model_name}: {prediction} (confidence: {confidence:.2f})"
            )
            return result

        except Exception as e:
            logger.error(f"Prediction failed for {model_name}: {e}")
            MODEL_PREDICTION_COUNT.labels(model_name=model_name, status="error").inc()
            return None

    def _get_required_features(self, feature_set: str) -> List[str]:
        """Get required features for a feature set."""
        feature_sets = {
            "voice_sentiment_v1": [
                "talk_to_listen_ratio",
                "interruption_count",
                "silence_ratio",
                "avg_pitch",
                "energy_db",
                "voice_confidence",
            ],
            "voice_quality_v1": [
                "talk_to_listen_ratio",
                "interruption_count",
                "silence_ratio",
                "jitter",
                "shimmer",
                "hnr",
                "speaking_rate",
            ],
            "default": [
                "talk_to_listen_ratio",
                "interruption_count",
                "silence_ratio",
                "avg_pitch",
                "energy_db",
                "voice_confidence",
            ],
        }
        return feature_sets.get(feature_set, feature_sets["default"])

    def _validate_features(self, features: Dict, required_features: List[str]) -> bool:
        """Validate that all required features are present and valid."""
        for feature in required_features:
            if feature not in features or features[feature] is None:
                return False
        return True

    async def batch_predict(
        self, model_name: str, call_speaker_pairs: List[str], version: str = None
    ) -> Dict[str, ModelPrediction]:
        """Make batch predictions for multiple call/speaker pairs."""
        tasks = []
        for pair in call_speaker_pairs:
            if ":" in pair:
                call_id, speaker_id = pair.split(":", 1)
                tasks.append(self.predict(model_name, call_id, speaker_id, version))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        predictions = {}
        for pair, result in zip(call_speaker_pairs, results):
            if isinstance(result, ModelPrediction):
                predictions[pair] = result
            elif isinstance(result, Exception):
                logger.error(f"Batch prediction failed for {pair}: {result}")

        return predictions


class ModelMonitoring:
    """Monitor model performance and data drift."""

    def __init__(self, online_store, offline_store):
        self.online_store = online_store
        self.offline_store = offline_store
        self.drift_detectors: Dict[str, Any] = {}

    async def check_feature_drift(
        self, model_name: str, feature_set: str, reference_data: pd.DataFrame
    ) -> Dict[str, float]:
        """Check for feature drift compared to reference data."""
        try:
            # Get recent production features
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(hours=24)
            recent_data = self.offline_store.get_training_data(start_date, end_date)

            if recent_data.empty:
                return {}

            drift_scores = {}
            required_features = self._get_feature_set_features(feature_set)

            for feature in required_features:
                if feature in recent_data.columns and feature in reference_data.columns:
                    # Calculate drift using Population Stability Index (PSI)
                    psi = self._calculate_psi(
                        reference_data[feature].dropna(), recent_data[feature].dropna()
                    )
                    drift_scores[feature] = psi

                    # Update metrics
                    MODEL_FEATURE_DRIFT.labels(
                        model_name=model_name, feature_name=feature
                    ).set(psi)

            return drift_scores

        except Exception as e:
            logger.error(f"Feature drift check failed: {e}")
            return {}

    def _calculate_psi(
        self, expected: pd.Series, actual: pd.Series, buckets: int = 10
    ) -> float:
        """Calculate Population Stability Index."""
        if len(expected) == 0 or len(actual) == 0:
            return 0.0

        # Create bins based on expected distribution
        expected_min = expected.min()
        expected_max = expected.max()

        if expected_min == expected_max:
            return 0.0

        bin_edges = np.linspace(expected_min, expected_max, buckets + 1)

        # Calculate expected distribution
        expected_hist, _ = np.histogram(expected, bins=bin_edges)
        expected_perc = expected_hist / len(expected)

        # Calculate actual distribution
        actual_hist, _ = np.histogram(actual, bins=bin_edges)
        actual_perc = actual_hist / len(actual)

        # Calculate PSI
        psi = 0.0
        for i in range(len(expected_perc)):
            if expected_perc[i] > 0 and actual_perc[i] > 0:
                psi += (actual_perc[i] - expected_perc[i]) * np.log(
                    actual_perc[i] / expected_perc[i]
                )

        return psi

    def _get_feature_set_features(self, feature_set: str) -> List[str]:
        """Get features for a feature set."""
        feature_sets = {
            "voice_sentiment_v1": [
                "talk_to_listen_ratio",
                "interruption_count",
                "silence_ratio",
                "avg_pitch",
                "energy_db",
                "voice_confidence",
            ],
            "voice_quality_v1": [
                "talk_to_listen_ratio",
                "interruption_count",
                "silence_ratio",
                "jitter",
                "shimmer",
                "hnr",
                "speaking_rate",
            ],
        }
        return feature_sets.get(feature_set, [])
