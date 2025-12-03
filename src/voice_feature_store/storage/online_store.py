import redis
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from ..features.voice_features import VoiceFeatures
from config.settings import settings


logger = logging.getLogger(__name__)


class OnlineFeatureStore:
    """Redis-based online feature store for low-latency feature serving."""

    def __init__(self):
        self.redis_client = self._create_redis_client()
        self.feature_ttl = timedelta(hours=24)  # TTL for feature keys

    def _create_redis_client(self) -> redis.Redis:
        """Create and return Redis client."""
        return redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

    def _get_feature_key(self, call_id: str, speaker_id: str) -> str:
        """Generate Redis key for features."""
        return f"voice_features:{call_id}:{speaker_id}"

    def _get_window_key(self, call_id: str, speaker_id: str, window: str) -> str:
        """Generate Redis key for windowed features."""
        return f"voice_features:{call_id}:{speaker_id}:{window}"

    def store_features(self, features: VoiceFeatures) -> bool:
        """Store voice features in Redis."""
        try:
            feature_key = self._get_feature_key(features.call_id, features.speaker_id)

            # Store latest features as hash
            feature_data = features.to_dict()
            pipeline = self.redis_client.pipeline()

            # Store individual features for easy access
            for field, value in feature_data.items():
                if value is not None:
                    if isinstance(value, list):
                        pipeline.hset(feature_key, field, json.dumps(value))
                    else:
                        pipeline.hset(feature_key, field, str(value))

            # Add timestamp and set TTL
            pipeline.hset(feature_key, "last_updated", features.timestamp.isoformat())
            pipeline.expire(feature_key, int(self.feature_ttl.total_seconds()))

            # Also store in windowed key for historical tracking
            window_key = self._get_window_key(
                features.call_id,
                features.speaker_id,
                features.timestamp.strftime("%Y%m%d%H%M"),
            )
            pipeline.setex(
                window_key,
                int(self.feature_ttl.total_seconds()),
                json.dumps(feature_data),
            )

            pipeline.execute()
            logger.debug(
                f"Stored features for {features.call_id}:{features.speaker_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            return False

    def get_latest_features(
        self, call_id: str, speaker_id: str
    ) -> Optional[VoiceFeatures]:
        """Retrieve latest features for a call/speaker."""
        try:
            feature_key = self._get_feature_key(call_id, speaker_id)
            feature_data = self.redis_client.hgetall(feature_key)

            if not feature_data:
                return None

            # Parse the feature data
            parsed_data = {}
            for key, value in feature_data.items():
                if key in ["mfccs"] and value:
                    try:
                        parsed_data[key] = json.loads(value)
                    except json.JSONDecodeError:
                        parsed_data[key] = value
                elif key in [
                    "talk_to_listen_ratio",
                    "silence_ratio",
                    "avg_pitch",
                    "pitch_variance",
                    "energy_db",
                    "spectral_centroid",
                    "voice_confidence",
                    "window_duration",
                    "speaking_duration",
                ]:
                    parsed_data[key] = float(value) if value else None
                elif key == "interruption_count":
                    parsed_data[key] = int(value) if value else None
                elif key == "voice_activity":
                    parsed_data[key] = value.lower() == "true" if value else None
                else:
                    parsed_data[key] = value

            return VoiceFeatures.from_dict(parsed_data)

        except Exception as e:
            logger.error(f"Failed to retrieve features for {call_id}:{speaker_id}: {e}")
            return None

    def get_feature_vector(
        self, call_id: str, speaker_id: str, feature_names: List[str]
    ) -> Dict[str, Any]:
        """Get specific features as a vector for model inference."""
        try:
            feature_key = self._get_feature_key(call_id, speaker_id)
            features = self.redis_client.hmget(feature_key, feature_names)

            result = {}
            for name, value in zip(feature_names, features):
                if value is not None:
                    try:
                        # Try to convert to float
                        result[name] = float(value)
                    except (ValueError, TypeError):
                        result[name] = value
                else:
                    result[name] = None

            return result

        except Exception as e:
            logger.error(
                f"Failed to get feature vector for {call_id}:{speaker_id}: {e}"
            )
            return {name: None for name in feature_names}

    def health_check(self) -> bool:
        """Check if Redis is accessible."""
        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
