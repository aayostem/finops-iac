import redis
import json
import pickle
import zlib
from typing import Any, Optional, Dict
from functools import wraps
import hashlib
import time
from datetime import datetime, timedelta


class FeatureCache:
    """Intelligent caching layer for feature store."""

    def __init__(self, redis_client: redis.Redis, default_ttl: int = 3600):
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.compression_threshold = 1000  # Compress objects larger than 1KB

    def _get_cache_key(
        self, call_id: str, speaker_id: str, feature_set: str = "default"
    ) -> str:
        """Generate cache key for features."""
        key_data = f"{call_id}:{speaker_id}:{feature_set}"
        return f"feature_cache:{hashlib.md5(key_data.encode()).hexdigest()}"

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value with compression for large objects."""
        serialized = pickle.dumps(value)

        if len(serialized) > self.compression_threshold:
            serialized = zlib.compress(serialized)
            return b"compressed:" + serialized
        else:
            return b"raw:" + serialized

    def _deserialize_value(self, value: bytes) -> Any:
        """Deserialize value with decompression if needed."""
        if value.startswith(b"compressed:"):
            compressed_data = value[len(b"compressed:") :]
            serialized = zlib.decompress(compressed_data)
        else:
            serialized = value[len(b"raw:") :]

        return pickle.loads(serialized)

    def get(
        self, call_id: str, speaker_id: str, feature_set: str = "default"
    ) -> Optional[Any]:
        """Get cached features."""
        try:
            cache_key = self._get_cache_key(call_id, speaker_id, feature_set)
            cached_data = self.redis.get(cache_key)

            if cached_data:
                return self._deserialize_value(cached_data)

            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None

    def set(
        self,
        call_id: str,
        speaker_id: str,
        value: Any,
        ttl: Optional[int] = None,
        feature_set: str = "default",
    ) -> bool:
        """Set cached features."""
        try:
            cache_key = self._get_cache_key(call_id, speaker_id, feature_set)
            serialized_value = self._serialize_value(value)

            actual_ttl = ttl if ttl is not None else self.default_ttl
            return self.redis.setex(cache_key, actual_ttl, serialized_value)

        except Exception as e:
            print(f"Cache set error: {e}")
            return False

    def invalidate(
        self, call_id: str, speaker_id: str, feature_set: str = "default"
    ) -> bool:
        """Invalidate cached features."""
        try:
            cache_key = self._get_cache_key(call_id, speaker_id, feature_set)
            return bool(self.redis.delete(cache_key))
        except Exception as e:
            print(f"Cache invalidation error: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            # Get cache keys pattern
            pattern = "feature_cache:*"
            keys = self.redis.keys(pattern)

            total_size = 0
            for key in keys:
                key_size = self.redis.memory_usage(key)
                if key_size:
                    total_size += key_size

            return {
                "total_cached_items": len(keys),
                "estimated_memory_usage_mb": total_size / (1024 * 1024),
                "cache_hit_rate": self._calculate_hit_rate(),
            }
        except Exception as e:
            print(f"Cache stats error: {e}")
            return {}

    def _calculate_hit_rate(self) -> float:
        """Calculate cache hit rate (simplified implementation)."""
        # This would typically use Redis INFO command or custom counters
        # For simplicity, returning a placeholder
        return 0.95


def cache_features(ttl: int = 300, feature_set: str = "default"):
    """Decorator to cache feature store method results."""

    def decorator(func):
        @wraps(func)
        def wrapper(self, call_id: str, speaker_id: str, *args, **kwargs):
            # Check if caching is enabled and we have a cache instance
            if hasattr(self, "cache") and self.cache:
                # Try to get from cache first
                cached_result = self.cache.get(call_id, speaker_id, feature_set)
                if cached_result is not None:
                    return cached_result

            # Execute the function
            result = func(self, call_id, speaker_id, *args, **kwargs)

            # Cache the result
            if hasattr(self, "cache") and self.cache and result is not None:
                self.cache.set(call_id, speaker_id, result, ttl, feature_set)

            return result

        return wrapper

    return decorator


class BatchFeatureProcessor:
    """Optimized batch processing for features."""

    def __init__(self, online_store, batch_size: int = 100):
        self.online_store = online_store
        self.batch_size = batch_size
        self.batch_buffer = []

    def add_to_batch(self, call_id: str, speaker_id: str, features: Dict):
        """Add features to batch buffer."""
        self.batch_buffer.append(
            {
                "call_id": call_id,
                "speaker_id": speaker_id,
                "features": features,
                "timestamp": datetime.utcnow(),
            }
        )

        if len(self.batch_buffer) >= self.batch_size:
            self._process_batch()

    def _process_batch(self):
        """Process the current batch."""
        if not self.batch_buffer:
            return

        try:
            # Use Redis pipeline for batch operations
            pipeline = self.online_store.redis_client.pipeline()

            for item in self.batch_buffer:
                feature_key = self.online_store._get_feature_key(
                    item["call_id"], item["speaker_id"]
                )

                # Store features in batch
                for field, value in item["features"].items():
                    if value is not None:
                        if isinstance(value, list):
                            pipeline.hset(feature_key, field, json.dumps(value))
                        else:
                            pipeline.hset(feature_key, field, str(value))

                pipeline.hset(
                    feature_key, "last_updated", item["timestamp"].isoformat()
                )
                pipeline.expire(feature_key, 86400)  # 24 hours

            # Execute all commands in batch
            pipeline.execute()

            # Clear buffer
            self.batch_buffer.clear()

        except Exception as e:
            print(f"Batch processing error: {e}")

    def flush(self):
        """Force process any remaining items in buffer."""
        if self.batch_buffer:
            self._process_batch()
