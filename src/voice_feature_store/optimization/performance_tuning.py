import asyncio
import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import redis
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Performance Metrics
FEATURE_COMPUTATION_TIME = Histogram(
    "feature_computation_time_seconds",
    "Time spent computing features",
    ["feature_type"],
)
MODEL_INFERENCE_TIME = Histogram(
    "model_inference_time_seconds", "Model inference time", ["model_name"]
)
CACHE_HIT_RATE = Gauge("cache_hit_rate", "Cache hit rate percentage")
MEMORY_USAGE = Gauge("memory_usage_bytes", "Memory usage in bytes")
CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage percentage")


@dataclass
class PerformanceConfig:
    """Performance tuning configuration."""

    # Feature computation
    feature_batch_size: int = 100
    feature_parallelism: int = 4
    feature_cache_ttl: int = 300

    # Model inference
    model_batch_size: int = 32
    model_parallelism: int = 2
    model_cache_size: int = 1000

    # Streaming
    streaming_buffer_size: int = 1000
    streaming_parallelism: int = 8
    streaming_checkpoint_interval: int = 30000

    # Memory management
    max_memory_usage: int = 4 * 1024 * 1024 * 1024  # 4GB
    garbage_collection_threshold: float = 0.8  # 80% memory usage
    object_pool_size: int = 1000


class PerformanceOptimizer:
    """Optimize performance through various techniques."""

    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.thread_pool = ThreadPoolExecutor(max_workers=config.feature_parallelism)
        self.object_pool = self._initialize_object_pool()
        self.cache_stats = {"hits": 0, "misses": 0}

    def _initialize_object_pool(self) -> Dict[str, List[Any]]:
        """Initialize object pool for reuse."""
        return {"audio_buffers": [], "feature_arrays": [], "model_inputs": []}

    def optimize_feature_computation(
        self, audio_chunks: List[np.ndarray]
    ) -> List[Dict]:
        """Optimize feature computation using batching and parallelism."""
        start_time = time.time()

        try:
            # Batch processing
            batches = [
                audio_chunks[i : i + self.config.feature_batch_size]
                for i in range(0, len(audio_chunks), self.config.feature_batch_size)
            ]

            # Parallel computation
            futures = []
            for batch in batches:
                future = self.thread_pool.submit(self._compute_features_batch, batch)
                futures.append(future)

            # Collect results
            all_features = []
            for future in futures:
                batch_features = future.result(timeout=30.0)  # 30 second timeout
                all_features.extend(batch_features)

            computation_time = time.time() - start_time
            FEATURE_COMPUTATION_TIME.labels(feature_type="batch").observe(
                computation_time
            )

            return all_features

        except Exception as e:
            logger.error(f"Optimized feature computation failed: {e}")
            # Fallback to sequential computation
            return self._compute_features_sequential(audio_chunks)

    def _compute_features_batch(self, audio_batch: List[np.ndarray]) -> List[Dict]:
        """Compute features for a batch of audio chunks."""
        features_batch = []

        for audio in audio_batch:
            try:
                # Reuse objects from pool
                feature_array = self._get_from_pool("feature_arrays") or np.zeros(100)

                # Compute features (simplified)
                features = {
                    "energy": np.mean(audio**2),
                    "zero_crossing_rate": self._compute_zcr(audio),
                    "spectral_centroid": self._compute_spectral_centroid(audio),
                    # ... other features
                }

                features_batch.append(features)

                # Return objects to pool
                self._return_to_pool("feature_arrays", feature_array)

            except Exception as e:
                logger.error(f"Batch feature computation failed for one chunk: {e}")
                features_batch.append({})

        return features_batch

    def _compute_features_sequential(
        self, audio_chunks: List[np.ndarray]
    ) -> List[Dict]:
        """Sequential fallback for feature computation."""
        features = []
        for audio in audio_chunks:
            try:
                feature = {
                    "energy": np.mean(audio**2),
                    "zero_crossing_rate": self._compute_zcr(audio),
                    "spectral_centroid": self._compute_spectral_centroid(audio),
                }
                features.append(feature)
            except Exception:
                features.append({})
        return features

    def optimize_model_inference(self, model, features_batch: List[Dict]) -> List[Any]:
        """Optimize model inference using batching and caching."""
        start_time = time.time()

        try:
            # Prepare batch inputs
            batch_inputs = self._prepare_model_batch(features_batch)

            # Batch inference
            with torch.no_grad():
                predictions = model(batch_inputs)

            inference_time = time.time() - start_time
            MODEL_INFERENCE_TIME.labels(model_name=model.__class__.__name__).observe(
                inference_time
            )

            return predictions

        except Exception as e:
            logger.error(f"Optimized model inference failed: {e}")
            # Fallback to sequential inference
            return self._sequential_model_inference(model, features_batch)

    def _prepare_model_batch(self, features_batch: List[Dict]) -> Any:
        """Prepare batch inputs for model inference."""
        # Convert features to model input format
        # This would be model-specific in production
        batch_size = len(features_batch)
        input_size = 100  # Example input size

        # Reuse array from pool
        batch_array = self._get_from_pool("model_inputs") or np.zeros(
            (batch_size, input_size)
        )

        for i, features in enumerate(features_batch):
            # Convert features to model input (simplified)
            if i < batch_array.shape[0]:
                batch_array[i] = self._features_to_model_input(features)

        return batch_array

    def _sequential_model_inference(
        self, model, features_batch: List[Dict]
    ) -> List[Any]:
        """Sequential fallback for model inference."""
        predictions = []
        for features in features_batch:
            try:
                input_data = self._features_to_model_input(features)
                prediction = model.predict(input_data.reshape(1, -1))
                predictions.append(prediction[0])
            except Exception:
                predictions.append(None)
        return predictions

    def _compute_zcr(self, audio: np.ndarray) -> float:
        """Compute zero-crossing rate."""
        return float(np.mean(np.abs(np.diff(np.sign(audio)))) / 2)

    def _compute_spectral_centroid(self, audio: np.ndarray) -> float:
        """Compute spectral centroid."""
        try:
            spectrum = np.abs(np.fft.rfft(audio))
            normalized_spectrum = spectrum / np.sum(spectrum)
            normalized_frequencies = np.linspace(0, 1, len(spectrum))
            return float(np.sum(normalized_frequencies * normalized_spectrum))
        except Exception:
            return 0.0

    def _features_to_model_input(self, features: Dict) -> np.ndarray:
        """Convert features to model input format."""
        # Simplified conversion - would be model-specific in production
        input_array = np.zeros(100)

        feature_mapping = {
            "energy": 0,
            "zero_crossing_rate": 1,
            "spectral_centroid": 2,
            # ... other feature mappings
        }

        for feature_name, value in features.items():
            if feature_name in feature_mapping:
                idx = feature_mapping[feature_name]
                input_array[idx] = value

        return input_array

    def _get_from_pool(self, pool_name: str) -> Optional[Any]:
        """Get object from pool."""
        if pool_name in self.object_pool and self.object_pool[pool_name]:
            return self.object_pool[pool_name].pop()
        return None

    def _return_to_pool(self, pool_name: str, obj: Any):
        """Return object to pool."""
        if pool_name in self.object_pool:
            if len(self.object_pool[pool_name]) < self.config.object_pool_size:
                self.object_pool[pool_name].append(obj)

    def update_cache_stats(self, hit: bool):
        """Update cache statistics."""
        if hit:
            self.cache_stats["hits"] += 1
        else:
            self.cache_stats["misses"] += 1

        # Calculate hit rate
        total = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total * 100) if total > 0 else 0
        CACHE_HIT_RATE.set(hit_rate)

    async def monitor_resources(self):
        """Monitor system resources and adjust configuration."""
        while True:
            try:
                # Monitor memory usage
                import psutil

                process = psutil.Process()
                memory_info = process.memory_info()
                memory_usage = memory_info.rss

                MEMORY_USAGE.set(memory_usage)

                # Monitor CPU usage
                cpu_percent = process.cpu_percent()
                CPU_USAGE.set(cpu_percent)

                # Adjust configuration based on resource usage
                await self._adjust_configuration(memory_usage, cpu_percent)

                # Wait before next check
                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                logger.error(f"Resource monitoring failed: {e}")
                await asyncio.sleep(30)  # Shorter wait on error

    async def _adjust_configuration(self, memory_usage: int, cpu_percent: float):
        """Adjust configuration based on resource usage."""
        memory_threshold = (
            self.config.max_memory_usage * self.config.garbage_collection_threshold
        )

        if memory_usage > memory_threshold:
            # Reduce memory usage
            logger.warning(f"High memory usage detected: {memory_usage} bytes")
            self._reduce_memory_usage()

        if cpu_percent > 80:
            # Reduce CPU-intensive operations
            logger.warning(f"High CPU usage detected: {cpu_percent}%")
            self._reduce_cpu_usage()

    def _reduce_memory_usage(self):
        """Reduce memory usage by clearing caches and pools."""
        # Clear object pools
        for pool_name in self.object_pool:
            self.object_pool[pool_name].clear()

        # Suggest garbage collection
        import gc

        gc.collect()

        logger.info("Memory usage reduction measures applied")

    def _reduce_cpu_usage(self):
        """Reduce CPU usage by adjusting parallelism."""
        # Reduce parallelism
        if self.config.feature_parallelism > 1:
            self.config.feature_parallelism -= 1
            logger.info(
                f"Reduced feature parallelism to {self.config.feature_parallelism}"
            )

        if self.config.model_parallelism > 1:
            self.config.model_parallelism -= 1
            logger.info(f"Reduced model parallelism to {self.config.model_parallelism}")

    def cleanup(self):
        """Clean up resources."""
        self.thread_pool.shutdown(wait=True)
        for pool in self.object_pool.values():
            pool.clear()


class AdvancedCachingStrategy:
    """Advanced caching strategy with multiple cache levels."""

    def __init__(self, redis_client: redis.Redis, local_cache_size: int = 10000):
        self.redis = redis_client
        self.local_cache = {}
        self.local_cache_size = local_cache_size
        self.access_patterns = {}  # Track access patterns for optimization

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with multi-level lookup."""
        start_time = time.time()

        # Level 1: Local memory cache
        if key in self.local_cache:
            value, timestamp = self.local_cache[key]
            if time.time() - timestamp < 300:  # 5-minute TTL for local cache
                self._record_access(key, "local_hit")
                return value
            else:
                # Expired, remove from local cache
                del self.local_cache[key]

        # Level 2: Redis cache
        try:
            redis_value = self.redis.get(key)
            if redis_value is not None:
                value = self._deserialize(redis_value)

                # Store in local cache
                self._set_local_cache(key, value)

                self._record_access(key, "redis_hit")
                return value
        except Exception as e:
            logger.warning(f"Redis cache access failed: {e}")

        self._record_access(key, "miss")
        return None

    def set(self, key: str, value: Any, ttl: int = 3600):
        """Set value in cache with multi-level storage."""
        try:
            # Level 1: Local memory cache
            self._set_local_cache(key, value)

            # Level 2: Redis cache
            serialized_value = self._serialize(value)
            self.redis.setex(key, ttl, serialized_value)

            self._record_access(key, "set")

        except Exception as e:
            logger.error(f"Cache set operation failed: {e}")

    def _set_local_cache(self, key: str, value: Any):
        """Set value in local cache with LRU eviction."""
        if len(self.local_cache) >= self.local_cache_size:
            # Remove least recently used item
            lru_key = min(
                self.access_patterns.items(), key=lambda x: x[1]["last_access"]
            )[0]
            if lru_key in self.local_cache:
                del self.local_cache[lru_key]
                del self.access_patterns[lru_key]

        self.local_cache[key] = (value, time.time())

    def _record_access(self, key: str, access_type: str):
        """Record cache access pattern."""
        if key not in self.access_patterns:
            self.access_patterns[key] = {
                "access_count": 0,
                "last_access": time.time(),
                "hit_types": [],
            }

        self.access_patterns[key]["access_count"] += 1
        self.access_patterns[key]["last_access"] = time.time()
        self.access_patterns[key]["hit_types"].append(access_type)

        # Limit history size
        if len(self.access_patterns[key]["hit_types"]) > 100:
            self.access_patterns[key]["hit_types"] = self.access_patterns[key][
                "hit_types"
            ][-50:]

    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage."""
        import pickle

        return pickle.dumps(value)

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from storage."""
        import pickle

        return pickle.loads(data)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        total_accesses = sum(
            pattern["access_count"] for pattern in self.access_patterns.values()
        )
        hit_types = {}

        for pattern in self.access_patterns.values():
            for hit_type in pattern["hit_types"]:
                hit_types[hit_type] = hit_types.get(hit_type, 0) + 1

        return {
            "local_cache_size": len(self.local_cache),
            "total_accesses": total_accesses,
            "hit_types": hit_types,
            "hit_rate": (
                (hit_types.get("local_hit", 0) + hit_types.get("redis_hit", 0))
                / total_accesses
                if total_accesses > 0
                else 0
            ),
            "most_accessed_keys": sorted(
                self.access_patterns.items(),
                key=lambda x: x[1]["access_count"],
                reverse=True,
            )[:10],
        }
