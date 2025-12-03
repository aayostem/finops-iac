import time
import logging
from functools import wraps
from prometheus_client import Counter, Histogram, Gauge

# Feature Store Metrics
FEATURE_COMPUTATION_TIME = Histogram(
    "feature_computation_seconds", "Time spent computing features", ["feature_type"]
)

FEATURE_STORE_REQUESTS = Counter(
    "feature_store_requests_total",
    "Total feature store requests",
    ["store_type", "operation", "status"],
)

ONLINE_STORE_LATENCY = Histogram(
    "online_store_latency_seconds", "Online store operation latency", ["operation"]
)

ACTIVE_CALLS = Gauge("active_calls_total", "Number of currently active calls")

FEATURE_CACHE_HITS = Counter(
    "feature_cache_hits_total", "Number of feature cache hits", ["feature_type"]
)


def monitor_feature_computation(feature_type):
    """Decorator to monitor feature computation performance."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                FEATURE_COMPUTATION_TIME.labels(feature_type=feature_type).observe(
                    time.time() - start_time
                )
                return result
            except Exception as e:
                logging.error(f"Feature computation failed for {feature_type}: {e}")
                raise

        return wrapper

    return decorator


def track_store_operation(store_type, operation):
    """Decorator to track store operations."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                FEATURE_STORE_REQUESTS.labels(
                    store_type=store_type, operation=operation, status="success"
                ).inc()
                return result
            except Exception as e:
                FEATURE_STORE_REQUESTS.labels(
                    store_type=store_type, operation=operation, status="error"
                ).inc()
                logging.error(f"Store operation failed {store_type}.{operation}: {e}")
                raise

        return wrapper

    return decorator
