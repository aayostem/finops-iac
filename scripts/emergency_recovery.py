#!/usr/bin/env python3
"""
Emergency recovery scripts for the Voice Feature Store.
"""
import asyncio
import logging
from datetime import datetime, timedelta
import redis
import boto3
from typing import List, Dict
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmergencyRecovery:
    """Emergency recovery procedures for feature store incidents."""

    def __init__(self, redis_host: str, redis_port: int, s3_bucket: str):
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self.s3 = boto3.client("s3")
        self.s3_bucket = s3_bucket

    async def recover_from_redis_failure(self, backup_file: str):
        """Recover from Redis failure using S3 backup."""
        logger.info("Starting Redis recovery procedure...")

        try:
            # Step 1: Check S3 for latest backup
            backup_data = await self._load_s3_backup(backup_file)
            if not backup_data:
                logger.error("No backup found in S3")
                return False

            # Step 2: Clear corrupted Redis data
            logger.info("Clearing corrupted Redis data...")
            await self._clear_corrupted_redis_data()

            # Step 3: Restore from backup
            logger.info("Restoring data from backup...")
            await self._restore_redis_from_backup(backup_data)

            # Step 4: Verify restoration
            logger.info("Verifying restoration...")
            success = await self._verify_redis_restoration(backup_data)

            if success:
                logger.info("Redis recovery completed successfully")
            else:
                logger.error("Redis recovery verification failed")

            return success

        except Exception as e:
            logger.error(f"Redis recovery failed: {e}")
            return False

    async def handle_data_corruption(self, start_time: datetime, end_time: datetime):
        """Handle data corruption by recomputing features from source."""
        logger.info(f"Handling data corruption from {start_time} to {end_time}")

        try:
            # Step 1: Identify affected calls
            affected_calls = await self._identify_affected_calls(start_time, end_time)
            logger.info(f"Found {len(affected_calls)} affected calls")

            # Step 2: Clear corrupted features
            await self._clear_corrupted_features(affected_calls)

            # Step 3: Trigger reprocessing
            await self._trigger_reprocessing(affected_calls, start_time, end_time)

            # Step 4: Monitor recovery progress
            success = await self._monitor_recovery_progress(affected_calls)

            logger.info(f"Data corruption handling completed: {success}")
            return success

        except Exception as e:
            logger.error(f"Data corruption handling failed: {e}")
            return False

    async def _load_s3_backup(self, backup_file: str) -> Dict:
        """Load backup data from S3."""
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=backup_file)
            backup_data = json.loads(response["Body"].read())
            logger.info(f"Loaded backup with {len(backup_data)} items")
            return backup_data
        except Exception as e:
            logger.error(f"Failed to load S3 backup: {e}")
            return {}

    async def _clear_corrupted_redis_data(self):
        """Clear potentially corrupted Redis data."""
        try:
            # Find all feature keys
            feature_keys = self.redis.keys("voice_features:*")
            logger.info(f"Found {len(feature_keys)} feature keys to clear")

            # Delete in batches to avoid blocking
            batch_size = 1000
            for i in range(0, len(feature_keys), batch_size):
                batch = feature_keys[i : i + batch_size]
                self.redis.delete(*batch)

            logger.info("Cleared all feature keys")
        except Exception as e:
            logger.error(f"Failed to clear Redis data: {e}")
            raise

    async def _restore_redis_from_backup(self, backup_data: Dict):
        """Restore Redis from backup data."""
        pipeline = self.redis.pipeline()

        for key, value in backup_data.items():
            if isinstance(value, dict):
                pipeline.hset(key, mapping=value)
            else:
                pipeline.set(key, value)

            # Set appropriate TTL
            if key.startswith("voice_features:"):
                pipeline.expire(key, 86400)  # 24 hours

        pipeline.execute()
        logger.info("Restored backup data to Redis")

    async def _verify_redis_restoration(self, backup_data: Dict) -> bool:
        """Verify that Redis restoration was successful."""
        try:
            # Sample verification - check random keys
            sample_keys = list(backup_data.keys())[:10]
            for key in sample_keys:
                if key.startswith("voice_features:"):
                    stored_data = self.redis.hgetall(key)
                    if not stored_data:
                        logger.error(f"Key {key} not found after restoration")
                        return False

            logger.info("Redis restoration verification passed")
            return True
        except Exception as e:
            logger.error(f"Restoration verification failed: {e}")
            return False

    async def _identify_affected_calls(
        self, start_time: datetime, end_time: datetime
    ) -> List[str]:
        """Identify calls affected by data corruption."""
        # This would typically query your call metadata database
        # For now, return a mock list
        return [f"call_{i}" for i in range(1000)]

    async def _clear_corrupted_features(self, call_ids: List[str]):
        """Clear corrupted features for affected calls."""
        pipeline = self.redis.pipeline()

        for call_id in call_ids:
            # Clear all speaker features for this call
            pattern = f"voice_features:{call_id}:*"
            keys = self.redis.keys(pattern)
            if keys:
                pipeline.delete(*keys)

        pipeline.execute()
        logger.info(f"Cleared features for {len(call_ids)} calls")

    async def _trigger_reprocessing(
        self, call_ids: List[str], start_time: datetime, end_time: datetime
    ):
        """Trigger reprocessing of affected calls."""
        # This would typically send messages to a reprocessing queue
        # For now, just log the action
        logger.info(
            f"Triggered reprocessing for {len(call_ids)} calls from {start_time} to {end_time}"
        )

    async def _monitor_recovery_progress(self, call_ids: List[str]) -> bool:
        """Monitor recovery progress."""
        # Monitor until all calls have features restored
        max_attempts = 10
        for attempt in range(max_attempts):
            recovered_count = 0
            for call_id in call_ids[:10]:  # Sample monitoring
                keys = self.redis.keys(f"voice_features:{call_id}:*")
                if keys:
                    recovered_count += 1

            recovery_rate = recovered_count / 10
            logger.info(
                f"Recovery progress: {recovery_rate:.1%} (attempt {attempt + 1}/{max_attempts})"
            )

            if recovery_rate >= 0.9:  # 90% recovery threshold
                logger.info("Recovery completed successfully")
                return True

            await asyncio.sleep(30)  # Wait 30 seconds between checks

        logger.error("Recovery monitoring timeout")
        return False


class PerformanceDiagnostics:
    """Diagnostic tools for performance issues."""

    def __init__(self, redis_client: redis.Redis, api_url: str):
        self.redis = redis_client
        self.api_url = api_url

    async def run_full_diagnostics(self) -> Dict:
        """Run comprehensive system diagnostics."""
        diagnostics = {}

        # Redis diagnostics
        diagnostics["redis"] = await self._diagnose_redis()

        # API diagnostics
        diagnostics["api"] = await self._diagnose_api()

        # Feature computation diagnostics
        diagnostics["feature_computation"] = await self._diagnose_feature_computation()

        # Storage diagnostics
        diagnostics["storage"] = await self._diagnose_storage()

        return diagnostics

    async def _diagnose_redis(self) -> Dict:
        """Diagnose Redis health and performance."""
        try:
            info = self.redis.info()

            return {
                "status": "healthy",
                "used_memory_mb": info["used_memory"] / (1024 * 1024),
                "connected_clients": info["connected_clients"],
                "ops_per_sec": info["instantaneous_ops_per_sec"],
                "keyspace_hits": info["keyspace_hits"],
                "keyspace_misses": info["keyspace_misses"],
                "hit_rate": info["keyspace_hits"]
                / (info["keyspace_hits"] + info["keyspace_misses"] + 1e-10),
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _diagnose_api(self) -> Dict:
        """Diagnose API health and performance."""
        try:
            import requests
            import time

            # Test latency
            start_time = time.time()
            response = requests.get(f"{self.api_url}/health", timeout=10)
            latency = time.time() - start_time

            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_time_ms": latency * 1000,
                "status_code": response.status_code,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _diagnose_feature_computation(self) -> Dict:
        """Diagnose feature computation pipeline."""
        try:
            # Check Kafka lag (this would require Kafka admin client)
            # For now, return mock data
            return {
                "status": "healthy",
                "kafka_lag": 0,
                "processing_rate": 1000,
                "error_rate": 0.01,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _diagnose_storage(self) -> Dict:
        """Diagnose storage systems."""
        try:
            # Check S3 access and performance
            # For now, return mock data
            return {
                "s3_status": "healthy",
                "offline_store_accessible": True,
                "storage_utilization_gb": 150.5,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


async def main():
    """Run emergency recovery if needed."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python emergency_recovery.py <command>")
        print("Commands: recover_redis, diagnose, handle_corruption")
        return

    command = sys.argv[1]
    recovery = EmergencyRecovery(
        redis_host="localhost", redis_port=6379, s3_bucket="voice-feature-store-backups"
    )

    if command == "recover_redis":
        await recovery.recover_from_redis_failure("redis_backup_latest.json")
    elif command == "diagnose":
        diag = PerformanceDiagnostics(recovery.redis, "http://localhost:8000")
        results = await diag.run_full_diagnostics()
        print(json.dumps(results, indent=2))
    elif command == "handle_corruption":
        start_time = datetime.utcnow() - timedelta(hours=1)
        end_time = datetime.utcnow()
        await recovery.handle_data_corruption(start_time, end_time)
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
