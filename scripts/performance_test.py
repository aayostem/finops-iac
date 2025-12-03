#!/usr/bin/env python3
"""
Performance testing script for the feature store.
"""
import time
import asyncio
import aiohttp
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PerformanceTester:
    """Performance tester for the feature store API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def test_single_feature_request(self, call_id: str, speaker_id: str) -> float:
        """Test latency of a single feature request."""
        start_time = time.time()
        try:
            async with self.session.get(
                f"{self.base_url}/features/{call_id}/{speaker_id}"
            ) as response:
                if response.status == 200:
                    await response.json()
                    return time.time() - start_time
                else:
                    logger.error(f"Request failed with status {response.status}")
                    return -1
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return -1

    async def test_batch_feature_request(self, call_speaker_pairs: list) -> float:
        """Test latency of a batch feature request."""
        start_time = time.time()
        try:
            params = {"call_speaker_pairs": call_speaker_pairs}
            async with self.session.get(
                f"{self.base_url}/features/batch", params=params
            ) as response:
                if response.status == 200:
                    await response.json()
                    return time.time() - start_time
                else:
                    logger.error(f"Batch request failed with status {response.status}")
                    return -1
        except Exception as e:
            logger.error(f"Batch request failed: {e}")
            return -1

    async def run_concurrent_tests(self, num_requests: int, concurrency: int):
        """Run concurrent performance tests."""
        logger.info(f"Running {num_requests} requests with concurrency {concurrency}")

        # Generate test data
        test_pairs = [
            (f"test_call_{i}", f"speaker_{i % 2}") for i in range(num_requests)
        ]

        # Test single requests
        single_latencies = []
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded_single_test(pair):
            async with semaphore:
                return await self.test_single_feature_request(pair[0], pair[1])

        tasks = [bounded_single_test(pair) for pair in test_pairs]
        single_latencies = await asyncio.gather(*tasks)

        # Filter out failed requests
        successful_latencies = [lat for lat in single_latencies if lat > 0]

        if successful_latencies:
            logger.info("Single Request Results:")
            logger.info(
                f"  Successful requests: {len(successful_latencies)}/{num_requests}"
            )
            logger.info(
                f"  Average latency: {statistics.mean(successful_latencies):.3f}s"
            )
            logger.info(
                f"  P95 latency: {statistics.quantiles(successful_latencies, n=20)[18]:.3f}s"
            )
            logger.info(f"  Max latency: {max(successful_latencies):.3f}s")

        # Test batch requests (in batches of 10)
        batch_latencies = []
        batch_size = 10

        for i in range(0, len(test_pairs), batch_size):
            batch = test_pairs[i : i + batch_size]
            batch_str = [f"{call}:{speaker}" for call, speaker in batch]
            latency = await self.test_batch_feature_request(batch_str)
            if latency > 0:
                batch_latencies.append(latency)

        if batch_latencies:
            logger.info("Batch Request Results:")
            logger.info(f"  Average latency: {statistics.mean(batch_latencies):.3f}s")
            logger.info(
                f"  Latency per feature: {statistics.mean(batch_latencies)/batch_size:.3f}s"
            )


async def main():
    """Run performance tests."""
    async with PerformanceTester() as tester:
        # Warm up
        logger.info("Warming up...")
        for _ in range(5):
            await tester.test_single_feature_request("test_call_1", "speaker_0")

        # Run tests
        await tester.run_concurrent_tests(num_requests=100, concurrency=10)
        await tester.run_concurrent_tests(num_requests=500, concurrency=50)


if __name__ == "__main__":
    asyncio.run(main())
