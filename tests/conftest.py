import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from voice_feature_store.features.voice_features import VoiceFeatures, AudioChunk
from voice_feature_store.storage.online_store import OnlineFeatureStore
from voice_feature_store.storage.offline_store import OfflineFeatureStore
from config.settings import settings


@pytest.fixture
def sample_audio_chunk():
    """Provide sample audio chunk for testing."""
    return AudioChunk(
        call_id="test_call_123",
        speaker_id="speaker_customer",
        timestamp="2023-10-27T10:00:00",
        audio_data=np.random.random(16000).astype(np.float32),  # 1 second of audio
        sample_rate=16000,
        chunk_duration=1.0,
    )


@pytest.fixture
def sample_voice_features():
    """Provide sample voice features for testing."""
    return VoiceFeatures(
        call_id="test_call_123",
        speaker_id="speaker_customer",
        timestamp="2023-10-27T10:00:00",
        window_duration=30.0,
        talk_to_listen_ratio=0.75,
        interruption_count=2,
        silence_ratio=0.15,
        speaking_duration=25.5,
        avg_pitch=220.5,
        pitch_variance=15.2,
        energy_db=-25.3,
        spectral_centroid=1500.0,
        mfccs=[
            -5.0,
            2.1,
            -0.5,
            0.3,
            -0.2,
            0.1,
            -0.05,
            0.02,
            -0.01,
            0.005,
            -0.002,
            0.001,
            0.0005,
        ],
        voice_activity=True,
        voice_confidence=0.95,
    )


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock_client = Mock()
    mock_client.ping.return_value = True
    mock_client.hset.return_value = True
    mock_client.hgetall.return_value = {}
    mock_client.hmget.return_value = [None, None]
    return mock_client


@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    mock_client = Mock()
    mock_client.upload_file.return_value = None
    mock_client.list_objects_v2.return_value = {"Contents": []}
    return mock_client


@pytest.fixture
def online_store(mock_redis):
    """Online feature store with mocked Redis."""
    store = OnlineFeatureStore()
    store.redis_client = mock_redis
    return store


@pytest.fixture
def offline_store(mock_s3_client):
    """Offline feature store with mocked S3."""
    store = OfflineFeatureStore()
    store.s3_client = mock_s3_client
    return store


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
