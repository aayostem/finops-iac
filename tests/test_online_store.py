import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime

from voice_feature_store.storage.online_store import OnlineFeatureStore
from voice_feature_store.features.voice_features import VoiceFeatures


class TestOnlineStore:
    """Test online feature store functionality."""

    def test_health_check_success(self, online_store, mock_redis):
        """Test successful health check."""
        mock_redis.ping.return_value = True
        assert online_store.health_check() is True

    def test_health_check_failure(self, online_store, mock_redis):
        """Test failed health check."""
        mock_redis.ping.side_effect = Exception("Connection failed")
        assert online_store.health_check() is False

    def test_store_features_success(
        self, online_store, mock_redis, sample_voice_features
    ):
        """Test successful feature storage."""
        mock_redis.pipeline.return_value = mock_redis
        mock_redis.execute.return_value = [1, 1]  # Simulate successful operations

        result = online_store.store_features(sample_voice_features)

        assert result is True
        assert mock_redis.hset.call_count > 0
        assert mock_redis.expire.call_count > 0

    def test_store_features_failure(
        self, online_store, mock_redis, sample_voice_features
    ):
        """Test feature storage failure."""
        mock_redis.pipeline.side_effect = Exception("Redis error")

        result = online_store.store_features(sample_voice_features)

        assert result is False

    def test_get_latest_features_success(self, online_store, mock_redis):
        """Test successful feature retrieval."""
        # Mock Redis response
        mock_redis.hgetall.return_value = {
            "call_id": "test_123",
            "speaker_id": "customer",
            "timestamp": "2023-10-27T10:00:00",
            "window_duration": "30.0",
            "talk_to_listen_ratio": "0.75",
            "interruption_count": "2",
            "voice_activity": "True",
        }

        features = online_store.get_latest_features("test_123", "customer")

        assert features is not None
        assert features.call_id == "test_123"
        assert features.talk_to_listen_ratio == 0.75
        assert features.interruption_count == 2
        assert features.voice_activity is True

    def test_get_latest_features_not_found(self, online_store, mock_redis):
        """Test feature retrieval when not found."""
        mock_redis.hgetall.return_value = {}

        features = online_store.get_latest_features("nonexistent", "customer")

        assert features is None

    def test_get_feature_vector(self, online_store, mock_redis):
        """Test feature vector retrieval."""
        mock_redis.hmget.return_value = ["0.75", "2", "-25.5"]

        feature_vector = online_store.get_feature_vector(
            "test_123",
            "customer",
            ["talk_to_listen_ratio", "interruption_count", "energy_db"],
        )

        assert "talk_to_listen_ratio" in feature_vector
        assert feature_vector["talk_to_listen_ratio"] == 0.75
        assert feature_vector["interruption_count"] == 2
        assert feature_vector["energy_db"] == -25.5

    def test_key_generation(self, online_store):
        """Test Redis key generation."""
        feature_key = online_store._get_feature_key("call_123", "speaker_456")
        assert feature_key == "voice_features:call_123:speaker_456"

        window_key = online_store._get_window_key(
            "call_123", "speaker_456", "202310271000"
        )
        assert window_key == "voice_features:call_123:speaker_456:202310271000"
