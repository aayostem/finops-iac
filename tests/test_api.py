import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from voice_feature_store.api.server import app
from voice_feature_store.features.voice_features import VoiceFeatures


class TestAPI:
    """Test API endpoints."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert data["message"] == "Voice Feature Store API"

    def test_health_check_success(self, client):
        """Test health check endpoint with successful dependencies."""
        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.health_check.return_value = True
            mock_store.return_value = mock_instance

            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["overall"] == "healthy"

    def test_health_check_failure(self, client):
        """Test health check endpoint with failed dependency."""
        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.health_check.return_value = False
            mock_store.return_value = mock_instance

            response = client.get("/health")
            assert response.status_code == 503
            data = response.json()
            assert data["overall"] == "unhealthy"

    def test_get_features_success(self, client):
        """Test successful feature retrieval."""
        mock_features = VoiceFeatures(
            call_id="test_123",
            speaker_id="customer",
            timestamp="2023-10-27T10:00:00",
            window_duration=30.0,
            talk_to_listen_ratio=0.75,
            interruption_count=2,
        )

        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.get_latest_features.return_value = mock_features
            mock_store.return_value = mock_instance

            response = client.get("/features/test_123/customer")
            assert response.status_code == 200
            data = response.json()
            assert data["call_id"] == "test_123"
            assert data["features"]["talk_to_listen_ratio"] == 0.75

    def test_get_features_not_found(self, client):
        """Test feature retrieval when not found."""
        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.get_latest_features.return_value = None
            mock_store.return_value = mock_instance

            response = client.get("/features/nonexistent/customer")
            assert response.status_code == 404

    def test_get_feature_vector(self, client):
        """Test feature vector retrieval."""
        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.get_feature_vector.return_value = {
                "talk_to_listen_ratio": 0.75,
                "interruption_count": 2,
                "energy_db": -25.5,
            }
            mock_store.return_value = mock_instance

            response = client.get(
                "/features/test_123/customer",
                params={
                    "feature_names": ["talk_to_listen_ratio", "interruption_count"]
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert "talk_to_listen_ratio" in data["features"]
            assert data["features"]["talk_to_listen_ratio"] == 0.75

    def test_batch_features(self, client):
        """Test batch feature retrieval."""
        with patch("voice_feature_store.api.server.OnlineFeatureStore") as mock_store:
            mock_instance = Mock()
            mock_instance.get_feature_vector.return_value = {
                "talk_to_listen_ratio": 0.75,
                "energy_db": -25.5,
            }
            mock_store.return_value = mock_instance

            response = client.get(
                "/features/batch",
                params={"call_speaker_pairs": ["call1:speaker1", "call2:speaker2"]},
            )
            assert response.status_code == 200
            data = response.json()
            assert "results" in data
            assert "call1:speaker1" in data["results"]

    def test_metrics_endpoint(self, client):
        """Test Prometheus metrics endpoint."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "voice_feature_store" in response.text

    def test_feature_registry_endpoint(self, client):
        """Test feature registry endpoint."""
        response = client.get("/feature-registry")
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        # Should contain our core features
        feature_names = [f["name"] for f in data["features"]]
        assert "talk_to_listen_ratio" in feature_names
        assert "interruption_count" in feature_names
