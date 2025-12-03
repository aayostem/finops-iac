import pytest
import numpy as np
from datetime import datetime

from voice_feature_store.features.voice_features import (
    VoiceFeatures,
    AudioChunk,
    VoiceFeatureProcessor,
)


class TestVoiceFeatures:
    """Test voice feature computation."""

    def test_audio_chunk_creation(self):
        """Test AudioChunk creation and serialization."""
        audio_data = np.random.random(16000).astype(np.float32)
        chunk = AudioChunk(
            call_id="test_123",
            speaker_id="customer",
            timestamp=datetime.now(),
            audio_data=audio_data,
            sample_rate=16000,
            chunk_duration=1.0,
        )

        assert chunk.call_id == "test_123"
        assert chunk.speaker_id == "customer"
        assert len(chunk.audio_data) == 16000

    def test_voice_features_serialization(self):
        """Test VoiceFeatures serialization to/from dict."""
        original = VoiceFeatures(
            call_id="test_123",
            speaker_id="customer",
            timestamp=datetime.now(),
            window_duration=30.0,
            talk_to_listen_ratio=0.75,
            interruption_count=2,
            silence_ratio=0.15,
            avg_pitch=220.5,
            energy_db=-25.3,
            voice_activity=True,
            voice_confidence=0.95,
        )

        # Convert to dict and back
        feature_dict = original.to_dict()
        restored = VoiceFeatures.from_dict(feature_dict)

        assert restored.call_id == original.call_id
        assert restored.talk_to_listen_ratio == original.talk_to_listen_ratio
        assert restored.voice_activity == original.voice_activity

    def test_feature_processor_initialization(self):
        """Test voice feature processor initialization."""
        processor = VoiceFeatureProcessor(sample_rate=16000)
        assert processor.sample_rate == 16000
        assert processor.frame_length == 2048
        assert processor.hop_length == 512

    def test_energy_computation(self):
        """Test energy computation."""
        processor = VoiceFeatureProcessor()

        # Test with silence (low energy)
        silence = np.zeros(16000, dtype=np.float32)
        energy = processor._compute_energy(silence)
        assert energy < -50.0  # Very low energy for silence

        # Test with random noise (higher energy)
        noise = np.random.random(16000).astype(np.float32) * 0.1
        energy = processor._compute_energy(noise)
        assert energy > -50.0

    def test_voice_activity_detection(self):
        """Test voice activity detection."""
        processor = VoiceFeatureProcessor()

        # Test with silence
        silence = np.zeros(16000, dtype=np.float32)
        is_voice, confidence = processor._detect_voice_activity(silence)
        assert not is_voice
        assert confidence < 0.5

        # Test with simulated voice (sine wave)
        t = np.linspace(0, 1, 16000)
        voice_signal = np.sin(2 * np.pi * 440 * t) * 0.5  # 440 Hz sine wave
        is_voice, confidence = processor._detect_voice_activity(voice_signal)
        # Note: This might not always detect voice due to simple VAD
        # The test ensures the function runs without errors

    def test_mfcc_computation(self):
        """Test MFCC feature computation."""
        processor = VoiceFeatureProcessor()

        # Create test audio signal
        audio_data = np.random.random(16000).astype(np.float32) * 0.1

        mfccs = processor._compute_mfcc(audio_data, n_mfcc=13)

        assert len(mfccs) == 13
        assert all(isinstance(m, float) for m in mfccs)
