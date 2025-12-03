import numpy as np
import librosa
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class AudioChunk:
    """Represents a chunk of audio data for processing."""

    call_id: str
    speaker_id: str
    timestamp: datetime
    audio_data: np.ndarray
    sample_rate: int
    chunk_duration: float


@dataclass
class VoiceFeatures:
    """Computed voice features for a specific time window."""

    call_id: str
    speaker_id: str
    timestamp: datetime
    window_duration: float

    # Temporal Features
    talk_to_listen_ratio: Optional[float] = None
    interruption_count: Optional[int] = None
    silence_ratio: Optional[float] = None
    speaking_duration: Optional[float] = None

    # Acoustic Features
    avg_pitch: Optional[float] = None
    pitch_variance: Optional[float] = None
    energy_db: Optional[float] = None
    spectral_centroid: Optional[float] = None
    mfccs: Optional[List[float]] = None  # First 13 MFCC coefficients

    # Voice Activity Detection
    voice_activity: Optional[bool] = None
    voice_confidence: Optional[float] = None

    def to_dict(self) -> Dict:
        """Convert features to dictionary for storage."""
        return {
            "call_id": self.call_id,
            "speaker_id": self.speaker_id,
            "timestamp": self.timestamp.isoformat(),
            "window_duration": self.window_duration,
            "talk_to_listen_ratio": self.talk_to_listen_ratio,
            "interruption_count": self.interruption_count,
            "silence_ratio": self.silence_ratio,
            "speaking_duration": self.speaking_duration,
            "avg_pitch": self.avg_pitch,
            "pitch_variance": self.pitch_variance,
            "energy_db": self.energy_db,
            "spectral_centroid": self.spectral_centroid,
            "mfccs": self.mfccs,
            "voice_activity": self.voice_activity,
            "voice_confidence": self.voice_confidence,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "VoiceFeatures":
        """Create VoiceFeatures from dictionary."""
        return cls(
            call_id=data["call_id"],
            speaker_id=data["speaker_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            window_duration=data["window_duration"],
            talk_to_listen_ratio=data.get("talk_to_listen_ratio"),
            interruption_count=data.get("interruption_count"),
            silence_ratio=data.get("silence_ratio"),
            speaking_duration=data.get("speaking_duration"),
            avg_pitch=data.get("avg_pitch"),
            pitch_variance=data.get("pitch_variance"),
            energy_db=data.get("energy_db"),
            spectral_centroid=data.get("spectral_centroid"),
            mfccs=data.get("mfccs"),
            voice_activity=data.get("voice_activity"),
            voice_confidence=data.get("voice_confidence"),
        )


class VoiceFeatureProcessor:
    """Processes audio chunks to compute voice features."""

    def __init__(self, sample_rate: int = 16000):
        self.sample_rate = sample_rate
        self.frame_length = 2048
        self.hop_length = 512

    def compute_features(self, audio_chunk: AudioChunk) -> VoiceFeatures:
        """Compute all voice features from an audio chunk."""
        features = VoiceFeatures(
            call_id=audio_chunk.call_id,
            speaker_id=audio_chunk.speaker_id,
            timestamp=audio_chunk.timestamp,
            window_duration=audio_chunk.chunk_duration,
        )

        # Basic acoustic features
        features.energy_db = self._compute_energy(audio_chunk.audio_data)
        features.spectral_centroid = self._compute_spectral_centroid(
            audio_chunk.audio_data
        )

        # Voice activity detection
        features.voice_activity, features.voice_confidence = (
            self._detect_voice_activity(audio_chunk.audio_data)
        )

        # Pitch-related features (only if voice activity detected)
        if features.voice_activity:
            pitch = self._compute_pitch(audio_chunk.audio_data)
            if pitch is not None:
                features.avg_pitch = np.mean(pitch)
                features.pitch_variance = np.var(pitch)

        # MFCC features
        features.mfccs = self._compute_mfcc(audio_chunk.audio_data)

        return features

    def _compute_energy(self, audio_data: np.ndarray) -> float:
        """Compute RMS energy in decibels."""
        rms = np.sqrt(np.mean(audio_data**2))
        return 20 * np.log10(rms + 1e-10)  # Avoid log(0)

    def _compute_spectral_centroid(self, audio_data: np.ndarray) -> float:
        """Compute spectral centroid."""
        try:
            spectral_centroid = librosa.feature.spectral_centroid(
                y=audio_data, sr=self.sample_rate, hop_length=self.hop_length
            )
            return float(np.mean(spectral_centroid))
        except Exception:
            return 0.0

    def _detect_voice_activity(self, audio_data: np.ndarray) -> Tuple[bool, float]:
        """Simple voice activity detection."""
        energy = self._compute_energy(audio_data)
        voice_threshold = -40.0  # dB, adjustable based on data
        confidence = min(1.0, max(0.0, (energy - voice_threshold + 20) / 40))
        return energy > voice_threshold, confidence

    def _compute_pitch(self, audio_data: np.ndarray) -> Optional[np.ndarray]:
        """Compute pitch using librosa."""
        try:
            pitches, magnitudes = librosa.piptrack(
                y=audio_data, sr=self.sample_rate, hop_length=self.hop_length
            )
            pitch = []
            for t in range(pitches.shape[1]):
                index = magnitudes[:, t].argmax()
                pitch_val = pitches[index, t]
                if pitch_val > 0:  # Valid pitch
                    pitch.append(pitch_val)
            return np.array(pitch) if pitch else None
        except Exception:
            return None

    def _compute_mfcc(self, audio_data: np.ndarray, n_mfcc: int = 13) -> List[float]:
        """Compute MFCC coefficients."""
        try:
            mfccs = librosa.feature.mfcc(
                y=audio_data,
                sr=self.sample_rate,
                n_mfcc=n_mfcc,
                hop_length=self.hop_length,
            )
            return [float(np.mean(mfccs[i])) for i in range(n_mfcc)]
        except Exception:
            return [0.0] * n_mfcc
