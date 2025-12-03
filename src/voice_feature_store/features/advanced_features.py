import numpy as np
import librosa
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import torch
import torchaudio
from scipy import signal
from scipy.stats import kurtosis, skew
import warnings

warnings.filterwarnings("ignore")


@dataclass
class AdvancedVoiceFeatures:
    """Advanced voice features for sophisticated voice analytics."""

    call_id: str
    speaker_id: str
    timestamp: datetime

    # Prosody Features
    pitch_contour: Optional[List[float]] = None
    intensity_contour: Optional[List[float]] = None
    speaking_rate: Optional[float] = None
    articulation_rate: Optional[float] = None

    # Spectral Features
    spectral_rolloff: Optional[float] = None
    spectral_bandwidth: Optional[float] = None
    spectral_flatness: Optional[float] = None
    zero_crossing_rate: Optional[float] = None
    harmonic_ratio: Optional[float] = None

    # Statistical Features
    jitter: Optional[float] = None  # Pitch variation
    shimmer: Optional[float] = None  # Amplitude variation
    hnr: Optional[float] = None  # Harmonics-to-noise ratio

    # Emotion-related Features
    formants: Optional[List[float]] = None  # F1, F2, F3 frequencies
    voice_quality: Optional[str] = None  # breathy, pressed, modal

    # Behavioral Features
    turn_taking_pattern: Optional[List[float]] = None
    response_latency: Optional[float] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        return {
            "call_id": self.call_id,
            "speaker_id": self.speaker_id,
            "timestamp": self.timestamp.isoformat(),
            "pitch_contour": self.pitch_contour,
            "intensity_contour": self.intensity_contour,
            "speaking_rate": self.speaking_rate,
            "articulation_rate": self.articulation_rate,
            "spectral_rolloff": self.spectral_rolloff,
            "spectral_bandwidth": self.spectral_bandwidth,
            "spectral_flatness": self.spectral_flatness,
            "zero_crossing_rate": self.zero_crossing_rate,
            "harmonic_ratio": self.harmonic_ratio,
            "jitter": self.jitter,
            "shimmer": self.shimmer,
            "hnr": self.hnr,
            "formants": self.formants,
            "voice_quality": self.voice_quality,
            "turn_taking_pattern": self.turn_taking_pattern,
            "response_latency": self.response_latency,
        }


class AdvancedVoiceAnalyzer:
    """Advanced voice feature analyzer using signal processing and ML."""

    def __init__(self, sample_rate: int = 16000):
        self.sample_rate = sample_rate
        self.frame_length = 2048
        self.hop_length = 512

        # Check for GPU availability
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def extract_advanced_features(
        self, audio_data: np.ndarray
    ) -> AdvancedVoiceFeatures:
        """Extract comprehensive advanced voice features."""
        features = AdvancedVoiceFeatures(
            call_id="temp",  # To be filled by caller
            speaker_id="temp",
            timestamp=datetime.utcnow(),
        )

        # Prosody analysis
        features.pitch_contour = self._extract_pitch_contour(audio_data)
        features.intensity_contour = self._extract_intensity_contour(audio_data)
        features.speaking_rate = self._estimate_speaking_rate(audio_data)

        # Spectral analysis
        features.spectral_rolloff = self._compute_spectral_rolloff(audio_data)
        features.spectral_bandwidth = self._compute_spectral_bandwidth(audio_data)
        features.spectral_flatness = self._compute_spectral_flatness(audio_data)
        features.zero_crossing_rate = self._compute_zcr(audio_data)
        features.harmonic_ratio = self._compute_harmonic_ratio(audio_data)

        # Voice quality measures
        features.jitter = self._compute_jitter(audio_data)
        features.shimmer = self._compute_shimmer(audio_data)
        features.hnr = self._compute_hnr(audio_data)

        # Formant analysis
        features.formants = self._extract_formants(audio_data)
        features.voice_quality = self._classify_voice_quality(audio_data)

        return features

    def _extract_pitch_contour(
        self, audio_data: np.ndarray, method: str = "pyin"
    ) -> List[float]:
        """Extract pitch contour using advanced methods."""
        try:
            if method == "pyin":
                f0, voiced_flag, voiced_probs = librosa.pyin(
                    audio_data,
                    fmin=50,
                    fmax=400,
                    sr=self.sample_rate,
                    frame_length=self.frame_length,
                    hop_length=self.hop_length,
                )
                pitch_contour = f0[voiced_flag].tolist()
            else:
                # Fallback to basic pitch detection
                pitches, magnitudes = librosa.piptrack(
                    y=audio_data, sr=self.sample_rate, hop_length=self.hop_length
                )
                pitch_contour = []
                for t in range(pitches.shape[1]):
                    index = magnitudes[:, t].argmax()
                    pitch = pitches[index, t]
                    if pitch > 0:
                        pitch_contour.append(pitch)

            return pitch_contour[:100]  # Limit contour length

        except Exception as e:
            print(f"Pitch contour extraction failed: {e}")
            return []

    def _extract_intensity_contour(self, audio_data: np.ndarray) -> List[float]:
        """Extract intensity contour."""
        try:
            # Compute RMS energy per frame
            frames = librosa.util.frame(
                audio_data, frame_length=self.frame_length, hop_length=self.hop_length
            )
            rms_energy = np.sqrt(np.mean(frames**2, axis=0))
            return rms_energy.tolist()[:50]  # Limit length
        except Exception:
            return []

    def _estimate_speaking_rate(self, audio_data: np.ndarray) -> float:
        """Estimate speaking rate in syllables per second."""
        try:
            # Simple heuristic based on zero-crossing rate peaks
            zcr = librosa.feature.zero_crossing_rate(
                audio_data, frame_length=self.frame_length, hop_length=self.hop_length
            )
            zcr_mean = np.mean(zcr)

            # Normalize to typical speaking rate range (3-6 syllables/sec)
            speaking_rate = 3 + (zcr_mean * 3)  # Heuristic mapping
            return min(6.0, max(3.0, speaking_rate))

        except Exception:
            return 4.0  # Default average speaking rate

    def _compute_spectral_rolloff(
        self, audio_data: np.ndarray, percentile: float = 0.85
    ) -> float:
        """Compute spectral rolloff point."""
        try:
            rolloff = librosa.feature.spectral_rolloff(
                y=audio_data,
                sr=self.sample_rate,
                hop_length=self.hop_length,
                roll_percent=percentile,
            )
            return float(np.mean(rolloff))
        except Exception:
            return 0.0

    def _compute_spectral_bandwidth(self, audio_data: np.ndarray) -> float:
        """Compute spectral bandwidth."""
        try:
            bandwidth = librosa.feature.spectral_bandwidth(
                y=audio_data, sr=self.sample_rate, hop_length=self.hop_length
            )
            return float(np.mean(bandwidth))
        except Exception:
            return 0.0

    def _compute_spectral_flatness(self, audio_data: np.ndarray) -> float:
        """Compute spectral flatness (tonality measure)."""
        try:
            flatness = librosa.feature.spectral_flatness(
                y=audio_data, hop_length=self.hop_length
            )
            return float(np.mean(flatness))
        except Exception:
            return 0.0

    def _compute_zcr(self, audio_data: np.ndarray) -> float:
        """Compute zero-crossing rate."""
        try:
            zcr = librosa.feature.zero_crossing_rate(
                audio_data, frame_length=self.frame_length, hop_length=self.hop_length
            )
            return float(np.mean(zcr))
        except Exception:
            return 0.0

    def _compute_harmonic_ratio(self, audio_data: np.ndarray) -> float:
        """Compute harmonic ratio."""
        try:
            harmonic_ratios = []
            frames = librosa.util.frame(
                audio_data, frame_length=self.frame_length, hop_length=self.hop_length
            )

            for frame in frames.T:
                # Compute autocorrelation to find periodicity
                autocorr = np.correlate(frame, frame, mode="full")
                autocorr = autocorr[autocorr.size // 2 :]

                # Find peaks (excluding zero lag)
                peaks, _ = signal.find_peaks(autocorr[1:])
                if len(peaks) > 0:
                    harmonic_ratio = np.max(autocorr[peaks + 1]) / autocorr[0]
                    harmonic_ratios.append(harmonic_ratio)

            return float(np.mean(harmonic_ratios)) if harmonic_ratios else 0.0

        except Exception:
            return 0.0

    def _compute_jitter(self, audio_data: np.ndarray) -> float:
        """Compute jitter (pitch period variability)."""
        try:
            pitches = self._extract_pitch_contour(audio_data)
            if len(pitches) < 3:
                return 0.0

            # Jitter as relative period-to-period variation
            periods = [1 / p for p in pitches if p > 0]
            if len(periods) < 2:
                return 0.0

            period_diffs = np.abs(np.diff(periods))
            jitter = np.mean(period_diffs) / np.mean(periods)
            return float(jitter)

        except Exception:
            return 0.0

    def _compute_shimmer(self, audio_data: np.ndarray) -> float:
        """Compute shimmer (amplitude variability)."""
        try:
            # Use intensity contour for amplitude measures
            intensity = self._extract_intensity_contour(audio_data)
            if len(intensity) < 3:
                return 0.0

            amplitude_diffs = np.abs(np.diff(intensity))
            shimmer = np.mean(amplitude_diffs) / np.mean(intensity)
            return float(shimmer)

        except Exception:
            return 0.0

    def _compute_hnr(self, audio_data: np.ndarray) -> float:
        """Compute harmonics-to-noise ratio."""
        try:
            # Simplified HNR estimation using cepstral analysis
            cepstrum = np.fft.irfft(np.log(np.abs(np.fft.rfft(audio_data)) + 1e-10))

            # Assume quefrency bins for harmonics and noise
            harmonic_bins = cepstrum[10:50]  # Adjust based on sample rate
            noise_bins = cepstrum[100:200]

            if len(noise_bins) == 0 or np.mean(noise_bins) == 0:
                return 0.0

            hnr = 10 * np.log10(np.mean(harmonic_bins) / np.mean(noise_bins))
            return float(hnr)

        except Exception:
            return 0.0

    def _extract_formants(
        self, audio_data: np.ndarray, num_formants: int = 3
    ) -> List[float]:
        """Extract formant frequencies using LPC."""
        try:
            # Pre-emphasis
            pre_emphasized = np.append(
                audio_data[0], audio_data[1:] - 0.97 * audio_data[:-1]
            )

            # Compute LPC coefficients
            lpc_order = 8  # Typically 8-12 for speech
            a = librosa.lpc(pre_emphasized, order=lpc_order)

            # Find roots (formants)
            roots = np.roots(a)
            roots = roots[np.imag(roots) >= 0]  # Keep only upper half

            # Convert to frequencies
            angs = np.arctan2(np.imag(roots), np.real(roots))
            freqs = angs * (self.sample_rate / (2 * np.pi))

            # Get lowest frequencies (formants)
            freqs = sorted(freqs)
            formants = [f for f in freqs if 100 < f < 4000][:num_formants]

            # Pad if necessary
            while len(formants) < num_formants:
                formants.append(0.0)

            return formants

        except Exception:
            return [0.0] * num_formants

    def _classify_voice_quality(self, audio_data: np.ndarray) -> str:
        """Classify voice quality (breathy, pressed, modal)."""
        try:
            hnr = self._compute_hnr(audio_data)
            jitter = self._compute_jitter(audio_data)
            shimmer = self._compute_shimmer(audio_data)

            if hnr > 20 and jitter < 0.02:
                return "modal"  # Normal voice
            elif hnr < 15:
                return "breathy"  # Excessive noise
            elif jitter > 0.05 or shimmer > 0.1:
                return "pressed"  # Strained voice
            else:
                return "modal"

        except Exception:
            return "unknown"
