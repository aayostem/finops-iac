import numpy as np
import torch
import torch.nn as nn
import torchaudio
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
from transformers import Wav2Vec2Processor, Wav2Vec2Model
import warnings

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


@dataclass
class DeepVoiceFeatures:
    """Deep learning based voice features."""

    embeddings: np.ndarray  # Wav2Vec2 embeddings
    attention_weights: np.ndarray  # Self-attention patterns
    contextual_features: np.ndarray  # Contextual representations
    emotion_embedding: np.ndarray  # Emotion-specific features
    speaker_characteristics: np.ndarray  # Speaker identity features


class Wav2Vec2FeatureExtractor:
    """Wav2Vec2 based feature extraction for advanced voice analytics."""

    def __init__(self, model_name: str = "facebook/wav2vec2-base-960h"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.processor = Wav2Vec2Processor.from_pretrained(model_name)
        self.model = Wav2Vec2Model.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()

        # Feature dimensions
        self.embedding_dim = 768  # for wav2vec2-base
        self.num_attention_heads = 12

    def extract_features(
        self, audio_data: np.ndarray, sample_rate: int = 16000
    ) -> DeepVoiceFeatures:
        """Extract deep learning based features from audio."""
        try:
            # Preprocess audio
            inputs = self.processor(
                audio_data, sampling_rate=sample_rate, return_tensors="pt", padding=True
            )

            # Move to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            # Extract features with attention
            with torch.no_grad():
                outputs = self.model(
                    **inputs, output_attentions=True, output_hidden_states=True
                )

            # Get embeddings (last hidden state)
            embeddings = outputs.last_hidden_state.mean(dim=1).cpu().numpy()[0]

            # Get attention weights (average across layers and heads)
            attention_weights = (
                torch.stack(outputs.attentions).mean(dim=(0, 2)).cpu().numpy()
            )
            attention_weights = attention_weights.mean(axis=0)  # Average over sequence

            # Get contextual features from intermediate layers
            hidden_states = torch.stack(outputs.hidden_states)
            contextual_features = (
                hidden_states.mean(dim=(0, 2)).cpu().numpy()[0]
            )  # Average over layers and sequence

            # Extract emotion and speaker characteristics
            emotion_embedding = self._extract_emotion_features(embeddings)
            speaker_characteristics = self._extract_speaker_characteristics(embeddings)

            return DeepVoiceFeatures(
                embeddings=embeddings,
                attention_weights=attention_weights,
                contextual_features=contextual_features,
                emotion_embedding=emotion_embedding,
                speaker_characteristics=speaker_characteristics,
            )

        except Exception as e:
            logger.error(f"Deep feature extraction failed: {e}")
            # Return zero features as fallback
            return self._get_zero_features()

    def _extract_emotion_features(self, embeddings: np.ndarray) -> np.ndarray:
        """Extract emotion-related features from embeddings."""
        # Simple PCA-based emotion projection (in production, use trained emotion model)
        emotion_components = embeddings[:128]  # Use first 128 dimensions for emotion
        return emotion_components

    def _extract_speaker_characteristics(self, embeddings: np.ndarray) -> np.ndarray:
        """Extract speaker characteristic features."""
        # Use specific dimensions for speaker characteristics
        speaker_components = embeddings[128:256]  # Next 128 dimensions
        return speaker_components

    def _get_zero_features(self) -> DeepVoiceFeatures:
        """Return zero features as fallback."""
        return DeepVoiceFeatures(
            embeddings=np.zeros(self.embedding_dim),
            attention_weights=np.zeros(50),  # Approximate attention dimension
            contextual_features=np.zeros(self.embedding_dim),
            emotion_embedding=np.zeros(128),
            speaker_characteristics=np.zeros(128),
        )


class CNNVoiceFeatureExtractor(nn.Module):
    """CNN-based feature extractor for raw audio waveforms."""

    def __init__(self, input_length: int = 16000):
        super().__init__()
        self.input_length = input_length

        # CNN architecture for raw audio
        self.conv_layers = nn.Sequential(
            # First conv block
            nn.Conv1d(1, 64, kernel_size=80, stride=4, padding=38),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.MaxPool1d(4),
            # Second conv block
            nn.Conv1d(64, 128, kernel_size=3, stride=1, padding=1),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.MaxPool1d(4),
            # Third conv block
            nn.Conv1d(128, 256, kernel_size=3, stride=1, padding=1),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.MaxPool1d(4),
            # Fourth conv block
            nn.Conv1d(256, 512, kernel_size=3, stride=1, padding=1),
            nn.BatchNorm1d(512),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1),
        )

        # Feature projection
        self.feature_projection = nn.Linear(512, 256)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass."""
        # x shape: (batch_size, 1, audio_length)
        x = self.conv_layers(x)
        x = x.squeeze(-1)  # Remove last dimension
        features = self.feature_projection(x)
        return features

    def extract_features(self, audio_data: np.ndarray) -> np.ndarray:
        """Extract CNN features from audio."""
        try:
            # Convert to tensor
            audio_tensor = torch.FloatTensor(audio_data).unsqueeze(0).unsqueeze(0)

            # Ensure correct length
            if audio_tensor.shape[2] > self.input_length:
                audio_tensor = audio_tensor[:, :, : self.input_length]
            elif audio_tensor.shape[2] < self.input_length:
                padding = self.input_length - audio_tensor.shape[2]
                audio_tensor = torch.nn.functional.pad(audio_tensor, (0, padding))

            # Extract features
            with torch.no_grad():
                features = self.forward(audio_tensor)

            return features.squeeze().numpy()

        except Exception as e:
            logger.error(f"CNN feature extraction failed: {e}")
            return np.zeros(256)


class DeepVoiceFeatureExtractor:
    """Orchestrator for deep learning based voice feature extraction."""

    def __init__(self):
        self.wav2vec_extractor = Wav2Vec2FeatureExtractor()
        self.cnn_extractor = CNNVoiceFeatureExtractor()

    def extract_comprehensive_features(
        self, audio_data: np.ndarray, sample_rate: int = 16000
    ) -> Dict[str, np.ndarray]:
        """Extract comprehensive deep learning features."""
        features = {}

        try:
            # Wav2Vec2 features
            wav2vec_features = self.wav2vec_extractor.extract_features(
                audio_data, sample_rate
            )
            features.update(
                {
                    "wav2vec_embeddings": wav2vec_features.embeddings,
                    "attention_patterns": wav2vec_features.attention_weights,
                    "contextual_features": wav2vec_features.contextual_features,
                    "emotion_embedding": wav2vec_features.emotion_embedding,
                    "speaker_characteristics": wav2vec_features.speaker_characteristics,
                }
            )

            # CNN features
            cnn_features = self.cnn_extractor.extract_features(audio_data)
            features["cnn_audio_features"] = cnn_features

            # Combined features
            combined_features = np.concatenate(
                [
                    wav2vec_features.embeddings,
                    cnn_features,
                    wav2vec_features.emotion_embedding,
                ]
            )
            features["combined_deep_features"] = combined_features

            # Feature statistics
            features["deep_feature_stats"] = self._compute_feature_statistics(features)

        except Exception as e:
            logger.error(f"Comprehensive deep feature extraction failed: {e}")
            features = self._get_fallback_features()

        return features

    def _compute_feature_statistics(
        self, features: Dict[str, np.ndarray]
    ) -> Dict[str, float]:
        """Compute statistics for deep features."""
        stats = {}

        for feature_name, feature_array in features.items():
            if isinstance(feature_array, np.ndarray):
                stats[f"{feature_name}_mean"] = float(np.mean(feature_array))
                stats[f"{feature_name}_std"] = float(np.std(feature_array))
                stats[f"{feature_name}_min"] = float(np.min(feature_array))
                stats[f"{feature_name}_max"] = float(np.max(feature_array))

        return stats

    def _get_fallback_features(self) -> Dict[str, np.ndarray]:
        """Get fallback features when extraction fails."""
        return {
            "wav2vec_embeddings": np.zeros(768),
            "attention_patterns": np.zeros(50),
            "contextual_features": np.zeros(768),
            "emotion_embedding": np.zeros(128),
            "speaker_characteristics": np.zeros(128),
            "cnn_audio_features": np.zeros(256),
            "combined_deep_features": np.zeros(1152),  # 768 + 256 + 128
            "deep_feature_stats": {
                "wav2vec_embeddings_mean": 0.0,
                "wav2vec_embeddings_std": 0.0,
                "combined_deep_features_mean": 0.0,
                "combined_deep_features_std": 0.0,
            },
        }
