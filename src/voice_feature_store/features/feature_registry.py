from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class FeatureDefinition:
    """Metadata definition for a feature."""

    name: str
    description: str
    data_type: str
    source: str
    owner: str
    created_date: datetime
    version: str
    tags: List[str]
    validation_rules: Optional[Dict] = None


class FeatureRegistry:
    """Central registry for feature metadata and lineage."""

    def __init__(self):
        self.features: Dict[str, FeatureDefinition] = {}
        self._initialize_core_features()

    def _initialize_core_features(self):
        """Initialize core voice analytics features."""
        core_features = [
            FeatureDefinition(
                name="talk_to_listen_ratio",
                description="Ratio of speaking time to listening time over a 30-second window",
                data_type="float",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["temporal", "behavioral", "real-time"],
                validation_rules={"min_value": 0.0, "max_value": 10.0},
            ),
            FeatureDefinition(
                name="interruption_count",
                description="Number of interruptions detected in a 1-minute window",
                data_type="int",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["temporal", "behavioral", "real-time"],
                validation_rules={"min_value": 0},
            ),
            FeatureDefinition(
                name="silence_ratio",
                description="Percentage of silence in the audio stream over a 1-minute window",
                data_type="float",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["acoustic", "behavioral", "real-time"],
                validation_rules={"min_value": 0.0, "max_value": 1.0},
            ),
            FeatureDefinition(
                name="avg_pitch",
                description="Average fundamental frequency (pitch) of the voice",
                data_type="float",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["acoustic", "real-time"],
                validation_rules={"min_value": 50.0, "max_value": 400.0},
            ),
            FeatureDefinition(
                name="energy_db",
                description="RMS energy of the audio signal in decibels",
                data_type="float",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["acoustic", "real-time"],
                validation_rules={"min_value": -100.0, "max_value": 0.0},
            ),
            FeatureDefinition(
                name="mfccs",
                description="Mel-frequency cepstral coefficients (first 13 coefficients)",
                data_type="list[float]",
                source="voice_feature_processor",
                owner="voice_analytics_team",
                created_date=datetime.utcnow(),
                version="1.0",
                tags=["acoustic", "real-time", "ml-ready"],
            ),
        ]

        for feature in core_features:
            self.register_feature(feature)

    def register_feature(self, feature: FeatureDefinition):
        """Register a new feature in the registry."""
        self.features[feature.name] = feature

    def get_feature(self, name: str) -> Optional[FeatureDefinition]:
        """Get feature definition by name."""
        return self.features.get(name)

    def get_all_features(self) -> List[Dict]:
        """Get all registered features as dictionaries."""
        return [
            {
                "name": feat.name,
                "description": feat.description,
                "data_type": feat.data_type,
                "source": feat.source,
                "owner": feat.owner,
                "created_date": feat.created_date.isoformat(),
                "version": feat.version,
                "tags": feat.tags,
                "validation_rules": feat.validation_rules,
            }
            for feat in self.features.values()
        ]

    def search_features(
        self, tag: Optional[str] = None, owner: Optional[str] = None
    ) -> List[Dict]:
        """Search features by tag or owner."""
        results = []
        for feature in self.features.values():
            if tag and tag not in feature.tags:
                continue
            if owner and feature.owner != owner:
                continue
            results.append(
                {
                    "name": feature.name,
                    "description": feature.description,
                    "data_type": feature.data_type,
                    "tags": feature.tags,
                }
            )
        return results
