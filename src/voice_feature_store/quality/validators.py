import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of feature validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    summary: Dict


class FeatureValidator:
    """Validate feature quality and data integrity."""

    def __init__(self):
        self.feature_rules = self._get_validation_rules()

    def _get_validation_rules(self) -> Dict:
        """Get validation rules for each feature."""
        return {
            "talk_to_listen_ratio": {
                "min_value": 0.0,
                "max_value": 10.0,
                "not_null": True,
            },
            "interruption_count": {"min_value": 0, "max_value": 100, "not_null": True},
            "silence_ratio": {"min_value": 0.0, "max_value": 1.0, "not_null": True},
            "avg_pitch": {"min_value": 50.0, "max_value": 400.0, "not_null": False},
            "energy_db": {"min_value": -100.0, "max_value": 0.0, "not_null": True},
            "voice_confidence": {"min_value": 0.0, "max_value": 1.0, "not_null": True},
        }

    def validate_feature_set(self, features: Dict) -> ValidationResult:
        """Validate a set of features."""
        errors = []
        warnings = []

        for feature_name, value in features.items():
            if feature_name in self.feature_rules:
                rule = self.feature_rules[feature_name]

                # Check null values
                if rule.get("not_null", False) and value is None:
                    errors.append(f"Feature {feature_name} cannot be null")
                    continue

                if value is None:
                    continue

                # Check value ranges
                if "min_value" in rule and value < rule["min_value"]:
                    errors.append(
                        f"Feature {feature_name} value {value} below minimum {rule['min_value']}"
                    )

                if "max_value" in rule and value > rule["max_value"]:
                    errors.append(
                        f"Feature {feature_name} value {value} above maximum {rule['max_value']}"
                    )

        # Check for missing critical features
        critical_features = [
            f for f, r in self.feature_rules.items() if r.get("not_null", False)
        ]
        missing_critical = [
            f for f in critical_features if f not in features or features[f] is None
        ]

        if missing_critical:
            warnings.append(f"Missing critical features: {missing_critical}")

        is_valid = len(errors) == 0

        summary = {
            "total_features": len(features),
            "validated_features": len([f for f in features if f in self.feature_rules]),
            "error_count": len(errors),
            "warning_count": len(warnings),
            "completeness_score": self._calculate_completeness(features),
        }

        return ValidationResult(
            is_valid=is_valid, errors=errors, warnings=warnings, summary=summary
        )

    def _calculate_completeness(self, features: Dict) -> float:
        """Calculate feature completeness score."""
        expected_features = list(self.feature_rules.keys())
        present_features = [
            f for f in expected_features if f in features and features[f] is not None
        ]

        return (
            len(present_features) / len(expected_features) if expected_features else 1.0
        )

    def validate_temporal_consistency(
        self, feature_history: List[Dict]
    ) -> ValidationResult:
        """Validate temporal consistency of feature sequences."""
        errors = []
        warnings = []

        if len(feature_history) < 2:
            return ValidationResult(
                True, errors, warnings, {"sequence_length": len(feature_history)}
            )

        # Convert to DataFrame for analysis
        df = pd.DataFrame(feature_history)

        # Check for abrupt changes
        for feature in self.feature_rules.keys():
            if feature in df.columns:
                values = df[feature].dropna()
                if len(values) > 1:
                    # Calculate rate of change
                    changes = np.diff(values)
                    mean_change = np.mean(np.abs(changes))
                    std_change = np.std(changes)

                    # Flag outliers (changes > 3 standard deviations)
                    outliers = np.abs(changes) > (mean_change + 3 * std_change)
                    outlier_count = np.sum(outliers)

                    if outlier_count > 0:
                        warnings.append(
                            f"Feature {feature} has {outlier_count} abrupt changes"
                        )

        summary = {
            "sequence_length": len(feature_history),
            "features_analyzed": len(
                [f for f in self.feature_rules.keys() if f in df.columns]
            ),
            "temporal_consistency_score": self._calculate_temporal_consistency(df),
        }

        return ValidationResult(True, errors, warnings, summary)

    def _calculate_temporal_consistency(self, df: pd.DataFrame) -> float:
        """Calculate temporal consistency score."""
        consistency_scores = []

        for feature in self.feature_rules.keys():
            if feature in df.columns:
                values = df[feature].dropna()
                if len(values) > 1:
                    # Use coefficient of variation as inverse consistency measure
                    cv = np.std(values) / (np.mean(values) + 1e-10)
                    consistency = 1.0 / (1.0 + cv)  # Convert to [0,1] scale
                    consistency_scores.append(consistency)

        return np.mean(consistency_scores) if consistency_scores else 1.0


class DataDriftDetector:
    """Detect data drift in feature distributions."""

    def __init__(self, reference_data: pd.DataFrame):
        self.reference_data = reference_data
        self.reference_stats = self._compute_reference_stats()

    def _compute_reference_stats(self) -> Dict:
        """Compute reference statistics for features."""
        stats = {}
        for column in self.reference_data.columns:
            if pd.api.types.is_numeric_dtype(self.reference_data[column]):
                stats[column] = {
                    "mean": self.reference_data[column].mean(),
                    "std": self.reference_data[column].std(),
                    "min": self.reference_data[column].min(),
                    "max": self.reference_data[column].max(),
                }
        return stats

    def detect_drift(self, current_data: pd.DataFrame, threshold: float = 0.1) -> Dict:
        """Detect data drift between reference and current data."""
        drift_report = {}

        for feature, ref_stats in self.reference_stats.items():
            if feature in current_data.columns:
                current_values = current_data[feature].dropna()

                if len(current_values) > 0:
                    # Calculate drift metrics
                    current_mean = current_values.mean()
                    current_std = current_values.std()

                    # Mean drift (standardized)
                    mean_drift = abs(current_mean - ref_stats["mean"]) / (
                        ref_stats["std"] + 1e-10
                    )

                    # Distribution similarity (simplified)
                    distribution_similarity = self._calculate_distribution_similarity(
                        current_values, ref_stats
                    )

                    drift_detected = (
                        mean_drift > threshold or distribution_similarity < 0.9
                    )

                    drift_report[feature] = {
                        "drift_detected": drift_detected,
                        "mean_drift": mean_drift,
                        "distribution_similarity": distribution_similarity,
                        "reference_mean": ref_stats["mean"],
                        "current_mean": current_mean,
                        "threshold": threshold,
                    }

        return drift_report

    def _calculate_distribution_similarity(
        self, current_values: pd.Series, ref_stats: Dict
    ) -> float:
        """Calculate distribution similarity score."""
        # Simple implementation using empirical rule
        within_1_std = np.sum(
            (current_values >= ref_stats["mean"] - ref_stats["std"])
            & (current_values <= ref_stats["mean"] + ref_stats["std"])
        )

        within_2_std = np.sum(
            (current_values >= ref_stats["mean"] - 2 * ref_stats["std"])
            & (current_values <= ref_stats["mean"] + 2 * ref_stats["std"])
        )

        # Calculate what percentage falls within expected ranges
        pct_within_1_std = within_1_std / len(current_values)
        pct_within_2_std = within_2_std / len(current_values)

        # Combined similarity score
        similarity = 0.6 * pct_within_1_std + 0.4 * pct_within_2_std
        return similarity
