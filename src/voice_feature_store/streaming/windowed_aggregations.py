from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import (
    TumblingProcessingTimeWindows,
    SlidingProcessingTimeWindows,
)
from pyflink.datastream.window import ProcessingTimeSessionWindows
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.functions import RuntimeContext, WindowFunction
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from typing import Dict, List, Any, Tuple
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class VoiceFeatureAggregator(AggregateFunction):
    """Aggregate voice features over time windows."""

    def create_accumulator(self) -> Dict[str, Any]:
        """Create initial accumulator for aggregation."""
        return {
            "call_id": None,
            "speaker_id": None,
            "features": {},
            "count": 0,
            "timestamps": [],
            "start_time": None,
            "end_time": None,
        }

    def add(self, value: Dict[str, Any], accumulator: Dict[str, Any]) -> Dict[str, Any]:
        """Add a value to the accumulator."""
        if accumulator["call_id"] is None:
            accumulator["call_id"] = value["call_id"]
            accumulator["speaker_id"] = value["speaker_id"]
            accumulator["start_time"] = value["timestamp"]

        accumulator["end_time"] = value["timestamp"]
        accumulator["count"] += 1
        accumulator["timestamps"].append(value["timestamp"])

        # Aggregate features
        for feature_name, feature_value in value.items():
            if feature_name not in ["call_id", "speaker_id", "timestamp"]:
                if feature_name not in accumulator["features"]:
                    accumulator["features"][feature_name] = {
                        "values": [],
                        "type": self._infer_feature_type(feature_value),
                    }

                if feature_value is not None:
                    accumulator["features"][feature_name]["values"].append(
                        feature_value
                    )

        return accumulator

    def get_result(self, accumulator: Dict[str, Any]) -> Dict[str, Any]:
        """Get the aggregation result."""
        if accumulator["count"] == 0:
            return {}

        result = {
            "call_id": accumulator["call_id"],
            "speaker_id": accumulator["speaker_id"],
            "window_start": accumulator["start_time"],
            "window_end": accumulator["end_time"],
            "sample_count": accumulator["count"],
            "aggregated_features": {},
        }

        # Compute aggregated values for each feature
        for feature_name, feature_data in accumulator["features"].items():
            values = feature_data["values"]
            if not values:
                continue

            if feature_data["type"] == "numeric":
                result["aggregated_features"][f"{feature_name}_mean"] = np.mean(values)
                result["aggregated_features"][f"{feature_name}_std"] = np.std(values)
                result["aggregated_features"][f"{feature_name}_min"] = np.min(values)
                result["aggregated_features"][f"{feature_name}_max"] = np.max(values)
                result["aggregated_features"][f"{feature_name}_median"] = np.median(
                    values
                )

            elif feature_data["type"] == "categorical":
                # For categorical, use mode
                unique, counts = np.unique(values, return_counts=True)
                result["aggregated_features"][f"{feature_name}_mode"] = unique[
                    np.argmax(counts)
                ]
                result["aggregated_features"][f"{feature_name}_unique_count"] = len(
                    unique
                )

        return result

    def merge(self, acc1: Dict[str, Any], acc2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two accumulators."""
        if acc1["call_id"] is None:
            return acc2
        if acc2["call_id"] is None:
            return acc1

        # Merge features
        for feature_name, feature_data in acc2["features"].items():
            if feature_name in acc1["features"]:
                acc1["features"][feature_name]["values"].extend(feature_data["values"])
            else:
                acc1["features"][feature_name] = feature_data

        # Update metadata
        acc1["count"] += acc2["count"]
        acc1["timestamps"].extend(acc2["timestamps"])
        acc1["end_time"] = max(acc1["end_time"], acc2["end_time"])
        acc1["start_time"] = min(acc1["start_time"], acc2["start_time"])

        return acc1

    def _infer_feature_type(self, value: Any) -> str:
        """Infer the type of a feature value."""
        if isinstance(value, (int, float)):
            return "numeric"
        elif isinstance(value, (str, bool)):
            return "categorical"
        else:
            return "numeric"  # Default to numeric


class ConversationalPatternDetector(ProcessWindowFunction):
    """Detect conversational patterns in voice features."""

    def __init__(self):
        self.pattern_thresholds = {
            "high_interruption": 5,  # interruptions per minute
            "monologue": 0.8,  # talk-to-listen ratio threshold
            "low_engagement": 0.4,  # silence ratio threshold
            "emotional_escalation": 0.15,  # pitch variance threshold
        }

    def process(
        self, key: Tuple, context: "ProcessWindowFunction.Context", elements: List[Dict]
    ) -> Iterable[Dict]:
        """Process window of features to detect patterns."""
        if not elements:
            return

        features_list = list(elements)

        # Extract basic conversation metrics
        call_id = features_list[0]["call_id"]
        speaker_id = features_list[0]["speaker_id"]

        patterns = self._analyze_conversation_patterns(features_list)

        if patterns:
            yield {
                "call_id": call_id,
                "speaker_id": speaker_id,
                "timestamp": datetime.utcnow().isoformat(),
                "window_start": context.window().start,
                "window_end": context.window().end,
                "detected_patterns": patterns,
                "pattern_confidence": self._calculate_pattern_confidence(patterns),
                "conversation_metrics": self._compute_conversation_metrics(
                    features_list
                ),
            }

    def _analyze_conversation_patterns(self, features_list: List[Dict]) -> List[str]:
        """Analyze features to detect conversation patterns."""
        patterns = []

        # Calculate aggregated metrics
        interruptions = [f.get("interruption_count", 0) for f in features_list]
        talk_ratios = [f.get("talk_to_listen_ratio", 0) for f in features_list]
        silence_ratios = [f.get("silence_ratio", 0) for f in features_list]
        pitch_variance = [f.get("pitch_variance", 0) for f in features_list]

        avg_interruptions = np.mean(interruptions) if interruptions else 0
        avg_talk_ratio = np.mean(talk_ratios) if talk_ratios else 0
        avg_silence_ratio = np.mean(silence_ratios) if silence_ratios else 0
        avg_pitch_variance = np.mean(pitch_variance) if pitch_variance else 0

        # Detect patterns
        if avg_interruptions > self.pattern_thresholds["high_interruption"]:
            patterns.append("high_interruption")

        if avg_talk_ratio > self.pattern_thresholds["monologue"]:
            patterns.append("monologue")

        if avg_silence_ratio > self.pattern_thresholds["low_engagement"]:
            patterns.append("low_engagement")

        if avg_pitch_variance > self.pattern_thresholds["emotional_escalation"]:
            patterns.append("emotional_escalation")

        return patterns

    def _calculate_pattern_confidence(self, patterns: List[str]) -> Dict[str, float]:
        """Calculate confidence scores for detected patterns."""
        # In a real implementation, this would use more sophisticated scoring
        return {pattern: 0.85 for pattern in patterns}

    def _compute_conversation_metrics(
        self, features_list: List[Dict]
    ) -> Dict[str, float]:
        """Compute overall conversation metrics."""
        metrics = {}

        numeric_features = [
            "talk_to_listen_ratio",
            "interruption_count",
            "silence_ratio",
            "avg_pitch",
            "energy_db",
            "voice_confidence",
        ]

        for feature in numeric_features:
            values = [
                f.get(feature) for f in features_list if f.get(feature) is not None
            ]
            if values:
                metrics[f"{feature}_mean"] = np.mean(values)
                metrics[f"{feature}_std"] = np.std(values)
                metrics[f"{feature}_trend"] = self._calculate_trend(values)

        return metrics

    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate the trend of values over time."""
        if len(values) < 2:
            return 0.0

        x = np.arange(len(values))
        slope, _ = np.polyfit(x, values, 1)
        return slope


class RealTimeAnomalyDetector(ProcessWindowFunction):
    """Detect anomalies in real-time voice features."""

    def __init__(self):
        self.baseline_stats = {}
        self.anomaly_threshold = 3.0  # Number of standard deviations

    def process(
        self, key: Tuple, context: "ProcessWindowFunction.Context", elements: List[Dict]
    ) -> Iterable[Dict]:
        """Process window to detect anomalies."""
        if not elements:
            return

        call_speaker_key = key[0]  # (call_id, speaker_id)
        features_list = list(elements)

        anomalies = self._detect_anomalies(call_speaker_key, features_list)

        if anomalies:
            yield {
                "call_id": features_list[0]["call_id"],
                "speaker_id": features_list[0]["speaker_id"],
                "timestamp": datetime.utcnow().isoformat(),
                "detected_anomalies": anomalies,
                "anomaly_score": self._calculate_anomaly_score(anomalies),
                "window_start": context.window().start,
                "window_end": context.window().end,
            }

    def _detect_anomalies(self, key: str, features_list: List[Dict]) -> List[Dict]:
        """Detect anomalies in feature values."""
        anomalies = []

        # Update baseline statistics
        self._update_baseline_stats(key, features_list)

        # Check latest feature values against baseline
        latest_features = features_list[-1]

        for feature_name, value in latest_features.items():
            if feature_name in ["call_id", "speaker_id", "timestamp"]:
                continue

            if value is None:
                continue

            baseline = self.baseline_stats[key].get(feature_name, {})
            if not baseline:
                continue

            mean = baseline.get("mean", 0)
            std = baseline.get("std", 1)

            if std > 0:  # Avoid division by zero
                z_score = abs(value - mean) / std

                if z_score > self.anomaly_threshold:
                    anomalies.append(
                        {
                            "feature": feature_name,
                            "value": value,
                            "expected_range": (mean - 2 * std, mean + 2 * std),
                            "z_score": z_score,
                            "severity": "high" if z_score > 5 else "medium",
                        }
                    )

        return anomalies

    def _update_baseline_stats(self, key: str, features_list: List[Dict]):
        """Update baseline statistics for anomaly detection."""
        if key not in self.baseline_stats:
            self.baseline_stats[key] = {}

        # Collect all feature values
        feature_values = {}
        for features in features_list:
            for feature_name, value in features.items():
                if feature_name in ["call_id", "speaker_id", "timestamp"]:
                    continue

                if value is not None:
                    if feature_name not in feature_values:
                        feature_values[feature_name] = []
                    feature_values[feature_name].append(value)

        # Update statistics
        for feature_name, values in feature_values.items():
            if len(values) >= 5:  # Minimum samples for meaningful stats
                self.baseline_stats[key][feature_name] = {
                    "mean": np.mean(values),
                    "std": np.std(values),
                    "count": len(values),
                    "last_updated": datetime.utcnow(),
                }

    def _calculate_anomaly_score(self, anomalies: List[Dict]) -> float:
        """Calculate overall anomaly score."""
        if not anomalies:
            return 0.0

        scores = [anomaly["z_score"] for anomaly in anomalies]
        return min(1.0, np.mean(scores) / 10.0)  # Normalize to [0, 1]


def create_advanced_streaming_topology(env: StreamExecutionEnvironment):
    """Create advanced streaming topology with windowed aggregations."""

    # Assuming we have a features stream from the main processor
    features_stream = env.from_collection(
        []
    )  # This would be connected to actual source

    # 1-minute tumbling windows for basic aggregations
    aggregated_features = (
        features_stream.key_by(lambda x: (x["call_id"], x["speaker_id"]))
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .aggregate(VoiceFeatureAggregator())
        .name("feature_aggregation_1min")
    )

    # 5-minute sliding windows for conversation patterns
    conversation_patterns = (
        features_stream.key_by(lambda x: (x["call_id"], x["speaker_id"]))
        .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
        .process(ConversationalPatternDetector())
        .name("conversation_pattern_detection")
    )

    # Real-time anomaly detection with session windows
    anomalies = (
        features_stream.key_by(lambda x: (x["call_id"], x["speaker_id"]))
        .window(ProcessingTimeSessionWindows.with_gap(Time.seconds(30)))
        .process(RealTimeAnomalyDetector())
        .name("real_time_anomaly_detection")
    )

    return {
        "aggregated_features": aggregated_features,
        "conversation_patterns": conversation_patterns,
        "anomalies": anomalies,
    }
