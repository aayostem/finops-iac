from pyflink.datastream import (
    StreamExecutionEnvironment,
    FlatMapFunction,
    RuntimeContext,
)
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
    MapStateDescriptor,
)
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from typing import Dict, List, Any, Optional, Iterator
import numpy as np
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


class StatefulVoiceFeatureAggregator(FlatMapFunction):
    """
    Stateful feature aggregator that maintains conversation context
    across multiple audio chunks for a single call.
    """

    def __init__(self, max_conversation_gap: int = 30000):  # 30 seconds
        self.max_conversation_gap = max_conversation_gap
        self.conversation_state = None
        self.feature_history_state = None
        self.speaker_profiles_state = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize state descriptors."""
        # Conversation state - tracks current conversation context
        conversation_descriptor = ValueStateDescriptor(
            "conversation_state",
            Types.PICKLED_BYTE_ARRAY(),  # Store complex Python objects
        )
        self.conversation_state = runtime_context.get_state(conversation_descriptor)

        # Feature history - maintains recent features for trend analysis
        history_descriptor = ListStateDescriptor(
            "feature_history", Types.PICKLED_BYTE_ARRAY()
        )
        self.feature_history_state = runtime_context.get_list_state(history_descriptor)

        # Speaker profiles - learns speaker characteristics over time
        speaker_descriptor = MapStateDescriptor(
            "speaker_profiles",
            Types.STRING(),  # speaker_id
            Types.PICKLED_BYTE_ARRAY(),  # speaker profile
        )
        self.speaker_profiles_state = runtime_context.get_map_state(speaker_descriptor)

    def flat_map(self, value: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """Process audio chunk with stateful context."""
        try:
            call_id = value["call_id"]
            speaker_id = value["speaker_id"]
            timestamp = datetime.fromisoformat(value["timestamp"])
            features = value.get("features", {})

            # Get or initialize conversation state
            conversation = self.conversation_state.value()
            if conversation is None:
                conversation = {
                    "call_id": call_id,
                    "start_time": timestamp,
                    "last_activity": timestamp,
                    "speaker_turns": [],
                    "conversation_phase": "initial",
                    "emotional_tone": "neutral",
                    "engagement_level": "medium",
                }

            # Update conversation state
            conversation = self._update_conversation_state(
                conversation, speaker_id, timestamp, features
            )
            self.conversation_state.update(conversation)

            # Update feature history
            self._update_feature_history(features, timestamp)

            # Update speaker profile
            self._update_speaker_profile(speaker_id, features, timestamp)

            # Generate enhanced features with context
            enhanced_features = self._generate_enhanced_features(features, conversation)

            yield enhanced_features

            # Check for conversation completion
            if self._is_conversation_completed(conversation, timestamp):
                # Generate conversation summary
                summary = self._generate_conversation_summary(conversation)
                yield summary

                # Clear state for this conversation
                self._clear_conversation_state()

        except Exception as e:
            logger.error(f"Stateful processing error: {e}")
            # Yield original features as fallback
            yield value

    def _update_conversation_state(
        self, conversation: Dict, speaker_id: str, timestamp: datetime, features: Dict
    ) -> Dict:
        """Update conversation state with new activity."""
        # Update timing
        conversation["last_activity"] = timestamp

        # Track speaker turns
        if (
            not conversation["speaker_turns"]
            or conversation["speaker_turns"][-1]["speaker"] != speaker_id
        ):
            conversation["speaker_turns"].append(
                {"speaker": speaker_id, "start_time": timestamp, "features": features}
            )

        # Limit speaker turns history
        if len(conversation["speaker_turns"]) > 50:
            conversation["speaker_turns"] = conversation["speaker_turns"][-50:]

        # Update conversation phase based on duration
        duration = (timestamp - conversation["start_time"]).total_seconds()
        if duration < 30:
            conversation["conversation_phase"] = "initial"
        elif duration < 180:
            conversation["conversation_phase"] = "middle"
        else:
            conversation["conversation_phase"] = "conclusion"

        # Update emotional tone based on features
        conversation["emotional_tone"] = self._detect_emotional_tone(features)

        # Update engagement level
        conversation["engagement_level"] = self._detect_engagement_level(
            features, conversation
        )

        return conversation

    def _update_feature_history(self, features: Dict, timestamp: datetime):
        """Update feature history for trend analysis."""
        history_item = {"timestamp": timestamp, "features": features}

        self.feature_history_state.add(history_item)

        # Limit history size
        history_list = list(self.feature_history_state.get())
        if len(history_list) > 100:  # Keep last 100 items
            self.feature_history_state.clear()
            for item in history_list[-100:]:
                self.feature_history_state.add(item)

    def _update_speaker_profile(
        self, speaker_id: str, features: Dict, timestamp: datetime
    ):
        """Update speaker profile with new features."""
        profile = self.speaker_profiles_state.get(speaker_id)

        if profile is None:
            profile = {
                "speaker_id": speaker_id,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "feature_stats": {},
                "conversation_count": 0,
                "total_duration": 0,
                "behavior_patterns": {},
            }

        # Update profile statistics
        profile["last_seen"] = timestamp
        profile["conversation_count"] += 1

        # Update feature statistics
        for feature_name, value in features.items():
            if isinstance(value, (int, float)):
                if feature_name not in profile["feature_stats"]:
                    profile["feature_stats"][feature_name] = {
                        "count": 0,
                        "mean": 0.0,
                        "m2": 0.0,  # For Welford's algorithm
                        "min": float("inf"),
                        "max": float("-inf"),
                    }

                stats = profile["feature_stats"][feature_name]
                stats["count"] += 1

                # Welford's algorithm for online variance
                delta = value - stats["mean"]
                stats["mean"] += delta / stats["count"]
                stats["m2"] += delta * (value - stats["mean"])

                # Update min/max
                stats["min"] = min(stats["min"], value)
                stats["max"] = max(stats["max"], value)

        self.speaker_profiles_state.put(speaker_id, profile)

    def _generate_enhanced_features(
        self, current_features: Dict, conversation: Dict
    ) -> Dict[str, Any]:
        """Generate features enhanced with conversation context."""
        enhanced_features = current_features.copy()

        # Add conversation context
        enhanced_features["conversation_phase"] = conversation["conversation_phase"]
        enhanced_features["emotional_tone"] = conversation["emotional_tone"]
        enhanced_features["engagement_level"] = conversation["engagement_level"]

        # Add temporal features
        enhanced_features["conversation_duration"] = (
            conversation["last_activity"] - conversation["start_time"]
        ).total_seconds()

        # Add speaker turn features
        turn_count = len(conversation["speaker_turns"])
        enhanced_features["speaker_turn_count"] = turn_count

        if turn_count > 1:
            current_speaker = conversation["speaker_turns"][-1]["speaker"]
            previous_speaker = conversation["speaker_turns"][-2]["speaker"]
            enhanced_features["speaker_switch"] = current_speaker != previous_speaker
        else:
            enhanced_features["speaker_switch"] = False

        # Add trend features from history
        trend_features = self._compute_trend_features()
        enhanced_features.update(trend_features)

        # Add derived features
        enhanced_features.update(
            self._compute_derived_features(current_features, conversation)
        )

        return enhanced_features

    def _detect_emotional_tone(self, features: Dict) -> str:
        """Detect emotional tone from voice features."""
        try:
            energy = features.get("energy_db", -50)
            pitch_var = features.get("pitch_variance", 0)
            speaking_rate = features.get("speaking_rate", 4.0)

            if energy > -30 and pitch_var > 20 and speaking_rate > 5.0:
                return "excited"
            elif energy < -40 and pitch_var < 5 and speaking_rate < 3.0:
                return "calm"
            elif pitch_var > 25 and energy > -35:
                return "stressed"
            else:
                return "neutral"

        except Exception:
            return "neutral"

    def _detect_engagement_level(self, features: Dict, conversation: Dict) -> str:
        """Detect engagement level from features and context."""
        try:
            silence_ratio = features.get("silence_ratio", 0.5)
            voice_confidence = features.get("voice_confidence", 0.5)
            response_latency = features.get("response_latency", 2.0)

            engagement_score = 0.0
            engagement_score += (1 - silence_ratio) * 0.4
            engagement_score += voice_confidence * 0.3
            engagement_score += max(0, 1 - response_latency / 5.0) * 0.3

            if engagement_score > 0.7:
                return "high"
            elif engagement_score > 0.4:
                return "medium"
            else:
                return "low"

        except Exception:
            return "medium"

    def _compute_trend_features(self) -> Dict[str, float]:
        """Compute trend features from historical data."""
        trend_features = {}

        try:
            history_list = list(self.feature_history_state.get())
            if len(history_list) < 5:
                return trend_features

            # Get recent features for trend analysis
            recent_items = history_list[-10:]  # Last 10 items

            numeric_features = [
                "energy_db",
                "avg_pitch",
                "speaking_rate",
                "voice_confidence",
            ]

            for feature in numeric_features:
                values = [
                    item["features"].get(feature)
                    for item in recent_items
                    if item["features"].get(feature) is not None
                ]

                if len(values) >= 3:
                    # Compute linear trend
                    x = np.arange(len(values))
                    slope, _ = np.polyfit(x, values, 1)

                    trend_features[f"{feature}_trend"] = float(slope)
                    trend_features[f"{feature}_volatility"] = float(np.std(values))

                    # Directional trend
                    if slope > 0.01:
                        trend_features[f"{feature}_direction"] = "increasing"
                    elif slope < -0.01:
                        trend_features[f"{feature}_direction"] = "decreasing"
                    else:
                        trend_features[f"{feature}_direction"] = "stable"

        except Exception as e:
            logger.warning(f"Trend computation failed: {e}")

        return trend_features

    def _compute_derived_features(
        self, current_features: Dict, conversation: Dict
    ) -> Dict[str, Any]:
        """Compute derived features from current features and context."""
        derived = {}

        # Conversational dynamics
        turn_count = len(conversation["speaker_turns"])
        if turn_count > 0:
            current_speaker_duration = sum(
                1
                for turn in conversation["speaker_turns"]
                if turn["speaker"] == conversation["speaker_turns"][-1]["speaker"]
            )
            derived["current_speaker_dominance"] = current_speaker_duration / turn_count

        # Emotional intensity
        energy = current_features.get("energy_db", -50)
        pitch_var = current_features.get("pitch_variance", 0)
        derived["emotional_intensity"] = (energy + 50) / 50 * 0.5 + min(
            pitch_var / 50, 1
        ) * 0.5

        # Engagement score
        silence_ratio = current_features.get("silence_ratio", 0.5)
        voice_confidence = current_features.get("voice_confidence", 0.5)
        derived["engagement_score"] = (1 - silence_ratio) * 0.6 + voice_confidence * 0.4

        return derived

    def _is_conversation_completed(
        self, conversation: Dict, timestamp: datetime
    ) -> bool:
        """Check if conversation has completed based on inactivity."""
        time_since_last_activity = (
            timestamp - conversation["last_activity"]
        ).total_seconds() * 1000
        return time_since_last_activity > self.max_conversation_gap

    def _generate_conversation_summary(self, conversation: Dict) -> Dict[str, Any]:
        """Generate summary when conversation completes."""
        duration = (
            conversation["last_activity"] - conversation["start_time"]
        ).total_seconds()

        summary = {
            "type": "conversation_summary",
            "call_id": conversation["call_id"],
            "start_time": conversation["start_time"].isoformat(),
            "end_time": conversation["last_activity"].isoformat(),
            "duration_seconds": duration,
            "speaker_turn_count": len(conversation["speaker_turns"]),
            "final_emotional_tone": conversation["emotional_tone"],
            "average_engagement": conversation["engagement_level"],
            "conversation_quality_score": self._compute_conversation_quality(
                conversation
            ),
            "key_insights": self._generate_conversation_insights(conversation),
        }

        return summary

    def _compute_conversation_quality(self, conversation: Dict) -> float:
        """Compute overall conversation quality score."""
        # Simple heuristic based on various factors
        score = 0.5  # Base score

        # Duration factor (optimal 2-10 minutes)
        duration = (
            conversation["last_activity"] - conversation["start_time"]
        ).total_seconds()
        if 120 <= duration <= 600:  # 2-10 minutes
            score += 0.2
        elif duration > 600:
            score -= 0.1

        # Turn count factor
        turn_count = len(conversation["speaker_turns"])
        if 10 <= turn_count <= 30:
            score += 0.2
        elif turn_count > 30:
            score += 0.1

        # Engagement factor
        if conversation["engagement_level"] == "high":
            score += 0.1

        return min(1.0, max(0.0, score))

    def _generate_conversation_insights(self, conversation: Dict) -> List[str]:
        """Generate key insights from conversation."""
        insights = []

        duration = (
            conversation["last_activity"] - conversation["start_time"]
        ).total_seconds()
        turn_count = len(conversation["speaker_turns"])

        if duration < 60:
            insights.append("Short conversation duration")
        elif duration > 600:
            insights.append("Extended conversation duration")

        if turn_count < 5:
            insights.append("Limited speaker interaction")
        elif turn_count > 40:
            insights.append("High level of speaker interaction")

        if conversation["emotional_tone"] == "excited":
            insights.append("Elevated emotional tone detected")
        elif conversation["emotional_tone"] == "stressed":
            insights.append("Stress indicators present")

        return insights

    def _clear_conversation_state(self):
        """Clear conversation state after completion."""
        self.conversation_state.clear()
        self.feature_history_state.clear()
        # Note: Speaker profiles are kept for future conversations
