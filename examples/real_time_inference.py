#!/usr/bin/env python3
"""
Example of using the feature store for real-time inference.
"""
import requests
import json
import time
from typing import Dict, List


class RealTimePredictor:
    """Example real-time predictor using voice features."""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.required_features = [
            "talk_to_listen_ratio",
            "interruption_count",
            "silence_ratio",
            "avg_pitch",
            "energy_db",
            "voice_confidence",
        ]

    def predict_customer_sentiment(self, call_id: str, speaker_id: str) -> Dict:
        """Predict customer sentiment using real-time features."""
        try:
            # Get latest features from online store
            response = requests.get(
                f"{self.api_base_url}/features/{call_id}/{speaker_id}",
                params={"feature_names": self.required_features},
            )

            if response.status_code != 200:
                return {
                    "error": "Failed to fetch features",
                    "status_code": response.status_code,
                }

            data = response.json()
            features = data["features"]

            # Simple rule-based sentiment analysis
            sentiment_score = self._compute_sentiment_score(features)
            sentiment_label = self._classify_sentiment(sentiment_score)

            return {
                "call_id": call_id,
                "speaker_id": speaker_id,
                "sentiment_score": sentiment_score,
                "sentiment_label": sentiment_label,
                "features_used": self.required_features,
                "timestamp": time.time(),
            }

        except Exception as e:
            return {"error": str(e)}

    def _compute_sentiment_score(self, features: Dict) -> float:
        """Compute sentiment score from voice features."""
        score = 0.5  # Neutral baseline

        # Positive indicators
        if features.get("talk_to_listen_ratio") is not None:
            # Balanced conversation is positive
            ratio = features["talk_to_listen_ratio"]
            if 0.3 <= ratio <= 0.7:
                score += 0.2
            elif ratio > 0.7:  # Too much talking
                score -= 0.1

        if features.get("interruption_count") is not None:
            # Few interruptions are positive
            interruptions = features["interruption_count"]
            if interruptions == 0:
                score += 0.1
            elif interruptions > 3:
                score -= 0.2

        if features.get("energy_db") is not None:
            # Moderate energy is positive
            energy = features["energy_db"]
            if -30 <= energy <= -20:
                score += 0.1
            elif energy < -40:  # Very low energy (disengaged)
                score -= 0.1

        if features.get("voice_confidence") is not None:
            # High voice confidence is positive
            confidence = features["voice_confidence"]
            score += confidence * 0.1

        return max(0.0, min(1.0, score))  # Clamp to [0, 1]

    def _classify_sentiment(self, score: float) -> str:
        """Classify sentiment based on score."""
        if score >= 0.7:
            return "positive"
        elif score >= 0.4:
            return "neutral"
        else:
            return "negative"

    def batch_predict(self, call_speaker_pairs: List[str]) -> Dict:
        """Perform batch prediction for multiple calls."""
        try:
            response = requests.get(
                f"{self.api_base_url}/features/batch",
                params={
                    "call_speaker_pairs": call_speaker_pairs,
                    "feature_names": self.required_features,
                },
            )

            if response.status_code != 200:
                return {"error": "Failed to fetch batch features"}

            data = response.json()
            results = {}

            for pair, features in data["results"].items():
                if features:  # Only predict if features are available
                    sentiment_score = self._compute_sentiment_score(features)
                    sentiment_label = self._classify_sentiment(sentiment_score)
                    results[pair] = {
                        "sentiment_score": sentiment_score,
                        "sentiment_label": sentiment_label,
                    }
                else:
                    results[pair] = {"error": "No features available"}

            return {"predictions": results}

        except Exception as e:
            return {"error": str(e)}


def main():
    """Example usage of the real-time predictor."""
    predictor = RealTimePredictor()

    # Single prediction
    result = predictor.predict_customer_sentiment("test_call_123", "customer")
    print("Single Prediction:")
    print(json.dumps(result, indent=2))

    # Batch prediction
    pairs = ["call_1:customer", "call_2:customer", "call_3:agent"]
    batch_result = predictor.batch_predict(pairs)
    print("\nBatch Prediction:")
    print(json.dumps(batch_result, indent=2))


if __name__ == "__main__":
    main()
