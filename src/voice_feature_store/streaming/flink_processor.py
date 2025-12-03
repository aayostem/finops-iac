from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, TimeCharacteristic
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.window import ProcessingTimeSessionWindows
import json
import logging
import numpy as np
from datetime import datetime
from typing import Dict, Any

from ..features.voice_features import VoiceFeatures, AudioChunk, VoiceFeatureProcessor
from ..storage.online_store import OnlineFeatureStore
from ..storage.offline_store import OfflineFeatureStore
from config.settings import settings


logger = logging.getLogger(__name__)


class AudioChunkSchema(SimpleStringSchema):
    """Schema for deserializing audio chunks from Kafka."""

    def deserialize(self, message: bytes) -> Dict[str, Any]:
        data = super().deserialize(message)
        return json.loads(data) if isinstance(data, str) else data


class VoiceFeatureMapFunction(MapFunction):
    """Flink MapFunction to process audio chunks and compute features."""

    def __init__(self):
        self.feature_processor = None
        self.online_store = None
        self.offline_store = None
        self.feature_batch = []
        self.batch_size = 100  # Batch size for offline storage

    def open(self, runtime_context: RuntimeContext):
        """Initialize resources when the function starts."""
        self.feature_processor = VoiceFeatureProcessor()
        self.online_store = OnlineFeatureStore()
        self.offline_store = OfflineFeatureStore()
        logger.info("VoiceFeatureMapFunction initialized")

    def map(self, value: Dict[str, Any]) -> Dict[str, Any]:
        """Process audio chunk and return features."""
        try:
            # Parse audio chunk from Kafka message
            audio_chunk = self._parse_audio_chunk(value)
            if not audio_chunk:
                return {}

            # Compute features
            features = self.feature_processor.compute_features(audio_chunk)

            # Store in online store (immediately for real-time access)
            self.online_store.store_features(features)

            # Batch for offline store
            self.feature_batch.append(features)
            if len(self.feature_batch) >= self.batch_size:
                self.offline_store.store_features_batch(self.feature_batch)
                self.feature_batch = []

            # Return features for Kafka output
            return features.to_dict()

        except Exception as e:
            logger.error(f"Error processing audio chunk: {e}")
            return {}

    def _parse_audio_chunk(self, data: Dict[str, Any]) -> Optional[AudioChunk]:
        """Parse Kafka message into AudioChunk object."""
        try:
            # Convert base64 audio data back to numpy array
            audio_data = np.frombuffer(
                (
                    data["audio_data"].encode("latin-1")
                    if isinstance(data["audio_data"], str)
                    else data["audio_data"]
                ),
                dtype=np.float32,
            )

            return AudioChunk(
                call_id=data["call_id"],
                speaker_id=data["speaker_id"],
                timestamp=datetime.fromisoformat(data["timestamp"]),
                audio_data=audio_data,
                sample_rate=data["sample_rate"],
                chunk_duration=data["chunk_duration"],
            )
        except Exception as e:
            logger.error(f"Failed to parse audio chunk: {e}")
            return None

    def close(self):
        """Cleanup when the function stops."""
        if self.feature_batch:
            self.offline_store.store_features_batch(self.feature_batch)


class FeatureAggregationFunction(MapFunction):
    """Aggregate features over time windows."""

    def __init__(self, window_size_seconds: int = 60):
        self.window_size = window_size_seconds
        self.feature_buffers = {}  # call_id -> list of features

    def map(self, value: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate features over time window."""
        try:
            call_id = value["call_id"]
            speaker_id = value["speaker_id"]

            # Initialize buffer for this call/speaker
            key = f"{call_id}:{speaker_id}"
            if key not in self.feature_buffers:
                self.feature_buffers[key] = []

            # Add current features to buffer
            self.feature_buffers[key].append(value)

            # Remove old features (outside window)
            current_time = datetime.fromisoformat(value["timestamp"])
            self.feature_buffers[key] = [
                f
                for f in self.feature_buffers[key]
                if (
                    current_time - datetime.fromisoformat(f["timestamp"])
                ).total_seconds()
                <= self.window_size
            ]

            # Compute aggregated features
            if self.feature_buffers[key]:
                return self._aggregate_features(key, self.feature_buffers[key])
            else:
                return {}

        except Exception as e:
            logger.error(f"Error in feature aggregation: {e}")
            return {}

    def _aggregate_features(self, key: str, features: List[Dict]) -> Dict[str, Any]:
        """Compute aggregated features over the window."""
        if not features:
            return {}

        # Convert to DataFrame-like aggregation
        talk_ratios = [
            f["talk_to_listen_ratio"] for f in features if f.get("talk_to_listen_ratio")
        ]
        interruptions = [
            f["interruption_count"] for f in features if f.get("interruption_count")
        ]
        energies = [f["energy_db"] for f in features if f.get("energy_db")]
        pitches = [f["avg_pitch"] for f in features if f.get("avg_pitch")]

        aggregated = {
            "call_id": features[0]["call_id"],
            "speaker_id": features[0]["speaker_id"],
            "timestamp": features[-1]["timestamp"],  # Latest timestamp
            "window_duration": self.window_size,
            "talk_to_listen_ratio_mean": np.mean(talk_ratios) if talk_ratios else 0.0,
            "talk_to_listen_ratio_std": np.std(talk_ratios) if talk_ratios else 0.0,
            "interruption_count_total": sum(interruptions) if interruptions else 0,
            "energy_mean": np.mean(energies) if energies else 0.0,
            "pitch_mean": np.mean(pitches) if pitches else 0.0,
            "pitch_std": np.std(pitches) if pitches else 0.0,
            "sample_count": len(features),
        }

        return aggregated


def create_kafka_source(env: StreamExecutionEnvironment, topic: str, group_id: str):
    """Create Kafka source connector."""
    properties = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
    }

    return env.add_source(
        FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=AudioChunkSchema(),
            properties=properties,
        )
    ).name(f"kafka_source_{topic}")


def create_kafka_sink(topic: str):
    """Create Kafka sink connector."""
    properties = {"bootstrap.servers": settings.kafka_bootstrap_servers}

    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties,
    )


def setup_flink_job():
    """Setup and configure the Flink streaming job."""
    # Set up streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    # Add Flink Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    # Create pipeline
    audio_source = create_kafka_source(
        env, settings.kafka_raw_audio_topic, settings.kafka_group_id
    )

    # Process audio chunks into features
    features_stream = (
        audio_source.map(VoiceFeatureMapFunction())
        .name("feature_computation")
        .filter(lambda x: x)
    )  # Filter out empty results

    # Aggregate features over 1-minute windows
    aggregated_stream = (
        features_stream.map(FeatureAggregationFunction(window_size_seconds=60))
        .name("feature_aggregation")
        .filter(lambda x: x)
    )

    # Send raw features to features topic
    features_stream.map(lambda x: json.dumps(x)).name("serialize_features").add_sink(
        create_kafka_sink(settings.kafka_features_topic)
    ).name("kafka_sink_features")

    # Send aggregated features to aggregated topic
    aggregated_stream.map(lambda x: json.dumps(x)).name(
        "serialize_aggregated"
    ).add_sink(create_kafka_sink("voice_features_aggregated")).name(
        "kafka_sink_aggregated"
    )

    return env


def main():
    """Main entry point for the Flink job."""
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting Voice Feature Processing Job")

    try:
        env = setup_flink_job()
        env.execute("Voice Feature Processing")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise


if __name__ == "__main__":
    main()
