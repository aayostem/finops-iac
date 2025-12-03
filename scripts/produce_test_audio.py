#!/usr/bin/env python3
"""
Script to produce test audio data to Kafka for development and testing.
"""
import json
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_synthetic_audio(duration: float, sample_rate: int = 16000) -> np.ndarray:
    """Generate synthetic audio data for testing."""
    t = np.linspace(0, duration, int(sample_rate * duration))

    # Generate voice-like signal (sine wave with some noise)
    fundamental_freq = 220 + np.random.random() * 100  # Random pitch between 220-320 Hz
    voice_signal = np.sin(2 * np.pi * fundamental_freq * t) * 0.5

    # Add some harmonics
    for harmonic in [2, 3, 4]:
        voice_signal += np.sin(2 * np.pi * fundamental_freq * harmonic * t) * 0.1

    # Add noise
    noise = np.random.normal(0, 0.02, len(t))
    voice_signal += noise

    return voice_signal.astype(np.float32)


def main():
    """Produce test audio data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    call_id = "test_call_" + str(int(time.time()))
    speaker_ids = ["customer", "agent"]

    logger.info(f"Starting to produce test audio for call {call_id}")

    try:
        chunk_id = 0
        while True:
            for speaker_id in speaker_ids:
                # Generate 1 second of audio
                audio_data = generate_synthetic_audio(1.0)

                # Convert to base64 for JSON serialization
                audio_bytes = audio_data.tobytes()
                audio_b64 = audio_bytes.hex()  # Using hex for simplicity

                message = {
                    "call_id": call_id,
                    "speaker_id": speaker_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "audio_data": audio_b64,
                    "sample_rate": 16000,
                    "chunk_duration": 1.0,
                    "chunk_id": chunk_id,
                }

                producer.send("raw_audio", value=message)
                logger.debug(f"Sent chunk {chunk_id} for {speaker_id}")

            chunk_id += 1
            time.sleep(1)  # Send 1 chunk per second per speaker

    except KeyboardInterrupt:
        logger.info("Stopping audio producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
