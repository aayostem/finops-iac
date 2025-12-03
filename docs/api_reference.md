# Voice Feature Store API Reference

## Overview

The Voice Feature Store API provides real-time access to computed voice features for analytics and machine learning applications.

## Base URL

```
https://voice-feature-store.yourcompany.com/api/v1
```

## Authentication

All API endpoints require JWT token authentication.

### Getting an Access Token

```bash
curl -X POST "https://voice-feature-store.yourcompany.com/api/v1/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=api_user&password=your_password"
```

Response:
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

### Using the Access Token

Include the token in the Authorization header:

```http
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...
```

## Rate Limiting

The API implements rate limiting to ensure fair usage:

| Endpoint | Limit | Window |
|----------|-------|--------|
| Feature Reads | 100 requests | per minute |
| Feature Writes | 50 requests | per minute |
| Training Data | 20 requests | per minute |

Rate limit headers are included in responses:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Remaining requests
- `X-RateLimit-Reset`: Unix timestamp when limit resets

## Endpoints

### Get Features

```http
GET /features/{call_id}/{speaker_id}
```

Retrieve the latest computed features for a specific call and speaker.

**Parameters:**
- `call_id` (path): Unique identifier for the call
- `speaker_id` (path): Identifier for the speaker (e.g., "customer", "agent")
- `feature_names` (query): Optional list of specific features to retrieve

**Response:**
```json
{
  "call_id": "call_12345",
  "speaker_id": "customer",
  "features": {
    "talk_to_listen_ratio": 0.75,
    "interruption_count": 2,
    "silence_ratio": 0.15,
    "avg_pitch": 220.5,
    "energy_db": -25.3,
    "voice_confidence": 0.95,
    "timestamp": "2023-10-27T10:00:00Z"
  }
}
```

### Batch Features

```http
GET /features/batch
```

Retrieve features for multiple call/speaker pairs in a single request.

**Parameters:**
- `call_speaker_pairs` (query): List of "call_id:speaker_id" pairs
- `feature_names` (query): Optional list of specific features

**Response:**
```json
{
  "results": {
    "call_123:customer": {
      "talk_to_listen_ratio": 0.75,
      "interruption_count": 2
    },
    "call_456:agent": {
      "talk_to_listen_ratio": 0.62,
      "interruption_count": 1
    }
  }
}
```

### Training Data

```http
GET /training-data
```

Retrieve historical features for model training with point-in-time correctness.

**Parameters:**
- `start_date` (query): Start date for data retrieval (ISO format)
- `end_date` (query): End date for data retrieval (ISO format)
- `call_ids` (query): Optional list of specific call IDs

**Response:**
```json
{
  "start_date": "2023-10-01T00:00:00Z",
  "end_date": "2023-10-27T23:59:59Z",
  "record_count": 15000,
  "data": [
    {
      "call_id": "call_123",
      "speaker_id": "customer",
      "timestamp": "2023-10-27T10:00:00Z",
      "talk_to_listen_ratio": 0.75,
      "interruption_count": 2,
      "...": "..."
    }
  ]
}
```

### Health Check

```http
GET /health
```

Check the health status of the API and its dependencies.

**Response:**
```json
{
  "api": "healthy",
  "online_store": "healthy",
  "overall": "healthy",
  "timestamp": "2023-10-27T10:00:00Z"
}
```

## Feature Definitions

### Core Voice Features

| Feature | Type | Description | Range |
|---------|------|-------------|-------|
| `talk_to_listen_ratio` | float | Ratio of speaking to listening time | 0.0 - 10.0 |
| `interruption_count` | int | Number of interruptions in window | 0 - 100 |
| `silence_ratio` | float | Percentage of silence in audio | 0.0 - 1.0 |
| `avg_pitch` | float | Average fundamental frequency | 50.0 - 400.0 Hz |
| `energy_db` | float | RMS energy in decibels | -100.0 - 0.0 dB |
| `voice_confidence` | float | Confidence in voice activity | 0.0 - 1.0 |

### Advanced Features

| Feature | Type | Description |
|---------|------|-------------|
| `spectral_centroid` | float | Center of mass of spectrum |
| `mfccs` | float[13] | Mel-frequency cepstral coefficients |
| `jitter` | float | Pitch period variability |
| `shimmer` | float | Amplitude variability |
| `formants` | float[3] | Formant frequencies F1, F2, F3 |

## Error Handling

The API uses standard HTTP status codes:

- `200`: Success
- `400`: Bad request (invalid parameters)
- `401`: Unauthorized (invalid or missing token)
- `403`: Forbidden (insufficient permissions)
- `404`: Not found (call/speaker not found)
- `429`: Too many requests (rate limit exceeded)
- `500`: Internal server error

Error response format:
```json
{
  "detail": "Error description",
  "error_code": "ERROR_TYPE",
  "timestamp": "2023-10-27T10:00:00Z"
}
```

## Code Examples

### Python Client

```python
import requests
from typing import List, Dict

class VoiceFeatureClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {token}"}
    
    def get_features(self, call_id: str, speaker_id: str) -> Dict:
        url = f"{self.base_url}/features/{call_id}/{speaker_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_batch_features(self, call_speaker_pairs: List[str]) -> Dict:
        url = f"{self.base_url}/features/batch"
        params = {"call_speaker_pairs": call_speaker_pairs}
        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()
        return response.json()

# Usage
client = VoiceFeatureClient(
    base_url="https://voice-feature-store.yourcompany.com/api/v1",
    token="your_jwt_token"
)

features = client.get_features("call_123", "customer")
batch_features = client.get_batch_features(["call_123:customer", "call_456:agent"])
```

### Real-time Inference Example

```python
def predict_sentiment(features: Dict) -> str:
    score = 0.5
    
    # Feature-based rules
    if features.get('talk_to_listen_ratio'):
        ratio = features['talk_to_listen_ratio']
        if 0.3 <= ratio <= 0.7:
            score += 0.2
    
    if features.get('energy_db') and features['energy_db'] > -30:
        score += 0.1
    
    if features.get('voice_confidence'):
        score += features['voice_confidence'] * 0.1
    
    if score >= 0.7:
        return "positive"
    elif score >= 0.4:
        return "neutral"
    else:
        return "negative"
```

```py
from voice_feature_store.client import VoiceFeatureClient

client = VoiceFeatureClient(
    base_url="http://localhost:8000",
    token="your-jwt-token"
)

# Get latest features
features = client.get_features("call_123", "customer")

# Batch features
batch_features = client.get_batch_features([
    "call_123:customer", 
    "call_456:agent"
])

# Real-time prediction
prediction = client.predict(
    model_name="sentiment_analysis",
    call_id="call_123", 
    speaker_id="customer"
)
```

```py
from voice_feature_store.streaming import VoiceFeatureProcessor

processor = VoiceFeatureProcessor()

# Process audio stream
async for audio_chunk in audio_stream:
    features = processor.compute_features(audio_chunk)
    await client.store_features(features)
    ```
## Best Practices

1. **Use batch endpoints** when retrieving features for multiple calls
2. **Cache feature responses** when possible to reduce API calls
3. **Handle rate limiting** with exponential backoff
4. **Validate features** before using them in models
5. **Monitor feature drift** using the data quality endpoints
```

**docs/architecture_decision_record.md**
```markdown
# Architecture Decision Records

## ADR 1: Dual Feature Store Pattern

**Date**: 2023-10-27
**Status**: Accepted

### Context
We need to serve features for both real-time inference (low latency) and batch training (high throughput with historical data).

### Decision
Implement a dual feature store architecture:
- **Online Store**: Redis for low-latency feature serving (<10ms)
- **Offline Store**: S3/Parquet for historical data and training

### Consequences
**Positive:**
- Optimized performance for both use cases
- Clear separation of concerns
- Cost-effective storage for historical data

**Negative:**
- Increased complexity with two storage systems
- Need to maintain consistency between stores
- Additional operational overhead

## ADR 2: Real-time Processing with Flink

**Date**: 2023-10-27
**Status**: Accepted

### Context
Voice features need to be computed in real-time from audio streams with low latency and exactly-once processing semantics.

### Decision
Use Apache Flink for stream processing due to:
- Strong state management for windowed aggregations
- Exactly-once processing guarantees
- Low latency with high throughput
- Rich windowing capabilities

### Consequences
**Positive:**
- Reliable feature computation
- Support for complex temporal features
- Scalable processing

**Negative:**
- Java dependency and JVM overhead
- Steeper learning curve
- More complex deployment

## ADR 3: Hybrid Package Management

**Date**: 2023-10-27
**Status**: Accepted

### Context
We need to manage both Python dependencies and system-level dependencies (Java, audio libraries) reliably across environments.

### Decision
Use a hybrid approach:
- **Conda**: For environment management and non-Python dependencies
- **pyproject.toml**: For modern Python packaging and dependency specification
- **.env**: For environment-specific configuration

### Consequences
**Positive:**
- Best of both worlds for data science and software engineering
- Reliable handling of complex dependencies
- Modern Python packaging standards

**Negative:**
- Multiple configuration files
- Slightly more complex setup process

## ADR 4: Feature Registry for Metadata Management

**Date**: 2023-10-27
**Status**: Accepted

### Context
As the number of features grows, we need centralized management of feature definitions, lineage, and metadata.

### Decision
Implement a feature registry that tracks:
- Feature definitions and schemas
- Data lineage and provenance
- Ownership and versioning
- Validation rules

### Consequences
**Positive:**
- Improved feature discoverability
- Better governance and compliance
- Reduced training-serving skew

**Negative:**
- Additional metadata management overhead
- Need to keep registry in sync with actual features
```
