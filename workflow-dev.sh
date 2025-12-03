# 1. Setup environment
conda env create -f conda-environment.yml
conda activate voice-feature-store
pip install -e ".[dev]"

# 2. Configure environment
cp .env.example .env
# Edit .env with your configuration

# 3. Start services
docker-compose -f docker/docker-compose.yml up -d

# section 2 starts here
# 4. Run tests
pytest tests/ -v

# 5. Start feature computation (in a new terminal)
python -m voice_feature_store.streaming.flink_processor

# 6. Start API server (in a new terminal)
python -m voice_feature_store.api.server

# 7. Produce test data (in a new terminal)
python scripts/produce_test_audio.py

# 8. Test the API
curl http://localhost:8000/health
curl http://localhost:8000/features/test_call_123/customer


# SECTION 3: starts here

### 5. Test with Sample Data
python scripts/produce_test_audio.py

# In another terminal, check features
curl http://localhost:8000/features/test_call_123/customer


## Production Deployment

### 1. Build and Push Docker Image
bash
./scripts/deploy_production.sh latest

### 2. Kubernetes Deploymentbash
# Using Helm
helm upgrade --install voice-feature-store helm/ \
  --namespace voice-analytics \
  --values helm/production-values.yaml

# Or using raw manifests
kubectl apply -f kubernetes/voice-feature-store.yaml
```

### 3. Configuration Management

# Create Kubernetes secrets
kubectl create secret generic voice-feature-store-secrets \
  --from-literal=jwt-secret-key='your-secret-key' \
  --from-literal=redis-password='your-redis-password' \
  --namespace voice-analytics






