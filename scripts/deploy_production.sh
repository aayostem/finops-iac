#!/bin/bash

set -e  # Exit on any error

echo "Starting production deployment of Voice Feature Store..."

# Configuration
REGISTRY="your-registry.com"
IMAGE_TAG="${1:-latest}"
NAMESPACE="voice-analytics"
KUBE_CONTEXT="production-cluster"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi
    
    # Check helm
    if ! command -v helm &> /dev/null; then
        log_error "helm is not installed"
        exit 1
    fi
    
    # Check docker
    if ! command -v docker &> /dev/null; then
        log_error "docker is not installed"
        exit 1
    fi
    
    # Verify Kubernetes context
    CURRENT_CONTEXT=$(kubectl config current-context)
    if [ "$CURRENT_CONTEXT" != "$KUBE_CONTEXT" ]; then
        log_warn "Current context is $CURRENT_CONTEXT, expected $KUBE_CONTEXT"
        read -p "Do you want to continue? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    log_info "Prerequisites check passed"
}

# Build and push Docker image
build_and_push_image() {
    log_info "Building Docker image..."
    
    docker build -t $REGISTRY/voice-feature-store:$IMAGE_TAG -f docker/Dockerfile .
    
    log_info "Pushing Docker image to registry..."
    docker push $REGISTRY/voice-feature-store:$IMAGE_TAG
    
    log_info "Docker image pushed successfully"
}

# Deploy to Kubernetes
deploy_to_kubernetes() {
    log_info "Deploying to Kubernetes..."
    
    # Create namespace if it doesn't exist
    kubectl get namespace $NAMESPACE || kubectl create namespace $NAMESPACE
    
    # Deploy using Helm
    log_info "Deploying with Helm..."
    helm upgrade --install voice-feature-store helm/ \
        --namespace $NAMESPACE \
        --set image.tag=$IMAGE_TAG \
        --set image.repository=$REGISTRY/voice-feature-store \
        --wait --timeout=10m
    
    log_info "Helm deployment completed"
}

# Run health checks
run_health_checks() {
    log_info "Running health checks..."
    
    # Get API service URL
    API_URL=$(kubectl get svc voice-feature-store-api -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    if [ -z "$API_URL" ]; then
        API_URL=$(kubectl get svc voice-feature-store-api -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
    fi
    
    if [ -z "$API_URL" ]; then
        log_error "Could not determine API URL"
        return 1
    fi
    
    API_URL="http://$API_URL:8000"
    
    # Wait for service to be ready
    log_info "Waiting for service to be ready..."
    for i in {1..30}; do
        if curl -s -f "$API_URL/health" > /dev/null 2>&1; then
            log_info "Service is healthy"
            break
        fi
        
        if [ $i -eq 30 ]; then
            log_error "Service health check timeout"
            return 1
        fi
        
        sleep 10
    done
    
    # Test feature endpoint
    log_info "Testing feature endpoint..."
    RESPONSE=$(curl -s "$API_URL/features/test_call_123/speaker_456" || echo "{}")
    if echo "$RESPONSE" | grep -q "error"; then
        log_warn "Feature endpoint test returned error (may be expected for non-existent call)"
    else
        log_info "Feature endpoint test passed"
    fi
    
    # Check pod status
    log_info "Checking pod status..."
    kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=voice-feature-store
    
    log_info "Health checks completed"
}

# Main deployment function
main() {
    log_info "Starting production deployment of Voice Feature Store"
    
    check_prerequisites
    build_and_push_image
    deploy_to_kubernetes
    run_health_checks
    
    log_info "Production deployment completed successfully!"
    
    # Display access information
    log_info "Deployment Summary:"
    echo "  - Image: $REGISTRY/voice-feature-store:$IMAGE_TAG"
    echo "  - Namespace: $NAMESPACE"
    echo "  - Kubernetes Context: $KUBE_CONTEXT"
    echo ""
    log_info "To access the API, use the LoadBalancer IP/hostname from:"
    echo "  kubectl get svc voice-feature-store-api -n $NAMESPACE"
}

# Run main function
main "$@"