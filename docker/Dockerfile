FROM continuumio/miniconda3:latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy environment files
COPY conda-environment.yml .
COPY pyproject.toml .

# Create conda environment
RUN conda env create -f conda-environment.yml

# Copy source code
COPY src/ ./src/
COPY config/ ./config/

# Activate conda environment and set Python path
ENV PATH /opt/conda/envs/voice-feature-store/bin:$PATH
ENV PYTHONPATH /app/src:$PYTHONPATH

# Expose ports
EXPOSE 8000 8081

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command (can be overridden)
CMD ["python", "-m", "voice_feature_store.api.server"]