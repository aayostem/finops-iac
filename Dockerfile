# Dockerfile
FROM continuumio/miniconda3:latest

# Copy environment file
COPY conda-environment.yml .
RUN conda env create -f conda-environment.yml

# Copy source code
COPY . /app
WORKDIR /app

# Activate environment and run
SHELL ["conda", "run", "-n", "voice-feature-store", "/bin/bash", "-c"]
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "voice-feature-store", "python", "-m", "voice_feature_store.api"]