# Build: docker build -t oncostream/producer:latest .
# Run:   docker run -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 oncostream/producer

FROM python:3.10-slim

# Métadonnées
LABEL maintainer="oncostream-team"
LABEL version="1.0.0"
LABEL description="Producteur de données génomiques NGS vers Kafka"

# Variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPIC=ngs-raw-reads \
    PRODUCER_RATE=10 \
    PRODUCER_DURATION=0

WORKDIR /app

# Installation des dépendances (cache Docker optimisé)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY ngs_producer.py .
COPY check_kafka.py .
# COPY stress_test.sh .  <-- DÉCOMMENTE SI TU AS CRÉÉ CE FICHIER

# Utilisateur non-root (sécurité)
RUN useradd -m -u 1000 producer && \
    chown -R producer:producer /app
    # && chmod +x stress_test.sh <-- DÉCOMMENTE SI TU AS LE FICHIER

USER producer

# Healthcheck (vérifie que Kafka est joignable)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python check_kafka.py || exit 1

# Point d'entrée par défaut
ENTRYPOINT ["python", "ngs_producer.py"]
CMD ["--rate", "10", "--duration", "0"]