# Build: docker build -t oncostream/dashboard:latest .
# Run:   docker run -p 8501:8501 -e HBASE_HOST=hbase oncostream/dashboard

FROM python:3.10-slim

# Métadonnées
LABEL maintainer="oncostream-team"
LABEL version="1.0.0"
LABEL description="Dashboard médical OncoStream (Streamlit + HBase)"

# Variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# Dépendances système (pour compilation happybase/thrift)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Installation Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY dashboard.py .
# COPY .streamlit/ .streamlit/ <-- DÉCOMMENTE SEULEMENT SI LE DOSSIER EXISTE

# Utilisateur non-root
RUN useradd -m -u 1000 streamlit && \
    chown -R streamlit:streamlit /app

USER streamlit

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Exposition du port
EXPOSE 8501

# Lancement Streamlit
ENTRYPOINT ["streamlit", "run"]
CMD ["dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]