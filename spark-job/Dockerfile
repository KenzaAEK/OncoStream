# DOCKERFILE : IMAGE DE BUILD
# Pour compiler les jobs Spark (Scala)

# 1. Base stable OpenJDK officielle
FROM eclipse-temurin:11-jdk-focal

# 2. Installer dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        dirmngr && \
    rm -rf /var/lib/apt/lists/*

# 3. Installer SBT depuis repository officiel
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" \
        | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
        | apt-key add - && \
    apt-get update && apt-get install -y sbt

# 4. Dossier de travail
WORKDIR /opt/spark-job