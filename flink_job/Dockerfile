# En: flink_job/Dockerfile

# 1. Empieza desde la imagen oficial de Flink
FROM flink:1.18.1-scala_2.12

# 2. Cambia al usuario root para instalar paquetes
USER root

# 3. Instalar wget + JDK + build-essential + Python y setear JAVA_HOME
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \ 
        default-jdk \
        build-essential \
        python3 \
        python3-dev \
        python3-pip \
        python-is-python3 && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# --- CORRECCIÓN FINAL V4: Descargar el JAR del conector ---
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG KAFKA_CONNECTOR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

RUN wget ${KAFKA_CONNECTOR_URL} -O /opt/flink/lib/flink-sql-connector-kafka.jar
# --- FIN CORRECCIÓN FINAL V4 ---

# 4. Copia solo el archivo de requisitos
COPY requirements.txt /opt/flink/job/requirements.txt

# 5. Instala las dependencias de PyFlink
RUN python3 -m pip install --no-cache-dir -r /opt/flink/job/requirements.txt

# 6. Copia el código de tu job de Flink
COPY app.py /opt/flink/job/app.py

# 7. Vuelve al usuario por defecto de Flink ('flink')
USER flink