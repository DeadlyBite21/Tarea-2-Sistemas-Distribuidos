# En: flink_job/Dockerfile

# 1. Empieza desde la imagen oficial de Flink (base Java)
FROM flink:1.18.1-scala_2.12

# 2. Cambia al usuario root para instalar
USER root

# 3. Instalar wget, JDK, build-essential, Python DEV y PIP
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

# 4. Copiar/Descargar el JAR del conector Kafka a /opt/flink/lib
RUN cp /opt/flink/connectors/*.jar /opt/flink/lib/ || \
    (echo "WARN: Kafka connector JAR not found..." && \
     wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar -O /opt/flink/lib/flink-sql-connector-kafka.jar)

# 5. Crea directorio, copia requisitos
RUN mkdir -p /opt/flink/job
COPY requirements.txt /opt/flink/job/requirements.txt

# 6. Actualizar pip y setuptools
RUN python3 -m pip install --upgrade pip setuptools

# 7. Instala las dependencias
RUN python3 -m pip install --no-cache-dir -r /opt/flink/job/requirements.txt

# 8. --- Configurar el entorno Python ---
ENV PYTHONPATH=/usr/local/lib/python3.10/site-packages:/usr/local/lib/python3.10/dist-packages:/opt/flink/lib:${PYTHONPATH}

# 9. --- Instalar y verificar pyflink ---
RUN python3 -m pip install --no-cache-dir apache-flink==1.18.1 apache-flink-libraries==1.18.1 && \
    python3 -m pip show apache-flink | grep Location && \
    ls -R /usr/local/lib/python3.10/dist-packages/pyflink/datastream/ && \
    python3 -c "from pyflink.datastream import TimeCharacteristic; print('Basic import successful')" && \
    python3 -c "from pyflink.datastream.functions import KeySelector; print('Functions import successful')" && \
    python3 -c "import pyflink.datastream as ds; print(dir(ds))" && \
    echo "Attempting WatermarkStrategy import..." && \
    python3 -c "from pyflink.common.watermark_strategy import WatermarkStrategy; print('***** WatermarkStrategy import OK during build *****')"

# 10. Copia el código del job y cambia permisos
COPY app.py /opt/flink/job/app.py
RUN chown -R flink:flink /opt/flink/job

# 11. Vuelve al usuario por defecto de Flink ('flink')
USER flink
WORKDIR /opt/flink