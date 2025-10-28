# Usa la imagen base original de Flink
FROM flink:1.18.1-scala_2.12

# Cambia a usuario root para poder instalar paquetes
USER root

# Instala el JDK, python3, pip3, y el symlink python -> python3
RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    python-is-python3 \
    && apt-get clean

# --- ¡SOLUCIÓN CORREGIDA! ---
# 1. Encuentra la ruta de la carpeta "include" (buscando el archivo jni.h)
# 2. Crea el enlace simbólico
RUN JDK_INCLUDE_DIR=$(find /usr/lib/jvm -type f -name "jni.h" -printf '%h\n' | head -n 1) && \
    if [ -z "$JDK_INCLUDE_DIR" ]; then \
        echo "ERROR: No se pudo encontrar la carpeta 'include' del JDK (que contiene jni.h)."; \
        exit 1; \
    else \
        echo "Enlace 'include' del JDK encontrado en: $JDK_INCLUDE_DIR"; \
        ln -s $JDK_INCLUDE_DIR /opt/java/openjdk/include; \
        echo "Enlace simbólico /opt/java/openjdk/include creado."; \
    fi

# Vuelve al usuario 'flink' por defecto
USER flink