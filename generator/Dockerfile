FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ¡IMPORTANTE! Copia el dataset al contenedor
COPY train.csv . 

COPY app.py .

# Comando para ejecutar el script
CMD ["python", "app.py"]