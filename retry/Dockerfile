# 1. Usar una imagen base de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo
WORKDIR /app

# 3. Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar todo el código
# (copiará retry_overload.py y retry_quota.py)
COPY . .

# El 'command' para ejecutar se especifica en el docker-compose.yml