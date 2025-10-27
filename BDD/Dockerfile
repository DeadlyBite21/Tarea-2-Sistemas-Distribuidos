# En: BDD/Dockerfile

# 1. Usar una imagen base de Python
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo
WORKDIR /app

# 3. Copiar solo el archivo de requisitos
COPY requirements.txt .

# 4. Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar el resto del código de la aplicación
COPY . .

# 6. ¡EL PASO QUE FALTABA!
# Ejecuta el servidor FastAPI usando Uvicorn en el puerto 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]