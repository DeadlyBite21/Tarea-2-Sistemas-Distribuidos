FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV BOOTSTRAP_SERVERS=kafka:9092
ENV TOPIC_REQUESTS=questions.pending
ENV STORAGE_SERVICE_URL=http://storage:8000
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host","0.0.0.0","--port","8000"]
