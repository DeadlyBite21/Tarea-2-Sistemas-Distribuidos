FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y wget && \
    wget -O /usr/local/bin/wait-for-it https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod +x /usr/local/bin/wait-for-it && \
    rm -rf /var/lib/apt/lists/*

COPY . .

EXPOSE 8003

CMD ["wait-for-it", "kafka:29092", "--timeout=60", "--", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]