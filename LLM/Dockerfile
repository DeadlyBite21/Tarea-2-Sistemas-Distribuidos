FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV BOOTSTRAP_SERVERS=kafka:9092
ENV TOPIC_IN=questions.pending
ENV TOPIC_OK=answers.success
ENV TOPIC_ERR_OVERLOAD=answers.error.overload
ENV TOPIC_ERR_QUOTA=answers.error.quota
EXPOSE 8003
CMD ["uvicorn", "llm_service:app", "--host", "0.0.0.0", "--port", "8003"]
