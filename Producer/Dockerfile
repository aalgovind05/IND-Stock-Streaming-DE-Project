FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 TZ=Asia/Kolkata

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
CMD ["python", "producer.py"]
