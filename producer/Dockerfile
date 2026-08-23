FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ ./

ENTRYPOINT ["python", "producer.py"]
