# Kestrel M1 batch pipeline: raw files -> curated Parquet -> PostgreSQL
FROM python:3.14-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kestrel_m1 ./kestrel_m1
COPY data/raw ./data/raw

CMD ["python", "-m", "kestrel_m1.pipeline"]
