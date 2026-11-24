# Kestrel M2: one image for the warehouse build, the Kafka producer/consumer and Spark.
# Build context = repository root (docker-compose.yml: context ..), because M2 reuses the
# M1 cleaning package and the raw pack committed under M1/.
FROM python:3.13-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    KESTREL_SPARK_JDBC_JAR=/opt/jars/postgresql-42.7.4.jar

# PostgreSQL JDBC driver for Spark (put on the driver class path by kestrel_m2.spark.common)
RUN mkdir -p /opt/jars \
 && curl -sSfL -o /opt/jars/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

WORKDIR /app
COPY M2/requirements.txt M2/requirements-dev.txt M2/
RUN pip install --no-cache-dir -r M2/requirements-dev.txt

COPY M1/kestrel_m1 M1/kestrel_m1
COPY M1/data/raw M1/data/raw
COPY M1/output/metrics M1/output/metrics
COPY M2/kestrel_m2 M2/kestrel_m2
COPY M2/tests M2/tests
COPY M2/pytest.ini M2/pytest.ini

WORKDIR /app/M2
CMD ["python", "-m", "kestrel_m2.build", "--check-idempotency"]
