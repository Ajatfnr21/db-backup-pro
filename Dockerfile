FROM python:3.11-slim

WORKDIR /app

# Install PostgreSQL and MySQL clients
RUN apt-get update && apt-get install -y \
    postgresql-client \
    mysql-client \
    mongodb-clients \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backup.py .
COPY tests/ ./tests/

EXPOSE 8000

CMD ["python", "backup.py", "serve"]
