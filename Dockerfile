FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app
ENV PYTHONPATH=/app

CMD ["python", "-m", "app.crawler.worker"]
