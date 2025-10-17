FROM python:3.11-slim

RUN pip install --no-cache-dir asyncpg==0.29.0

WORKDIR /app
COPY app.py /app/app.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "/app/app.py"]
