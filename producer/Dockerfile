FROM python:3.11-slim
WORKDIR /app
COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/app.py /app/app.py
COPY producer/healthcheck.py /app/healthcheck.py
CMD ["python", "app.py"]
