FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies first (for better build caching)
COPY inference_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and schema
COPY inference_service/app.py /app/app.py
COPY schemas/ohlcv_1m_predictions.avsc /schemas/ohlcv_1m_predictions.avsc

# Environment variables
ENV MODEL_PATH=/models/model.pkl

# Run inference service
CMD ["python", "app.py"]
