FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies (cached)
COPY trainer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy training script
COPY trainer/train.py /app/train.py

# Environment variables
ENV MODEL_PATH=/models/model.pkl

# Default command
CMD ["python", "train.py"]
