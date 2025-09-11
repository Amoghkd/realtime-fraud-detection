FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies including those required for LightGBM
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    cmake \
    libboost-dev \
    libboost-system-dev \
    libboost-filesystem-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and create ML model directory
COPY . .
RUN mkdir -p /app/ml-training

# Create non-root user and fix permissions
RUN chown -R spark:spark /app /opt/spark
USER spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip
ENV ML_MODEL_PATH=/app/ml-training/model.pkl

# Start the fraud detection service
CMD ["python3", "fraud_detection_simple.py"]
