# Base image for Python. Use 3.12 for wider compatibility.
FROM python:3.12-slim

# Install Java (required for Spark) and cURL for downloads
RUN apt-get update && apt-get install -y openjdk-21-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set the working directory inside the container
WORKDIR /app

# Copy application files
COPY ./fraud_detection_simple.py ./fraud_detection_simple.py
COPY ./model.onnx ./model.onnx

# Install Python build essentials first to avoid dependency issues
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install application dependencies
# IMPORTANT: Updated numpy version for Python 3.12 compatibility
RUN pip install --no-cache-dir \
    numpy \
    scikit-learn==1.3.2 \
    joblib==1.3.2 \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    onnxruntime

# Expose ports if needed (optional)
EXPOSE 4040

# Environment variables for Spark job
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9094
ENV ML_MODEL_PATH=/app/model.onnx
ENV SPARK_MASTER=local[*]

# Command to run the Spark job
CMD ["python", "./fraud_detection_simple.py"]