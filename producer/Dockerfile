FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 producer && chown -R producer:producer /app
USER producer

# Default environment variables
ENV KAFKA_BOOTSTRAP=kafka:9094
ENV KAFKA_TOPIC=transactions-raw
ENV TXN_RATE=5

# Run the production generator
CMD ["sh", "-c", "python generate_stream_production.py --kafka --rate $TXN_RATE --topic $KAFKA_TOPIC --bootstrap-servers $KAFKA_BOOTSTRAP"]
