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

# Start the transaction producer with Kafka output
CMD ["python", "generate_stream.py", "--kafka", "--rate", "5", "--n_customers", "500"]
