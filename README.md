# Real-time Fraud Detection Pipeline

This repository contains a complete example of a real-time fraud detection pipeline combining rule-based detection and ML scoring.

Quick start:

1. Start infrastructure:

   docker-compose -f infra/docker-compose.yml up -d

2. Generate dataset:

   python3 dataset-generator/generate_transactions.py --n 50000

3. Create Kafka topics and ES index mappings:

   infra/kafka-init.sh
   infra/es-init.sh

4. Start producer:

   python3 producer/producer.py --file data/transactions_50k.csv --delay-ms 20

5. Train model (optional):

   python3 ml-training/train_model.py

6. Start ml-service and api (or run via docker-compose builds)

7. Submit Spark job:

   cd spark-job
   ./spark-submit.sh

Open Kibana: http://localhost:5601
