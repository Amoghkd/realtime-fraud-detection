#!/usr/bin/env python3
"""
Simple Elasticsearch Alert Aggregator
Just consumes alerts from Kafka and indexes them to Elasticsearch for Kibana
"""

import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleESAggregator:
    def __init__(self):
        # Configuration from environment variables
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        es_host = os.getenv('ELASTICSEARCH_HOST', 'localhost')
        es_port = int(os.getenv('ELASTICSEARCH_PORT', '9200'))
        consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'es-aggregator')
        
        # Kafka setup
        self.consumer = KafkaConsumer(
            'alerts.rule.high_value',
            'alerts.rule.velocity', 
            'alerts.rule.impossible_travel',
            'alerts.ml.v1',
            bootstrap_servers=kafka_servers,
            group_id=consumer_group,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Elasticsearch setup
        self.es = Elasticsearch([{'host': es_host, 'port': es_port}])
        self.index_name = os.getenv('ELASTICSEARCH_INDEX', 'fraud-alerts')
        
        # Create index if it doesn't exist
        self.create_index()
        
    def create_index(self):
        """Create Elasticsearch index with mapping"""
        mapping = {
            "mappings": {
                "properties": {
                    "alert_id": {"type": "keyword"},
                    "alert_type": {"type": "keyword"},
                    "transaction_id": {"type": "keyword"},
                    "transaction_amount": {"type": "float"},
                    "merchant_name": {"type": "text"},
                    "customer_id": {"type": "keyword"},
                    "fraud_score": {"type": "float"},
                    "severity": {"type": "keyword"},
                    "reason": {"type": "text"},
                    "timestamp": {"type": "date"},
                    "alert_timestamp": {"type": "date"},
                    "topic": {"type": "keyword"},
                    "@timestamp": {"type": "date"}
                }
            }
        }
        
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"✅ Created Elasticsearch index: {self.index_name}")
            else:
                logger.info(f"✅ Elasticsearch index already exists: {self.index_name}")
        except Exception as e:
            logger.error(f"❌ Error creating index: {e}")
    
    def process_alert(self, alert, topic):
        """Process and enrich alert before indexing"""
        # Add topic and timestamp info
        alert['topic'] = topic
        alert['@timestamp'] = datetime.utcnow().isoformat()
        
        # Ensure we have an alert_id
        if 'alert_id' not in alert:
            alert['alert_id'] = f"{alert.get('transaction_id', 'unknown')}_{alert.get('alert_type', 'unknown')}_{int(datetime.utcnow().timestamp())}"
        
        # Convert timestamp strings to proper format if needed
        if 'timestamp' in alert and isinstance(alert['timestamp'], str):
            try:
                # Try to parse various timestamp formats
                alert['timestamp'] = datetime.fromisoformat(alert['timestamp'].replace('Z', '+00:00')).isoformat()
            except:
                alert['timestamp'] = datetime.utcnow().isoformat()
        
        if 'alert_timestamp' in alert and isinstance(alert['alert_timestamp'], str):
            try:
                alert['alert_timestamp'] = datetime.fromisoformat(alert['alert_timestamp'].replace('Z', '+00:00')).isoformat()
            except:
                alert['alert_timestamp'] = datetime.utcnow().isoformat()
        
        return alert
    
    def index_alert(self, alert):
        """Index alert to Elasticsearch"""
        try:
            result = self.es.index(
                index=self.index_name,
                id=alert['alert_id'],
                body=alert
            )
            logger.info(f"📝 Indexed alert {alert['alert_id']} to Elasticsearch")
            return result
        except Exception as e:
            logger.error(f"❌ Error indexing alert: {e}")
            return None
    
    def run(self):
        """Main processing loop"""
        logger.info("🚀 Starting Simple ES Aggregator")
        logger.info(f"📊 Consuming from topics: alerts.rule.high_value, alerts.rule.velocity, alerts.rule.impossible_travel, alerts.ml.v1")
        logger.info(f"📈 Indexing to Elasticsearch: {self.index_name}")
        
        alert_count = 0
        
        try:
            for message in self.consumer:
                alert_count += 1
                topic = message.topic
                alert = message.value
                
                logger.info(f"📨 Received alert #{alert_count} from {topic}")
                logger.info(f"   Type: {alert.get('alert_type', 'UNKNOWN')}")
                logger.info(f"   Transaction: {alert.get('transaction_id', 'N/A')}")
                
                # Process and enrich the alert
                processed_alert = self.process_alert(alert, topic)
                
                # Index to Elasticsearch
                result = self.index_alert(processed_alert)
                
                if result:
                    logger.info(f"✅ Alert successfully indexed to Elasticsearch")
                else:
                    logger.warning(f"⚠️  Failed to index alert")
                
                logger.info("   " + "="*50)
                
        except KeyboardInterrupt:
            logger.info(f"\n🔄 Processed {alert_count} alerts total")
            logger.info("👋 ES Aggregator stopped")
        except Exception as e:
            logger.error(f"❌ Error in processing loop: {e}")

if __name__ == "__main__":
    aggregator = SimpleESAggregator()
    aggregator.run()
