import json
import time
import random
import logging
from kafka import KafkaProducer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka Producer: {e}")
        return None

def main():
    topic_name = 'events_topic'
    producer = create_producer()
    
    if not producer:
        logger.error("Exiting due to producer initialization failure.")
        return

    fake = Faker()
    possible_keys = ["user_1", "user_2", "user_3"]

    logger.info("Starting to send events. Press Ctrl+C to stop.")
    
    try:
        while True:
            selected_key = random.choice(possible_keys)
            
            payload = {
                "transaction_id": fake.uuid4(),
                "name": fake.name(),
                "amount": round(random.uniform(10.0, 500.0), 2),
                "timestamp": fake.iso8601()
            }
            
            producer.send(topic=topic_name, key=selected_key, value=payload)
            
            logger.info(f"Sent event to {topic_name} - Key: {selected_key} | Payload: {payload}")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.error(f"An error occurred while sending data: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed.")

if __name__ == "__main__":
    main()
