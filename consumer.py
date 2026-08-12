import json
import logging
import sqlite3
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_FILE = 'events_sink.db'

def setup_database():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id TEXT PRIMARY KEY,
                event_count INTEGER DEFAULT 0,
                total_amount REAL DEFAULT 0.0
            )
        ''')
        conn.commit()
        conn.close()
        logger.info("Database setup completed.")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")

def update_user_stats(user_id, amount):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_stats (user_id, event_count, total_amount)
            VALUES (?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                event_count = event_count + 1,
                total_amount = total_amount + excluded.total_amount
        ''', (user_id, amount))
        
        conn.commit()
        cursor.execute('SELECT event_count, total_amount FROM user_stats WHERE user_id = ?', (user_id,))
        stats = cursor.fetchone()
        conn.close()
        
        return stats
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        return None

def create_consumer():
    try:
        consumer = KafkaConsumer(
            'events_topic',
            bootstrap_servers=['localhost:29092'],
            group_id='assignment_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(2, 6, 0),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
        )
        logger.info("Kafka Consumer initialized successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Error initializing Kafka Consumer: {e}")
        return None

def main():
    setup_database()
    consumer = create_consumer()
    
    if not consumer:
        logger.error("Exiting due to consumer initialization failure.")
        return

    logger.info("Listening for messages... Press Ctrl+C to stop.")
    
    try:
        for message in consumer:
            user_id = message.key
            payload = message.value
            partition = message.partition
            
            amount = payload.get('amount', 0.0)
            stats = update_user_stats(user_id, amount)
            
            if stats:
                event_count, total_amount = stats
                logger.info(
                    f"[Partition: {partition}] Processed event for {user_id}. "
                    f"Total Events: {event_count}, Total Amount: {total_amount:.2f}"
                )
            
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"An error occurred while consuming data: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
