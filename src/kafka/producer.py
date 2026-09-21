import json
import time
import os
import sys
from kafka import KafkaProducer
from loguru import logger
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.data_generator.trade_generator import generate_trade_batch

load_dotenv()

# ── Config ────────────────────────────────────────────────
# Inside docker network, use kafka:29092. From host machine, use localhost:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trades")
TRADES_PER_SECOND = int(os.getenv("TRADES_PER_SECOND", 5))

# ── Producer Setup ────────────────────────────────────────
def create_producer():
    """Create and return a Kafka producer"""
    producer = KafkaProducer( 
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
        compression_type="gzip"
    )
    logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


def send_trade(producer, trade):
    """Send a single trade to Kafka topic"""
    producer.send(
        topic=KAFKA_TOPIC,
        key=trade["stock_symbol"],
        value=trade
    )


def run_producer(duration_seconds=30):
    """
    Main producer loop
    Continuously generates and sends trades to Kafka
    """
    producer = create_producer()
    logger.info(f"Starting producer — sending to topic: {KAFKA_TOPIC}")
    logger.info(f"Rate: {TRADES_PER_SECOND} trades/second")

    start_time = time.time()
    total_sent = 0
    suspicious_sent = 0

    try:
        while True:
            if duration_seconds:
                elapsed = time.time() - start_time
                if elapsed >= duration_seconds:
                    logger.info(f"Duration limit reached: {duration_seconds}s")
                    break

            batch = generate_trade_batch(batch_size=TRADES_PER_SECOND)

            for trade in batch:
                send_trade(producer, trade)
                total_sent += 1

                if trade.get("is_suspicious"):
                    suspicious_sent += 1
                    logger.warning(
                        f"🚨 Suspicious trade sent | "
                        f"{trade.get('suspicious_type')} | "
                        f"{trade.get('trader_id')} | "
                        f"{trade.get('stock_symbol')} | "
                        f"Qty: {trade.get('quantity'):,}"
                    )

            producer.flush()

            logger.info(
                f"Batch sent | Total: {total_sent} | "
                f"Suspicious: {suspicious_sent} | "
                f"Topic: {KAFKA_TOPIC}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.flush()
        producer.close()
        logger.info(
            f"Producer closed | "
            f"Total sent: {total_sent} | "
            f"Suspicious: {suspicious_sent}"
        )


if __name__ == "__main__":
    log_file = os.getenv("LOG_PATH", "/opt/airflow/logs/pipeline.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger.add(log_file, rotation="10 MB")
    
    run_producer(duration_seconds=30)