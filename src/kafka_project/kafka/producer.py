import time

from confluent_kafka import Producer, KafkaException

# from kafka_project.core.config import settings

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#


def create_producer(conf: dict):
    producer = Producer(conf)
    print("✅ Producer created successfully")
    return producer


# -----------------------------------------------------------------------------#


def produce_message(producer, topic: str, value: str, key: str = None):
    try:
        producer.produce(
            topic,
            key=str(key) if key else None,
            value=value,
            callback=delivery_report,
        )
        producer.poll(0)

    except BufferError:
        print("⚠️ Local producer queue is full. Flushing...")
        producer.flush()
        time.sleep(0.5)
        producer.produce(
            topic,
            key=str(key) if key else None,
            value=value,
            callback=delivery_report,
        )
    except KeyboardInterrupt:
        print("❌ Production interrupted by user.")
        final_flush(producer)
    except KafkaException as e:
        print(f"❌ Exception during produce: {e.args[0]}")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")


# -----------------------------------------------------------------------------#


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err.str()}")
    else:
        print(
            f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


# -----------------------------------------------------------------------------#


def final_flush(producer):

    print("🚿 Flushing producer buffer...")
    producer.flush(timeout=10)
    print("✅ All messages flushed.")


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    pass
