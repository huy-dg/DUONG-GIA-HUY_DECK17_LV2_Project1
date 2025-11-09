import time

from scripts.setup import init_source, init_streaming_process

# from kafka_project.kafka.topic import delete_topic
from kafka_project.kafka.producer import produce_message, final_flush
from kafka_project.kafka.consumer import consumer_polling
from kafka_project.core.logger import setup_logger

logger = setup_logger(__name__, level="DEBUG")

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#
"""
Description:
    - Main pipeline function to stream data from source Kafka topic to streaming Kafka topic.
    - source consumer config: see in setup.py script.
    - streaming producer config: see in setup.py script.
    - pipeline procedures:
        1. Consume message from source topic.
        2. Produce message to streaming topic.
        3. If producer buffer is full, wait until some messages are delivered.
        4. On keyboard interrupt, flush remaining messages in producer buffer.
        5. Finally, flush and close the producer.

"""


def main():
    source_consumer = init_source()
    streaming_producer, streaming_topic_name, admin = init_streaming_process()
    try:
        for msg, *_ in consumer_polling(consumer=source_consumer, timeout=5.0):
            produce_message(
                producer=streaming_producer,
                topic=streaming_topic_name,
                key=None,
                value=msg,
            )

            while len(streaming_producer) > 100_000:
                logger.info("⚠️ Producer buffer full → waiting...")
                streaming_producer.poll(1)
                time.sleep(0.5)
    except KeyboardInterrupt:
        logger.exception("❌ Streaming interrupted by user.")
    finally:
        final_flush(streaming_producer)
        logger.info("✅ Streaming finished.")
        # delete_topic(admin, streaming_topic_name)


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    main()
