import time
from confluent_kafka import Consumer, KafkaException, KafkaError  # ,TopicPartition
from kafka_project.core.config import settings

from kafka_project.core.json_processing import save_to_json


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#


def consumer_config():
    consumer_conf = {
        "bootstrap.servers": settings.source_bootstrap_servers,
        "security.protocol": settings.source_security_protocol,
        "sasl.mechanism": settings.source_sasl_mechanism,
        "sasl.username": settings.source_sasl_username,
        "sasl.password": settings.source_sasl_password,
        "group.id": settings.source_consumer_group_id,
        "auto.offset.reset": settings.topic_auto_offset_reset,
        "enable.auto.commit": settings.enable_auto_commit,
    }
    return consumer_conf


# -----------------------------------------------------------------------------#


def consumer_subscribe(conf, topic_name, on_assign):
    consumer = Consumer(conf)
    topic_name = topic_name if isinstance(topic_name, list) else [topic_name]
    consumer.subscribe(topic_name, on_assign=on_assign)
    print(f"Subscribed to topic: {topic_name}")
    return consumer


# -----------------------------------------------------------------------------#


def on_assign(consumer, partitions):
    print("✅ Assigned partitions:", [p.partition for p in partitions])
    consumer.assign(partitions)


# -----------------------------------------------------------------------------#


def consumer_polling(consumer, timeout):
    message_count = 0

    try:
        print("📡 Listening for messages... (Ctrl+C to stop)")
        while True:
            msg = consumer.poll(timeout)

            if msg is None:
                # print("No message received within the timeout period.")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                        f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                    )
                    break
                else:
                    raise KafkaException(msg.error())

            message_count += 1
            print(f"📥 Received message #{message_count}")
            yield msg.value().decode("utf-8")

    except Exception as e:
        print(f"❌ Error during polling: {str(e)}")
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user during polling.")

    finally:
        consumer.close()
        print(f"📦 Total messages consumed: {message_count}")


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    consumer_conf = consumer_config()
    source_topic = [settings.source_topic_name]
    consumer = consumer_subscribe(
        conf=consumer_conf, topic_name=source_topic, on_assign=on_assign
    )

    start = time.perf_counter()
    message = []
    for msg in consumer_polling(consumer=consumer, timeout=3.0):
        message.append(msg)
    end = time.perf_counter()

    save_to_json(data=message, output_dir="./data", save_name="source_topic_messages")
    consumer.close()

    print("Time elapsed (s): ", end - start)


# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#
# -----------------------------------------------------------------------------#

"""
def consumer_topic_message_count(consumer,source_topic):
    metadata=consumer.list_topics(timeout=5).topics
    partitions = metadata[source_topic[0]].partitions.keys()
    topic_list = [TopicPartition(source_topic[0], p) for p in partitions]
    beginning_offsets = consumer.beginning_offset(topic_list)
    end_offsets = consumer.end_offsets(topic_list)
    print(metadata)
    print(partitions)
    print(topic_list)

    
    total_messages = 0
    for tp in topic_list:
        start = beginning_offsets[tp]
        end = end_offsets[tp]
        diff = end - start
        total_messages += diff
        print(f"🧩 Partition {tp.partition}: start={start}, end={end}, count={diff}")

    print(f"📊 Total message in topic '{source_topic}': {total_messages}")
"""
