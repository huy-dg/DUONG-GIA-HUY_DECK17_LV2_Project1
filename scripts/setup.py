from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from confluent_kafka.admin import AdminClient
from kafka_project.core.config import settings
from kafka_project.kafka.topic import create_new_topic
from kafka_project.kafka.producer import create_producer
from kafka_project.kafka.consumer import consumer_subscribe, on_assign

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#


def init_source():
    source_consumer_config = {
        "bootstrap.servers": settings.source_bootstrap_servers,
        "security.protocol": settings.source_security_protocol,
        "sasl.mechanism": settings.source_sasl_mechanism,
        "sasl.username": settings.source_sasl_username,
        "sasl.password": settings.source_sasl_password,
        "group.id": settings.source_consumer_group_id,
        "auto.offset.reset": settings.topic_auto_offset_reset,
        "enable.auto.commit": settings.enable_auto_commit,
    }
    source_topic_name = settings.source_topic_name

    source_consumer = consumer_subscribe(
        conf=source_consumer_config, topic_name=source_topic_name, on_assign=on_assign
    )

    return source_consumer


# -----------------------------------------------------------------------------#


def init_streaming_process():
    admin_config = {
        "bootstrap.servers": settings.streaming_bootstrap_servers,
        "security.protocol": settings.streaming_security_protocol,
        "sasl.mechanism": settings.streaming_sasl_mechanism,
        "sasl.username": settings.streaming_sasl_username,
        "sasl.password": settings.streaming_sasl_password,
    }

    streaming_topic_config = {
        "name": settings.streaming_topic_name,
        "partitions": settings.topic_partitions,
        "replication_factor": settings.topic_replication_factor,
    }
    streaming_producer_config = {
        "bootstrap.servers": settings.streaming_bootstrap_servers,
        "security.protocol": settings.streaming_security_protocol,
        "sasl.mechanism": settings.streaming_sasl_mechanism,
        "sasl.username": settings.streaming_sasl_username,
        "sasl.password": settings.streaming_sasl_password,
        "acks": settings.acks,
        "enable.idempotence": True,
        "linger.ms": settings.linger_ms,
        "batch.size": settings.batch_size,
        "retries": settings.retries,
        "retry.backoff.ms": settings.retry_backoff_ms,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 10_000,  # ~10MB buffer
    }

    admin = AdminClient(admin_config)
    create_new_topic(admin, streaming_topic_config)
    producer = create_producer(streaming_producer_config)

    return producer, streaming_topic_config["name"], admin


# -----------------------------------------------------------------------------#


def init_consumer_process():
    streaming_consumer_config = {
        "bootstrap.servers": settings.streaming_bootstrap_servers,
        "security.protocol": settings.streaming_security_protocol,
        "sasl.mechanism": settings.streaming_sasl_mechanism,
        "sasl.username": settings.streaming_sasl_username,
        "sasl.password": settings.streaming_sasl_password,
        "group.id": settings.streaming_consumer_group_id,
        "auto.offset.reset": settings.topic_auto_offset_reset,
        "enable.auto.commit": settings.enable_auto_commit,
        "enable.partition.eof": True,
    }
    streaming_topic_name = settings.streaming_topic_name
    consumer = consumer_subscribe(
        conf=streaming_consumer_config,
        topic_name=streaming_topic_name,
        on_assign=on_assign,
    )

    return consumer, streaming_topic_name


# -----------------------------------------------------------------------------#


def init_mongodb():
    mongo_uri = settings.mongodb_uri
    database_name = settings.database_name
    collection_name = settings.collection_name

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000, maxPoolSize=10)
        db = client[database_name]
        collection = db[collection_name]

        client.admin.command("ping")
        print("✅ Connected to MongoDB successfully!")
        return collection

    except ConnectionFailure as e:
        print(f"❌ MongoDB connection error: {e}")
    except ServerSelectionTimeoutError as err:
        print(f"❌ Could not connect to MongoDB: {err}")
        raise
    except Exception as e:
        print(f"❌ Unexpected MongoDB connection error: {e}")
        raise


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    pass
