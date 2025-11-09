from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from confluent_kafka import TopicPartition, Consumer
from confluent_kafka.admin import AdminClient
from kafka_project.core.config import settings
from kafka_project.kafka.topic import create_new_topic
from kafka_project.kafka.producer import create_producer
from kafka_project.kafka.consumer import consumer_subscribe, on_assign
from kafka_project.core.logger import setup_logger

logger = setup_logger(__name__, level="DEBUG")

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
        "enable.auto.commit": True,
    }
    source_topic_name = settings.source_topic_name

    source_consumer = consumer_subscribe(
        conf=source_consumer_config, topic_name=source_topic_name, on_assign=on_assign
    )
    logger.debug(f"✅ℹ️ ⚙️ Consumer: {source_consumer}")
    logger.debug(f"✅ℹ️ 🗃️ Source topic: {source_topic_name}")
    logger.debug(f"✅ℹ️ 🌐 Kafka Servers: {settings.source_bootstrap_servers}")

    logger.info(
        f"✅: Initiate consumer: {source_consumer} - Source topic: {source_topic_name}"
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

    logger.debug(f"✅ℹ️ ⚙️ Producer: {producer}")
    logger.debug(f"✅ℹ️ 🗃️ Streaming topic: {streaming_topic_config["name"]}")
    logger.debug(
        f"✅ℹ️ 🌐 Streaming Kafka Servers: {settings.streaming_bootstrap_servers}"
    )

    return producer, streaming_topic_config["name"], admin


# -----------------------------------------------------------------------------#


def init_consumer_process(offset_collection):
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

    offset_metadata = offset_collection.find_one({"_id": streaming_topic_name})
    if offset_metadata is None:
        consumer = consumer_subscribe(
            conf=streaming_consumer_config,
            topic_name=streaming_topic_name,
            on_assign=on_assign,
        )
        logger.info("⚙️ No saved offsets — subscribing normally.")
    else:
        logger.info(f"🗃️ Topic {streaming_topic_name} info loaded from mongoDB.")
        for p, o in offset_metadata["partitions"].items():
            logger.info(f"📂 Partition {p} - Offset {o}.")

        partitions = [
            TopicPartition(streaming_topic_name, int(partition), int(offset) + 1)
            for partition, offset in offset_metadata["partitions"].items()
        ]
        consumer = Consumer(streaming_consumer_config)
        consumer.assign(partitions)
        logger.info(f"✅ Assigned consumer to topic: {offset_metadata["_id"]}")

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
        offset_collection = db["offset_metadata"]

        client.admin.command("ping")
        logger.info("✅ Connected to MongoDB successfully!")
        return collection, offset_collection

    except ConnectionFailure as e:
        logger.exception(f"❌ MongoDB connection error: {e}")
    except ServerSelectionTimeoutError as err:
        logger.exception(f"❌ Could not connect to MongoDB: {err}")
        raise
    except Exception as e:
        logger.exception(f"❌ Unexpected MongoDB connection error: {e}")
        raise


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    pass
