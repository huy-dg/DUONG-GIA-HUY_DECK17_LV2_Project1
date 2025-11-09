import json
import time
import threading
from queue import Queue, Full, Empty

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from scripts.setup import init_consumer_process, init_mongodb
from kafka_project.kafka.consumer import consumer_polling
from kafka_project.core.json_processing import save_to_json
from kafka_project.core.logger import setup_logger

# from kafka_project.core.config import settings

logger = setup_logger(__name__, level="DEBUG")

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#
"""
Description:
    - This function consumes messages from a Kafka topic and writes to a queue.

Arguments:
    - consumer: Kafka consumer instance
    - topic_name: Name of the Kafka topic to consume from
    - queue: Thread-safe queue to put the consumed messages into

Returns: 
    - None
"""
"""
new_offset_metadata structure:
{
    "topic": topic_name,
    "partitions": {
        "0": 1234
    }
}
"""


def run_consumer_to_queue(consumer, topic_name, queue):

    print(f"Polling from Kafka topic: {topic_name}")

    for message, topic, partition, offset in consumer_polling(consumer, timeout=10.0):
        try:
            if message is None or type(message) is not str:
                continue
            data = json.loads(message)

            new_offset_metadata = {
                "topic": topic_name,
                "partitions": {str(partition): offset},
            }
            logger.debug("✅ℹ️: message consumed and loaded")
            # manual commit
            # consumer.commit(asynchronous=False)
        except json.JSONDecodeError:
            logger.exception(f"⚠️ Skipping invalid JSON message: {message}")
            continue
        except Exception as e:
            logger.exception(f"❌ Unexpected error (skipping to next message): {e}")
            continue
        try:
            queue.put(data, timeout=2)
            logger.debug("✅ℹ️: data put to queue successfully")
            yield new_offset_metadata
        except Full:
            logger.debug("✅ℹ️: Queue is full")
            time.sleep(2)


# -----------------------------------------------------------------------------#
"""
Description:
    - This function read data from queue in batch and call the bulk_write function
    to write into mongoDB.

Arguments:
    - queue: Thread-safe queue to get data
    - collection: MongoDB collection instance
    - batch_size: Number of documents to write in each batch
    - write_interval: Time interval (in seconds) to flush the batch

Returns: 
    - None
"""


def queue_writer_to_mongodb(
    queue, offset_doc, collection, batch_size=2000, write_interval=5
):

    batch = []
    write_checkpoint = time.time()
    offset_collection = offset_doc[0]

    while True:
        try:

            data = queue.get(timeout=1)  # data in queue is dict
            if data == "STOP":
                break
            batch.append(UpdateOne({"_id": data["_id"]}, {"$set": data}, upsert=True))

            # Flush if batch full or timeout
            if (
                len(batch) >= batch_size
                or time.time() - write_checkpoint > write_interval
            ):
                bulk_write_to_mongodb(batch, collection, retries=3)
                logger.debug("✅ℹ️: Bulk data written")

                with offset_doc[2]:
                    offset_metadata = offset_doc[1].copy()
                offset_collection.update_one(
                    {"_id": offset_metadata["topic"]},
                    {"$set": {"partitions": offset_metadata["partitions"]}},
                    upsert=True,
                )
                logger.debug(
                    f"✅ℹ️: Offset_metadata written: {offset_metadata['partitions'].items()}"
                )

                write_checkpoint = time.time()

        except Empty:
            if batch:
                bulk_write_to_mongodb(batch, collection, retries=3)
                offset_collection.update_one(
                    {"_id": offset_metadata["topic"]},
                    {"$set": {"partitions": offset_metadata["partitions"]}},
                    upsert=True,
                )
                logger.debug(
                    f"✅ℹ️: Offset_metadata written: {offset_metadata['partitions'].items()}"
                )

                write_checkpoint = time.time()
            time.sleep(1)
        except Exception as e:
            logger.exception(f"❌ Error in mongo_writer_worker: {e}")
            continue

    # Write the remaining batch before exiting
    if batch:
        bulk_write_to_mongodb(batch, collection, retries=3)
        offset_collection.update_one(
            {"_id": offset_metadata["topic"]},
            {"$set": {"partitions": offset_metadata["partitions"]}},
            upsert=True,
        )
        logger.debug(
            f"✅ℹ️: Offset_metadata written: {offset_metadata['partition'].items()}"
        )
    logger.info("🧹 Mongo writer thread stopped.")


# -----------------------------------------------------------------------------#
"""
Description:
    - This function handle the bulk write operation to MongoDB with retry logic;
    if failed after retries, saves the batch to a JSON file for further processing.

Arguments:
    - batch: List of MongoDB write operations
    - collection: MongoDB collection instance
    - retries: Number of retries for failed writes

Returns: 
    - None
"""


def bulk_write_to_mongodb(batch, collection, retries=3):
    retry = 0
    while retry < retries:
        try:
            result = collection.bulk_write(batch, ordered=False)
            logger.info(
                f"📦 Batch written: inserted={result.upserted_count}, modified={result.modified_count}"
            )
        except BulkWriteError as e:
            logger.exception(f"⚠️ Bulk write warning: {e.details}")
            retry += 1
            time.sleep(1)
            if retry == retries:
                save_to_json(batch, "./data", "failed_mongo_batch")
                raise Exception(
                    "Max retries reached for bulk write. Saved for further processing."
                )
            continue
        except Exception as e:
            logger.exception(f"❌ MongoDB batch insert error: {e}")
            retry += 1
            time.sleep(1)
            continue
        else:
            batch.clear()
            break
    batch.clear()


# -----------------------------------------------------------------------------#
"""
Description:
    - Main pipeline function to stream data from Kafka to MongoDB.
    - consumer config: see in setup.py script.
    - mongodb config: see in setup.py script.
    - pipeline procedures:
        1. Consume message from Kafka topic.
        2. Put message into a thread-safe queue and safe offsets into offset_metadata.
        3. A separate thread reads from the queue in batches and writes to MongoDB
           while also write offset_metadata into mongoDB.
        4. On keyboard interrupt, stop consuming and wait for the writer thread to finish.
        5. If writer failed after many retries, save the failed batch to a JSON file.
        6. If consumed all messages, receive EOF and send "STOP" signal to writer.
        7. Writer thread flushes remaining messages and exits.
"""
"""
offset_metadata writer structure:
{
    "topic": topic_name,
    "partitions": {
        "0": 1234,
        "1": 1445,
        "2": 1175,
        ...
    }
}
"""


def main():
    collection, offset_collection = init_mongodb()
    consumer, streaming_topic_name = init_consumer_process(offset_collection)
    message_queue = Queue(maxsize=20_000)
    offset_metadata = {"topic": streaming_topic_name, "partitions": {}}
    offset_lock = threading.Lock()
    offset_doc = [offset_collection, offset_metadata, offset_lock]

    logger.debug("✅ℹ️: Process initiation OK")

    writer_thread = threading.Thread(
        target=queue_writer_to_mongodb,
        args=(message_queue, offset_doc, collection, 2000, 5),
        daemon=False,
    )

    writer_thread.start()
    logger.info(
        f"🚀 Start reading from Kafka → MongoDB ({streaming_topic_name} → {collection})"
    )

    try:
        for new in run_consumer_to_queue(consumer, streaming_topic_name, message_queue):
            if "partitions" in new:
                offset_metadata["partitions"].update(new["partitions"])
            with offset_doc[2]:
                offset_doc[1] = offset_metadata
    except KeyboardInterrupt:
        logger.exception("❌ Streaming interrupted by user.")
    except Exception as e:
        logger.exception(f"❌ Unexpected error during streaming: {e}")
    finally:
        message_queue.put("STOP")
        writer_thread.join()
        logger.info("✅ Streaming finished.")


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    main()
