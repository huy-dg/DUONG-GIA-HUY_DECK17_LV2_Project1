import json
import time
import threading
from queue import Queue, Full, Empty

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from scripts.setup import init_consumer_process, init_mongodb
from kafka_project.kafka.consumer import consumer_polling
from kafka_project.core.json_processing import save_to_json

# from kafka_project.core.config import settings

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


def run_consumer_to_queue(consumer, topic_name, queue):
    print(f"Polling from Kafka topic: {topic_name}")
    for message in consumer_polling(consumer, timeout=1.0):
        try:
            if message is None or type(message) is not str:
                continue
            data = json.loads(message)
        except json.JSONDecodeError:
            print(f"⚠️ Skipping invalid JSON message: {message}")
            continue
        except Exception as e:
            print(f"❌ Unexpected error (skipping to next message): {e}")
            continue
        while True:
            try:
                queue.put(data, timeout=2)
                break
            except Full:
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


def queue_writer_to_mongodb(queue, collection, batch_size=2000, write_interval=5):

    batch = []
    write_checkpoint = time.time()

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
                write_checkpoint = time.time()

        except Full:
            if batch:
                bulk_write_to_mongodb(batch, collection, retries=3)
                write_checkpoint = time.time()
        except Empty:
            if batch:
                bulk_write_to_mongodb(batch, collection, retries=3)
                write_checkpoint = time.time()
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error in mongo_writer_worker: {e}")
            continue

    # Write the remaining batch before exiting
    if batch:
        bulk_write_to_mongodb(batch, collection, retries=3)
    print("🧹 Mongo writer thread stopped.")


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
            print(
                f"📦 Batch written: inserted={result.upserted_count}, modified={result.modified_count}"
            )
        except BulkWriteError as e:
            print(f"⚠️ Bulk write warning: {e.details}")
            retry += 1
            time.sleep(1)
            if retry == retries:
                save_to_json(batch, "./data", "failed_mongo_batch")
                raise Exception(
                    "Max retries reached for bulk write. Saved for further processing."
                )
            continue
        except Exception as e:
            print(f"❌ MongoDB batch insert error: {e}")
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
        2. Put message into a thread-safe queue.
        3. A separate thread reads from the queue in batches and writes to MongoDB.
        4. On keyboard interrupt, stop consuming and wait for the writer thread to finish.
        5. If writer failed after many retries, save the failed batch to a JSON file.
        6. If consumed all messages, receive EOF and send "STOP" signal to writer.
        7. Writer thread flushes remaining messages and exits.
"""


def streaming():
    collection = init_mongodb()
    consumer, streaming_topic_name = init_consumer_process()
    message_queue = Queue(maxsize=10_000)

    writer_thread = threading.Thread(
        target=queue_writer_to_mongodb,
        args=(message_queue, collection, 2000, 5),
        daemon=True,
    )

    writer_thread.start()
    print(
        f"🚀 Start reading from Kafka → MongoDB ({streaming_topic_name} → {collection})"
    )

    try:
        run_consumer_to_queue(consumer, streaming_topic_name, message_queue)
    except KeyboardInterrupt:
        print("❌ Streaming interrupted by user.")
    except Exception as e:
        print(f"❌ Unexpected error during streaming: {e}")
    finally:
        message_queue.put("STOP")
        writer_thread.join()
        print("✅ Streaming finished.")


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    streaming()
