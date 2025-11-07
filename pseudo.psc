# Consumer polling and push message to queue immediately
def run_consume_to_queue(consumer, topic_name, queue): 
    print(topic_name)
    for message in consumer_polling(consumer, timeout=1.0): 
        if message is None:
            continue
        data = json.load(message)
        while True:
            try: 
                push to queue(data, timeout=2)
                break
            except queue is Full:
                -> time.sleep(2)

# writer get messages from queue and gather in batch, 
# if batch full or reach the write_interval then push to mongodb
def queue_writer_to_mongodb(queue, batch_size = 2000, write_interval, mongodb_collection):
    while True: (# no break signal)
        try:
            message = queue.get(timeout=0.5)
            if "STOP":
                break
                
            batch.append(write_query_to_mongodb)

            if batch_size is full or reach write_interval
                -> bulk_write to mongodb_collection (check for idempotence) 
        
        except queue is Full:
            -> bulk_write to mongodb_collection (check for idempotence)

        except queue is Empty:
            if batch has msg 
                -> bulk_write to mongodb_collection (check for idempotence)
            wait for timeout -> break
    
    if batch has messages:
        -> bulk_write to mongodb_collection (check for idempotence)
                    break 


# main thead that will run the consumer activities,
# writer thread will run in the back
def streaming():
    init_mongodb -> get collection
    init_consumer -> get consumer,topic_name
    init queue(maxsize=10_000)

    create queue_to_mongodb thread -> thread_1
    thread_1.start()

    try:
        run_consume_to_queue(consumer,topic_name,queue)
    except KeyboardInterrupt:
        logging("Stopping downstream pipeline...")
    finally:
        msg_queue.put("STOP")
        thread_write_to_mongodb.join()
        logging("Streaming stopped.")