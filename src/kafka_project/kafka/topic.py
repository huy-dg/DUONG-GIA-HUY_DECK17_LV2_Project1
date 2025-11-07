from confluent_kafka.admin import AdminClient, NewTopic, KafkaException, KafkaError
from kafka_project.core.config import settings

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def config_admin():
    conf = {
            'bootstrap.servers':    settings.streaming_bootstrap_servers,
            'security.protocol':    settings.streaming_security_protocol,
            'sasl.mechanism':       settings.streaming_sasl_mechanism,
            'sasl.username':        settings.streaming_sasl_username,
            'sasl.password':        settings.streaming_sasl_password
    }
    admin = AdminClient(conf)
    return admin

#-----------------------------------------------------------------------------#

def config_topic():
    topic_config = {
        'name':                 settings.streaming_topic_name,
        'partitions':           settings.topic_partitions,
        'replication_factor':   settings.topic_replication_factor
    }
    return topic_config

#-----------------------------------------------------------------------------#

def create_new_topic(admin, topic_config):
    """
    Connects to a Kafka server using the provided bootstrap servers
    and create a new topic

    Args:
        admin (obj): Admin Object that control Kafka servers.

    Returns:
        Confirmation of created topic message.
    """
    topic_list = admin.list_topics(timeout=10).topics
    print(f"Existing topics: {list(topic_list)}")

    new_topic_name = topic_config['name']
    partitions = topic_config['partitions']
    replication_factor = topic_config['replication_factor']

    new_topic = NewTopic(topic=new_topic_name, num_partitions=partitions, replication_factor=replication_factor)
    fs = admin.create_topics([new_topic])
    for topic,f in fs.items():
        try:
            f.result(timeout=10)
            print(f"Created topic {topic}")
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"⚠️ Topic '{topic}' already exists")
            else:
                print(f"❌ Failed to create topic '{topic}': {err.str()}")
        except Exception as e:    
            print(f"Error: {e}")

#-----------------------------------------------------------------------------#

def describe_topics(admin):
    """
    Describe topics from Kafka server.

    Args:
        admin (obj): Admin Object that control Kafka servers.

    Returns:
        Information of existing topics.
    """
    topic_list = admin.list_topics(timeout=10).topics
    print(f"Existing topics: {list(topic_list)}")
    for topic_name in list(topic_list):
        print(f"{topic_name}")
        topic_info = topic_list[topic_name]
        for pid, part in topic_info.partitions.items():
            leader = part.leader
            replicas = part.replicas
            isr = part.isrs
            print(f"Partition {pid}: leader={leader}, replicas={replicas}, isr={isr}")

#-----------------------------------------------------------------------------#

def delete_topic(admin, topic_name):
    """
    Deletes a topic from Kafka server.

    Args:
        admin (obj): Admin Object that control Kafka servers.

    Returns:
        Confirmation of delete message.
    """
    fs = admin.delete_topics([topic_name], operation_timeout=10)

    for topic,f in fs.items():
        try:
            f.result(timeout=10)
            print(f"🗑️ Deleted topic '{topic}'")
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"⚠️ Topic '{topic}' does not exist.")
            else:
                print(f"❌ Failed to delete topic '{topic}': {err.str()}")
        except Exception as e:    
            print(f"Error: {e}")

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    admin = config_admin()
    topic_config = config_topic()
    #create_new_topic(admin,topic_config)
    describe_topics(admin)
    delete_topic(admin, topic_name=topic_config['name'])
