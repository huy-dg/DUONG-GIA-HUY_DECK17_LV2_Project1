from pydantic_settings import BaseSettings, SettingsConfigDict

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#


class Settings(BaseSettings):
    # Data source Server configuration
    source_bootstrap_servers: str
    source_security_protocol: str
    source_sasl_mechanism: str
    source_sasl_username: str
    source_sasl_password: str

    # Data source consumer configuration
    source_consumer_group_id: str
    source_topic_name: str

    # Data streaming Server configuration
    streaming_bootstrap_servers: str
    streaming_security_protocol: str
    streaming_sasl_mechanism: str
    streaming_sasl_username: str
    streaming_sasl_password: str
    streaming_topic_name: str
    streaming_consumer_group_id: str

    # General topic/consumer/producer configuration
    topic_partitions: int
    topic_replication_factor: int
    topic_auto_offset_reset: str
    enable_auto_commit: bool
    acks: str
    linger_ms: int
    retries: int
    retry_backoff_ms: int
    batch_size: int

    # MongoDB configuration
    mongodb_uri: str
    database_name: str
    collection_name: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    print(settings.data_source_bootstrap_servers)
    print(settings.kafka_topic)
    print(settings.streaming_bootstrap_servers)
