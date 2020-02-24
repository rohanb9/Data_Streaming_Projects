"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        
        # :: Configure the broker properties below. Reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
             "bootstrap.servers": BROKER_URL,
              "schema.registry.url": SCHEMA_REGISTRY_URL
        }
        
        self.client = AdminClient({"bootstrap.servers": BROKER_URL})
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # :: Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties,
                                     default_key_schema=self.key_schema,
                                     default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # :: Code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        logger.info("-------------- 1] Check if topic exists in kafka (Extra check)  -----------------")
        #exists = False
        exists = self.topic_exists()
        logger.info(f"Topic {self.topic_name} exists: {exists}")
        #         client = AdminClient({"bootstrap.servers": BROKER_URL})
        #         topic_meta = client.list_topics(timeout = 50)
        #         print(str(topic_meta.topics))

        if exists is False:
            print(self.topic_name)
            logger.info("-------------- 2] Topic creation -----------------")
            futures = client.create_topics(
                [
                    NewTopic(
                        topic = self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                        config={
                            "compression.type":"lz4"
                        }
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as e:
                    print(f"failed to create topic {self.topic_name}: {e}")

    def topic_exists(self):
        #         print("--------------")
        #         client = AdminClient({'bootstrap.servers': self.broker_properties["bootstrap.servers"]})
        topic_metadata = self.client.list_topics()
        logger.info(str(topic_metadata))
        return topic_metadata.topics.get(self.topic_name) is not None

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # :: Cleanup code for the Producer here
        if self.producer is not None:
            self.producer.flush()
        logger.info("Producer close - with cleanup")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
