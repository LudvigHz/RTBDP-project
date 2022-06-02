import time

import requests
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import Schema
from google.transit import gtfs_realtime_pb2


class Entur_URLS:
    VEHICLE_POSITIONS = "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
    TRIP_UPDATES = "https://api.entur.io/realtime/v1/gtfs-rt/trip-updates"


def setup_topic():

    # Create the Kafka admin client
    config = {
        "bootstrap.servers": "localhost:29092",
    }
    client = AdminClient(config)

    try:
        # Try to create the topic with the desired configuration (# partitions)
        topics = [NewTopic(topic="vehicle-positions", num_partitions=4)]
        futures = client.create_topics(new_topics=topics)  # async operation
        futures["vehicle-positions"].result()  # wait for operation to complete
        print("Created topic 'vehicle-positions' with 4 partitions")

    except KafkaException:
        # The topic may already exist: check if its configuration matches
        # the desired one
        cluster_metadata = client.list_topics(topic="vehicle-positions")
        num_partitions = len(cluster_metadata.topics["vehicle-positions"].partitions)
        if num_partitions != 4:
            raise Exception(f"Invalid #partitions: {num_partitions} != 4")
        print("Found topic 'vehicle-positions' with 4 partitions")


def configure_schema():

    client = SchemaRegistryClient({"url": "http://localhost:8085/"})

    try:
        with open("gtfs-realtime.proto", "r") as proto_file:
            content = proto_file.read()

            schema = Schema(content, "PROTOBUF")
            client.register_schema("gtfs", schema)
    except FileExistsError:
        print("Could not find gtfs-realtime.proto file")


def produce():
    configure_schema()
    setup_topic()
    print("Starting producer")
    config = {
        "bootstrap.servers": "localhost:29092",
        "compression.type": "gzip",
        "acks": "1",
        # "linger.ms": "5000",
        "delivery.timeout.ms": "86400000",
    }
    producer = Producer(config)

    try:
        while True:
            res = requests.get(Entur_URLS.VEHICLE_POSITIONS)
            print(
                f"Recieved {len(res.content)} bytes of data from Entur, pushing to kafka..."
            )
            producer.produce("vehicle-positions", key=None, value=res.content)

            # Fetch data every 15 seconds. Matches entur api updates
            time.sleep(15)
    except KeyboardInterrupt:
        exit(0)
    finally:
        producer.flush()


if __name__ == "__main__":
    produce()
