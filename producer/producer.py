import time

import requests
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import Schema
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2
from icecream import ic


class Entur_URLS:
    VEHICLE_POSITIONS = "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
    TRIP_UPDATES = "https://api.entur.io/realtime/v1/gtfs-rt/trip-updates"


class TRIP_UPDATES:
    url = Entur_URLS.TRIP_UPDATES
    topic = "trip-updates"


class VEHICLE_POSITIONS:
    url = Entur_URLS.VEHICLE_POSITIONS
    topic = "vehicle-positions"


current_src = TRIP_UPDATES


def setup_topic():

    # Create the Kafka admin client
    config = {
        "bootstrap.servers": "localhost:29092",
    }
    client = AdminClient(config)

    try:
        # Try to create the topic with the desired configuration (# partitions)
        topics = [NewTopic(topic=current_src.topic, num_partitions=4)]
        futures = client.create_topics(new_topics=topics)  # async operation
        futures[current_src.topic].result()  # wait for operation to complete
        print(f"Created topic {current_src.topic} with 4 partitions")

    except KafkaException:
        # The topic may already exist: check if its configuration matches
        # the desired one
        cluster_metadata = client.list_topics(topic=current_src.topic)
        num_partitions = len(cluster_metadata.topics[current_src.topic].partitions)
        if num_partitions != 4:
            raise Exception(f"Invalid #partitions: {num_partitions} != 4")
        print(f"Found topic {current_src.topic} with 4 partitions")


def configure_schema():

    client = SchemaRegistryClient({"url": "http://localhost:8085/"})

    try:
        with open("gtfs-realtime.proto", "r") as proto_file:
            content = proto_file.read()

            schema = Schema(content, "PROTOBUF")
            client.register_schema("gtfs", schema)
    except FileExistsError:
        print("Could not find gtfs-realtime.proto file")


def process_message(msg, **kwargs):
    produce = kwargs.get("produce", None)
    ts = msg.header.timestamp * 1000
    print(f"Timestamp is {ts}")
    for entity in msg.entity:
        if entity.HasField("trip_update"):
            for update in entity.trip_update.stop_time_update:
                produce(MessageToJson(update), ts)


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
        "message.max.bytes": "5000000",  # Trip messages are very large
    }
    producer = Producer(config)

    try:
        while True:
            res = requests.get(Entur_URLS.TRIP_UPDATES)
            msg = gtfs_realtime_pb2.FeedMessage()
            msg.ParseFromString(res.content)
            print(
                f"Recieved {len(res.content)} bytes of data from Entur, pushing to kafka..."
            )
            process_message(
                msg,
                produce=lambda msg, ts: producer.produce(
                    current_src.topic, key=None, value=msg, timestamp=ts
                ),
            )

            # Fetch data every 15 seconds. Entur updates the data every 15 secs
            time.sleep(15)
    except KeyboardInterrupt:
        exit(0)
    finally:
        producer.flush()


if __name__ == "__main__":
    produce()
