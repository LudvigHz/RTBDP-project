import logging
import time

import requests
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2

logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.INFO)


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
        topics = [
            NewTopic(
                topic=current_src.topic,
                num_partitions=4,
                config={"retention.ms": "90000"},
            )
        ]
        futures = client.create_topics(new_topics=topics)  # async operation
        futures[current_src.topic].result()  # wait for operation to complete
        logging.info(f"Created topic {current_src.topic} with 4 partitions")

    except KafkaException:
        # The topic may already exist: check if its configuration matches
        # the desired one
        cluster_metadata = client.list_topics(topic=current_src.topic)
        num_partitions = len(cluster_metadata.topics[current_src.topic].partitions)
        if num_partitions != 4:
            raise Exception(f"Invalid #partitions: {num_partitions} != 4")
        logging.info(f"Found topic {current_src.topic} with 4 partitions")


def process_message(msg, **kwargs):
    produce = kwargs.get("produce", None)
    ts = msg.header.timestamp * 1000
    logging.info(f"Processing new message: Timestamp is {ts}")
    for entity in msg.entity:
        if entity.HasField("trip_update"):
            for update in entity.trip_update.stop_time_update:
                produce(MessageToJson(update), ts)


def produce():
    setup_topic()
    logging.info("Starting producer")
    config = {
        "bootstrap.servers": "localhost:29092",
        "compression.type": "gzip",
        "acks": "1",
        "batch.size": "65536",  # 64 Mb
        "queue.buffering.max.messages": "10000000",
        "linger.ms": "1000",
        "delivery.timeout.ms": "86400000",
        "message.max.bytes": "5000000",  # Trip messages are very large
    }
    producer = Producer(config)

    try:
        while True:
            res = requests.get(Entur_URLS.TRIP_UPDATES)
            msg = gtfs_realtime_pb2.FeedMessage()
            msg.ParseFromString(res.content)
            logging.info(
                f"Recieved {len(res.content)} bytes of data from Entur, pushing to kafka..."
            )
            try:
                process_message(
                    msg,
                    produce=lambda msg, ts: producer.produce(
                        current_src.topic, key=None, value=msg, timestamp=ts
                    ),
                )
            except BufferError:
                logging.warning("Local buffer full, skipping message...")

            # Fetch data every 15 seconds. Entur updates the data every 15 secs
            time.sleep(15)
    except KeyboardInterrupt:
        exit(0)
    finally:
        producer.flush()


if __name__ == "__main__":
    produce()
