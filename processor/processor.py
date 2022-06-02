import requests
from google.transit import gtfs_realtime_pb2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


class Entur_URLS:
    VEHICLE_POSITIONS = "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
    TRIP_UPDATES = "https://api.entur.io/realtime/v1/gtfs-rt/trip-updates"


def processor():

    feed = gtfs_realtime_pb2.FeedMessage()
    env = StreamExecutionEnvironment.get_execution_environment()

    tenv = StreamTableEnvironment.create(env)

    res = requests.get(Entur_URLS.VEHICLE_POSITIONS)


if __name__ == "__main__":
    processor()
