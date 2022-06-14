from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

KAFKA_TOPIC = "trip-updates"
KAFKA_SINK_TOPIC = "avg-stop-delays"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
STOPS_DATA_FILE = 'stops.txt'


def processor():

    jar_dir = Path(__file__).parent.resolve()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(f"file://{jar_dir}/flink-sql-connector-kafka-1.15.0.jar")
    env.set_parallelism(1)

    tenv = StreamTableEnvironment.create(env)

    tenv.execute_sql(
        f"""
         CREATE TABLE Stop_updates (
             `stopId` VARCHAR,
             `arrival` STRING,
             `departure` STRING,
             `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
             WATERMARK FOR `ts` AS `ts` - INTERVAL '20' SECONDS
         ) WITH (
             'connector' = 'kafka',
             'topic' = '{KAFKA_TOPIC}',
             'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
             'properties.group.id' = 'processor-python',
             'scan.startup.mode' = 'earliest-offset',
             'format' = 'json'
         )
     """
    )

    # This file contiains the information of all stops
    # referenced by the data stream. This will allow us to associate
    # the geographical position with each data point
    tenv.execute_sql(
        f"""
        CREATE TABLE Stops (
            `stop_id` STRING,
            `stop_name` STRING,
            `stop_lat` STRING,
            `stop_lon` STRING,
            `stop_desc` STRING,
            `location_type` STRING,
            `parent_station` STRING,
            `wheelchair_boarding` STRING,
            `stop_timezone` STRING,
            `vehicle_type` STRING,
            `platform_code` STRING
        ) WITH (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = 'file://{jar_dir}/{STOPS_DATA_FILE}'
        )
        """
    )

    print("Created table `Stops`")

    tenv.execute_sql(
        f"""
        CREATE TABLE Stop_delays (
            `stopId` VARCHAR,
            `stop_name` STRING,
            `stop_lat` STRING,
            `stop_lon` STRING,
            `location_type` STRING,
            `vehicle_type` STRING,
            `avg_departure_delay` DOUBLE,
            `avg_arrival_delay` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'processor-python',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """
    )

    print("Created sink table `Stop_delays`")

    # tenv.execute_sql(
                    # """
                     # SELECT
                        # stopId,
                        # stop_lat,
                        # ts,
                        # JSON_VALUE(`departure`, '$.delay') AS `departure_delay`
                    # FROM Stop_updates
                    # INNER JOIN Stops on stopId = Stops.stop_id
                    # LIMIT 10
    # """).print()

    stop_delays = tenv.sql_query(
        """
        SELECT
            `stopId`,
            `stop_name`,
            `stop_lat`,
            `stop_lon`,
            `location_type`,
            `vehicle_type`,
            `avg_departure_delay`,
            `avg_arrival_delay`
        FROM (
            SELECT
                `stopId`,
                AVG(JSON_VALUE(`departure`, '$.delay' RETURNING INTEGER DEFAULT 0 ON EMPTY)) AS `avg_departure_delay`,
                AVG(JSON_VALUE(`arrival`, '$.delay' RETURNING INTEGER DEFAULT 0 ON EMPTY)) AS `avg_arrival_delay`,
                COUNT(*) as `count`
            FROM TABLE(TUMBLE(TABLE Stop_updates, DESCRIPTOR(ts), INTERVAL '30' SECONDS))
            GROUP BY `stopId`, `window_start`, `window_end`
        )
        INNER JOIN Stops ON stopId = Stops.stop_id
        """
    )

    print("Starting job...")

    tenv.create_statement_set().add_insert("Stop_delays", stop_delays).execute().wait()


if __name__ == "__main__":
    processor()
