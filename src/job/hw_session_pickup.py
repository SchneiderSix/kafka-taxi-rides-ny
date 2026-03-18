from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-session-pickup',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE session_pickup_sink (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'session_pickup',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        create_source(t_env)
        create_sink(t_env)

        t_env.execute_sql("""
            INSERT INTO session_pickup_sink
            SELECT
                SESSION_START(event_timestamp, INTERVAL '5' MINUTES) AS session_start,
                SESSION_END(event_timestamp, INTERVAL '5' MINUTES) AS session_end,
                PULocationID,
                COUNT(*) AS num_trips
            FROM green_trips
            GROUP BY SESSION(event_timestamp, INTERVAL '5' MINUTES), PULocationID
        """).wait()

    except Exception as e:
        print("Job failed:", str(e))


if __name__ == '__main__':
    run()
