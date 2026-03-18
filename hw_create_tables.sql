docker exec -it kafka-postgres-1 psql -U postgres -d postgres -c "
CREATE TABLE IF NOT EXISTS tumbling_pickup (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT
);

CREATE TABLE IF NOT EXISTS session_pickup (
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT
);

CREATE TABLE IF NOT EXISTS hourly_tips (
    window_start TIMESTAMP,
    total_tips DOUBLE PRECISION
);
"
