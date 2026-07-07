CREATE DATABASE IF NOT EXISTS yagpcc;

CREATE TABLE IF NOT EXISTS yagpcc._yagpcc_meta (
    version        Int32,
    applied_at     DateTime64(3, 'UTC') DEFAULT now64(),
    yagpcc_version String,
    direction      LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (version, applied_at);

CREATE TABLE IF NOT EXISTS yagpcc.query_events (
    event_time      DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    status          Enum8('UNKNOWN'=0, 'SUBMIT'=1, 'START'=2, 'DONE'=3,
                          'QUERY_DONE'=4, 'ERROR'=5, 'CANCELLING'=6,
                          'CANCELED'=7, 'END'=8),
    query_id        UInt64,
    plan_id         UInt64,
    tmid            Int32,
    session_id      Int32,
    command_count   Int32,
    submit_time     Nullable(DateTime64(3, 'UTC')),
    start_time      Nullable(DateTime64(3, 'UTC')),
    end_time        Nullable(DateTime64(3, 'UTC')),
    duration_ms     Nullable(UInt64),
    user            LowCardinality(String),
    database        LowCardinality(String),
    resource_group  LowCardinality(String),
    generator       Enum8('UNKNOWN'=0, 'PLANNER'=1, 'OPTIMIZER'=2),
    template_query  String,
    template_plan   String,
    segments        Array(Tuple(
        dbid Int32, segindex Int32,
        cpu_user_sec Float64, cpu_kernel_sec Float64,
        rss_bytes UInt64, ntuples UInt64,
        read_bytes UInt64, write_bytes UInt64
    )),
    total_cpu_user_sec       Float64,
    total_cpu_kernel_sec     Float64,
    total_running_sec        Float64,
    total_rss_bytes          UInt64,
    total_read_bytes         UInt64,
    total_write_bytes        UInt64,
    total_ntuples            UInt64,
    total_shared_blks_hit    UInt64,
    total_shared_blks_read   UInt64,
    total_blk_read_time_sec  Float64,
    total_blk_write_time_sec Float64,
    total_sent_bytes         UInt64,
    total_recv_bytes         UInt64,
    spill_files              Int32,
    spill_bytes              Int64,
    plan_tree                String CODEC(ZSTD(3)),
    yagpcc_version           LowCardinality(String),
    schema_version           UInt32
) ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (query_id, plan_id, event_time)
TTL toDate(event_time) + INTERVAL {{.RetentionDays}} DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS yagpcc.aggregated_metrics (
    bucket_time       DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    query_id          UInt64,
    plan_id           UInt64,
    user              LowCardinality(String),
    database          LowCardinality(String),
    resource_group    LowCardinality(String),
    executions        UInt64,
    total_cpu_sec     Float64,
    total_running_sec Float64,
    total_rss_bytes   UInt64,
    total_io_bytes    UInt64,
    total_ntuples     UInt64,
    avg_duration_ms   Float64,
    max_duration_ms   UInt64
) ENGINE = SummingMergeTree((executions, total_cpu_sec, total_running_sec, total_rss_bytes, total_io_bytes, total_ntuples))
PARTITION BY toDate(bucket_time)
ORDER BY (user, database, query_id, plan_id, bucket_time)
TTL toDate(bucket_time) + INTERVAL {{.RetentionDays}} DAY DELETE;

CREATE TABLE IF NOT EXISTS yagpcc.session_snapshots (
    snapshot_time   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    session_id      Int32,
    pid             Int32,
    user            LowCardinality(String),
    database        LowCardinality(String),
    application     LowCardinality(String),
    client_addr     String,
    backend_start   DateTime64(3, 'UTC'),
    xact_start      Nullable(DateTime64(3, 'UTC')),
    query_start     Nullable(DateTime64(3, 'UTC')),
    state_change    Nullable(DateTime64(3, 'UTC')),
    state           LowCardinality(String),
    waiting         UInt8,
    query           String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_time)
ORDER BY (snapshot_time, session_id)
TTL toDate(snapshot_time) + INTERVAL {{.RetentionDays}} DAY DELETE;
