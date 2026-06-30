CREATE TABLE IF NOT EXISTS ops.job_runs (
    job_id              STRING,
    run_id               STRING,
    job_name             STRING,
    result_state          STRING,
    period_start_time     TIMESTAMP,
    period_end_time       TIMESTAMP,
    duration_seconds      LONG,
    _collected_at          TIMESTAMP
)
USING DELTA
COMMENT 'Historized job run snapshots collected every 15 minutes';

CREATE TABLE IF NOT EXISTS ops.cluster_metrics (
    cluster_id            STRING,
    cluster_name           STRING,
    cpu_utilization         DOUBLE,
    memory_utilization      DOUBLE,
    node_count               INT,
    _collected_at             TIMESTAMP
)
USING DELTA
COMMENT 'Historized cluster health snapshots';

CREATE TABLE IF NOT EXISTS ops.table_health (
    table_name              STRING,
    row_count                 LONG,
    size_gb                    DOUBLE,
    file_count                  INT,
    last_updated                TIMESTAMP,
    _collected_at                TIMESTAMP
)
USING DELTA
COMMENT 'Freshness and size checks for monitored Delta tables';

CREATE TABLE IF NOT EXISTS ops.alerts (
    alert_id                  STRING,
    alert_type                  STRING,
    severity                     STRING,
    message                      STRING,
    triggered_at                  TIMESTAMP
)
USING DELTA
COMMENT 'All triggered alerts with severity and human-readable message';
