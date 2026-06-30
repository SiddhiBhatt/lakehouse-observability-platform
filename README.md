# Lakehouse Observability Platform

A monitoring and alerting framework for Azure Databricks that turns cluster health, job run history, and Delta table quality into a live operational dashboard — built for platform engineering teams who need to know what's happening across their Lakehouse before users complain.

> Built by Siddhi Bhatt | Azure Databricks | PySpark | Delta Lake | Power BI | Databricks SQL

---

## Why this project exists

Most data teams find out about pipeline failures from angry Slack messages, not dashboards. As a platform scales past a handful of jobs, nobody has a single place to see: which jobs are failing, which clusters are wasting money, which tables haven't been refreshed in days.

This platform answers three questions an internal platform team is always being asked:
- **Is everything running?** (Job success/failure trends)
- **Is everything fresh?** (Table staleness detection)
- **Is everything efficient?** (Cluster utilization and cost)

---

## Architecture

```
Databricks system tables
        │
        ▼
┌─────────────────────┐
│  Metrics Collector   │  ← Reads system.compute, system.lakeflow tables
│  (scheduled, 15 min)  │    Pulls cluster, job, and table metadata
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  Delta Metrics Store  │  ← ops.cluster_metrics, ops.job_runs, ops.table_health
│   (historized)         │    Queryable history, not just a snapshot
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  Alerting Rules Engine │  ← SLA breach, failure streaks, stale tables,
│                         │    cost spikes — writes to ops.alerts
└──────────┬───────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
 Power BI    Databricks SQL
 dashboard      alerts
```

---

## Key features

| Feature | Detail |
|---|---|
| Job monitoring | Tracks every job run — success, failure, duration, retries |
| Cluster health | CPU, memory, node count, idle time per cluster |
| Table freshness | Flags tables that haven't been updated within their expected SLA |
| Cost visibility | Surfaces cluster runtime cost trends over time |
| Automated alerting | Configurable rules — 3 consecutive failures, SLA breach, stale table |
| Fully historized | Every metric snapshot stored in Delta — trend analysis, not just current state |
| Zero external dependencies | Built entirely on Databricks system tables — no agents to install |

---

## Repo structure

```
lakehouse-observability-platform/
│
├── collectors/
│   ├── cluster_metrics_collector.py    # Pulls cluster CPU/memory/node stats
│   ├── job_run_collector.py            # Pulls job success/failure/duration
│   └── table_health_collector.py       # Checks Delta table freshness and size
│
├── alerting/
│   ├── alert_rules_engine.py           # Evaluates rules, writes to ops.alerts
│   └── alert_rules_config.yml          # Configurable thresholds
│
├── config/
│   └── ops_schema.sql                  # DDL for ops.* monitoring tables
│
├── utils/
│   └── notification_utils.py           # Email/Slack notification helpers
│
├── notebooks/
│   ├── 01_setup_ops_schema.ipynb       # One-time setup
│   ├── 02_run_collectors.ipynb         # Runs all 3 collectors
│   └── 03_run_alerting.ipynb           # Evaluates alert rules
│
├── infra/
│   └── databricks_workflow.yml         # Scheduled job — runs every 15 min
│
├── docs/
│   └── architecture.md
│
├── requirements.txt
└── README.md
```

---

## Core module: `job_run_collector.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

class JobRunCollector:
    """
    Collects job run history from Databricks system tables
    and writes a historized snapshot to ops.job_runs.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def collect(self):
        df = self.spark.sql("""
            SELECT
                job_id,
                run_id,
                job_name,
                result_state,
                period_start_time,
                period_end_time,
                DATEDIFF(SECOND, period_start_time, period_end_time) AS duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= current_timestamp() - INTERVAL 1 DAY
        """)

        df = df.withColumn("_collected_at", current_timestamp())

        (df.write
            .format("delta")
            .mode("append")
            .saveAsTable("ops.job_runs")
        )

        failed = df.filter(col("result_state") == "FAILED").count()
        total = df.count()
        return {"total_runs": total, "failed_runs": failed}
```

---

## Alert rules engine: `alert_rules_engine.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import uuid

class AlertRulesEngine:
    """
    Evaluates configurable alert rules against ops tables.
    Writes triggered alerts to ops.alerts with severity levels.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def evaluate_job_failures(self, threshold: int = 3):
        """Alert if any job has failed 'threshold' times in a row."""
        failures = self.spark.sql(f"""
            SELECT job_name, COUNT(*) as failure_count
            FROM ops.job_runs
            WHERE result_state = 'FAILED'
              AND period_start_time >= current_timestamp() - INTERVAL 1 DAY
            GROUP BY job_name
            HAVING COUNT(*) >= {threshold}
        """)

        for row in failures.collect():
            self._write_alert(
                alert_type="repeated_job_failure",
                severity="high",
                message=f"Job '{row['job_name']}' failed {row['failure_count']} times in 24h"
            )

        return failures.count()

    def evaluate_table_staleness(self, sla_hours: int = 24):
        """Alert if any monitored table hasn't refreshed within its SLA."""
        stale = self.spark.sql(f"""
            SELECT table_name, last_updated
            FROM ops.table_health
            WHERE last_updated < current_timestamp() - INTERVAL {sla_hours} HOURS
        """)

        for row in stale.collect():
            self._write_alert(
                alert_type="stale_table",
                severity="medium",
                message=f"Table '{row['table_name']}' last updated {row['last_updated']} (SLA: {sla_hours}h)"
            )

        return stale.count()

    def _write_alert(self, alert_type: str, severity: str, message: str):
        alert_df = self.spark.createDataFrame([{
            "alert_id": str(uuid.uuid4()),
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
        }])
        alert_df = alert_df.withColumn("triggered_at", current_timestamp())
        alert_df.write.format("delta").mode("append").saveAsTable("ops.alerts")
```

---

## Ops schema: `ops_schema.sql`

```sql
CREATE TABLE IF NOT EXISTS ops.job_runs (
    job_id              STRING,
    run_id              STRING,
    job_name            STRING,
    result_state        STRING,
    period_start_time   TIMESTAMP,
    period_end_time     TIMESTAMP,
    duration_seconds    LONG,
    _collected_at        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops.cluster_metrics (
    cluster_id          STRING,
    cluster_name        STRING,
    cpu_utilization     DOUBLE,
    memory_utilization  DOUBLE,
    node_count          INT,
    _collected_at        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops.table_health (
    table_name          STRING,
    row_count            LONG,
    size_gb              DOUBLE,
    file_count            INT,
    last_updated          TIMESTAMP,
    _collected_at         TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ops.alerts (
    alert_id             STRING,
    alert_type           STRING,
    severity              STRING,
    message               STRING,
    triggered_at          TIMESTAMP
)
USING DELTA;
```

---

## Databricks Workflow — runs every 15 minutes

```yaml
name: lakehouse_observability_pipeline
schedule:
  quartz_cron_expression: "0 */15 * * * ?"
  timezone_id: "America/New_York"

tasks:
  - task_key: collect_metrics
    notebook_task:
      notebook_path: /notebooks/02_run_collectors

  - task_key: evaluate_alerts
    depends_on: [{ task_key: collect_metrics }]
    notebook_task:
      notebook_path: /notebooks/03_run_alerting
```

---

## Getting started

```bash
git clone https://github.com/SiddhiBhatt/lakehouse-observability-platform.git
pip install -r requirements.txt

# In Databricks: run setup notebook to create ops.* tables
# Open notebooks/01_setup_ops_schema.ipynb

# Run collectors manually or let the scheduled workflow handle it
python -c "
from collectors.job_run_collector import JobRunCollector
collector = JobRunCollector(spark)
result = collector.collect()
print(result)
"
```

---

## Tech stack

`Azure Databricks` · `PySpark` · `Delta Lake` · `Databricks SQL` · `Power BI` · `Databricks Workflows` · `Python 3.10+`

---

## About

Built by **Siddhi Bhatt**, Data Engineer with 5+ years of experience building enterprise data pipelines and platform tooling on Azure Databricks.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Siddhi_Bhatt-blue)](https://linkedin.com/in/siddhibhatt)
