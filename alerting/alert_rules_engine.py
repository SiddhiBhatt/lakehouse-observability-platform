"""
alerting/alert_rules_engine.py

Evaluates configurable alert rules against ops tables.
Writes triggered alerts to ops.alerts with severity levels.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import uuid
import logging

logger = logging.getLogger(__name__)


class AlertRulesEngine:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def evaluate_job_failures(self, threshold: int = 3) -> int:
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

        count = failures.count()
        logger.info(f"Job failure check: {count} alerts triggered")
        return count

    def evaluate_table_staleness(self, sla_hours: int = 24) -> int:
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

        count = stale.count()
        logger.info(f"Table staleness check: {count} alerts triggered")
        return count

    def evaluate_cluster_cost(self, cpu_threshold: float = 90.0) -> int:
        hot_clusters = self.spark.sql(f"""
            SELECT cluster_name, AVG(cpu_utilization) as avg_cpu
            FROM ops.cluster_metrics
            WHERE _collected_at >= current_timestamp() - INTERVAL 1 DAY
            GROUP BY cluster_name
            HAVING AVG(cpu_utilization) >= {cpu_threshold}
        """)

        for row in hot_clusters.collect():
            self._write_alert(
                alert_type="high_cluster_utilization",
                severity="low",
                message=f"Cluster '{row['cluster_name']}' averaging {row['avg_cpu']:.1f}% CPU over 24h"
            )

        count = hot_clusters.count()
        logger.info(f"Cluster cost check: {count} alerts triggered")
        return count

    def _write_alert(self, alert_type: str, severity: str, message: str):
        alert_df = self.spark.createDataFrame([{
            "alert_id": str(uuid.uuid4()),
            "alert_type": alert_type,
            "severity": severity,
            "message": message,
        }])
        alert_df = alert_df.withColumn("triggered_at", current_timestamp())
        alert_df.write.format("delta").mode("append").saveAsTable("ops.alerts")
