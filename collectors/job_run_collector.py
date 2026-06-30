"""
collectors/job_run_collector.py

Collects job run history from Databricks system tables
and writes a historized snapshot to ops.job_runs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging

logger = logging.getLogger(__name__)


class JobRunCollector:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def collect(self) -> dict:
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

        logger.info(f"Collected {total} job runs, {failed} failed")
        return {"total_runs": total, "failed_runs": failed}
