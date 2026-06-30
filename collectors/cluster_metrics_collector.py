"""
collectors/cluster_metrics_collector.py

Collects cluster health metrics (CPU, memory, node count)
from Databricks system tables and writes a snapshot to ops.cluster_metrics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging

logger = logging.getLogger(__name__)


class ClusterMetricsCollector:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def collect(self) -> dict:
        df = self.spark.sql("""
            SELECT
                cluster_id,
                cluster_name,
                avg_cpu_user_percent AS cpu_utilization,
                avg_mem_used_percent AS memory_utilization,
                worker_count AS node_count
            FROM system.compute.node_timeline
            WHERE start_time >= current_timestamp() - INTERVAL 1 DAY
        """)

        df = df.withColumn("_collected_at", current_timestamp())

        (df.write
            .format("delta")
            .mode("append")
            .saveAsTable("ops.cluster_metrics")
        )

        cluster_count = df.select("cluster_id").distinct().count()
        logger.info(f"Collected metrics for {cluster_count} clusters")
        return {"clusters_monitored": cluster_count}
