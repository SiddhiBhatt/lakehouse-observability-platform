"""
collectors/table_health_collector.py

Checks Delta table freshness, size, and file count for a configured
list of monitored tables. Writes results to ops.table_health.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging

logger = logging.getLogger(__name__)


class TableHealthCollector:

    def __init__(self, spark: SparkSession, monitored_tables: list):
        """
        Args:
            monitored_tables: list of fully-qualified table names to check,
                               e.g. ["dev_catalog.gold.vendor_summary"]
        """
        self.spark = spark
        self.monitored_tables = monitored_tables

    def collect(self) -> dict:
        results = []

        for table_name in self.monitored_tables:
            try:
                history = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
                last_updated = history.collect()[0]["timestamp"]

                detail = self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
                size_gb = detail["sizeInBytes"] / (1024 ** 3)
                file_count = detail["numFiles"]

                row_count = self.spark.table(table_name).count()

                results.append({
                    "table_name": table_name,
                    "row_count": row_count,
                    "size_gb": round(size_gb, 4),
                    "file_count": file_count,
                    "last_updated": last_updated,
                })
            except Exception as e:
                logger.error(f"Failed to check table health for {table_name}: {str(e)}")

        if results:
            df = self.spark.createDataFrame(results)
            df = df.withColumn("_collected_at", current_timestamp())
            (df.write
                .format("delta")
                .mode("append")
                .saveAsTable("ops.table_health")
            )

        logger.info(f"Checked health for {len(results)} tables")
        return {"tables_checked": len(results)}
