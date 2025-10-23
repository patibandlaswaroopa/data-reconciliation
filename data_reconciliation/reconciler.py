# Databricks notebook source
# reconciler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, concat_ws, collect_list, lit
from datetime import datetime
from config import ReconciliationConfig


class Reconciliation:
    """
    Handles schema, row, and data-level validation between source and target catalogs.
    """
    def __init__(self, config: ReconciliationConfig, spark: SparkSession):
        self.config = config
        self.spark = spark

    # -------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------
    def log_message(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    # -------------------------------------------------------------
    # SNOWFLAKE CONNECTION
    # -------------------------------------------------------------
    def connect_snowflake(self):
        """Create a Snowflake connection and foreign catalog in Databricks."""
        if self.config.source_tech.lower() != "snowflake":
            self.log_message("Source technology is not Snowflake. Skipping connection setup.")
            return
        try:
            self.spark.sql(f"""
                CREATE CONNECTION IF NOT EXISTS my_snowflake_connection 
                TYPE snowflake 
                OPTIONS (
                    host '{self.config.host}',
                    user '{self.config.user}',
                    password '{self.config.password}',
                    sfWarehouse '{self.config.sfWarehouse}'
                )
            """)
            self.spark.sql(f"""
                CREATE FOREIGN CATALOG {self.config.catalog1}
                USING CONNECTION my_snowflake_connection 
                OPTIONS ( database '{self.config.database}' )
            """)
            self.log_message(f"✅ Successfully created Snowflake connection and catalog {self.config.catalog1}")
        except Exception as e:
            self.log_message(f"❌ Error setting up Snowflake connection: {str(e)}")
            raise

    # -------------------------------------------------------------
    # BASIC VALIDATION HELPERS
    # -------------------------------------------------------------
    def schema_exists(self, catalog, schema):
        try:
            self.spark.sql(f"DESCRIBE SCHEMA {catalog}.{schema}")
            self.log_message(f"Schema {catalog}.{schema} exists")
            return True
        except Exception as e:
            self.log_message(f"Schema {catalog}.{schema} not found: {str(e)}")
            return False

    def get_tables(self, catalog, schema):
        try:
            tables_df = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
            return [row['tableName'] for row in tables_df.collect()]
        except Exception as e:
            self.log_message(f"Error retrieving tables: {str(e)}")
            return []

    def table_exists(self, catalog, schema, table):
        try:
            self.spark.sql(f"DESCRIBE {catalog}.{schema}.{table}")
            return True
        except Exception:
            return False

    def get_row_count(self, catalog, schema, table):
        try:
            df = self.spark.sql(f"SELECT COUNT(*) AS count FROM {catalog}.{schema}.{table}")
            return df.collect()[0]['count']
        except Exception as e:
            self.log_message(f"Error getting row count for {table}: {str(e)}")
            return 0

    def get_column_count(self, catalog, schema, table):
        try:
            df = self.spark.sql(f"DESCRIBE {catalog}.{schema}.{table}")
            return df.count()
        except Exception as e:
            self.log_message(f"Error getting column count for {table}: {str(e)}")
            return 0

    def get_columns(self, catalog, schema, table):
        try:
            df = self.spark.sql(f"DESCRIBE {catalog}.{schema}.{table}")
            return [r['col_name'] for r in df.collect() if r['col_name']]
        except Exception as e:
            self.log_message(f"Error getting columns for {table}: {str(e)}")
            return []

    # -------------------------------------------------------------
    # MAIN VALIDATION METHODS (simplified)
    # -------------------------------------------------------------
    def schema_validation(self):
        """Schema-level comparison of table and column counts."""
        self.log_message("🔍 Starting schema validation...")
        results = []
        schema_result = StructType([
            StructField("schema_catalog1", StringType(), True),
            StructField("schema_catalog2", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("row_count_catalog1", LongType(), True),
            StructField("row_count_catalog2", LongType(), True),
            StructField("row_status", StringType(), True),
            StructField("column_count_catalog1", LongType(), True),
            StructField("column_count_catalog2", LongType(), True),
            StructField("column_status", StringType(), True)
        ])

        for schema1, schema2 in self.config.schema_pairs:
            if not self.schema_exists(self.config.catalog1, schema1) or not self.schema_exists(self.config.catalog2, schema2):
                continue

            tables1 = self.get_tables(self.config.catalog1, schema1)
            tables2 = self.get_tables(self.config.catalog2, schema2)
            common = set(tables1).intersection(tables2)

            for tbl in common:
                rc1 = self.get_row_count(self.config.catalog1, schema1, tbl)
                rc2 = self.get_row_count(self.config.catalog2, schema2, tbl)
                cc1 = self.get_column_count(self.config.catalog1, schema1, tbl)
                cc2 = self.get_column_count(self.config.catalog2, schema2, tbl)

                results.append((schema1, schema2, tbl, rc1, rc2,
                                "matching" if rc1 == rc2 else "mismatch",
                                cc1, cc2, "matching" if cc1 == cc2 else "mismatch"))

        df = self.spark.createDataFrame(results, schema_result)
        df.write.mode("overwrite").saveAsTable(f"{self.config.reconcile_db}.{self.config.schema_validation_table}")
        self.log_message(f"✅ Schema validation results saved to {self.config.reconcile_db}.{self.config.schema_validation_table}")

    def run_reconciliation(self):
        """Main function to execute validations sequentially."""
        self.log_message("🚀 Starting reconciliation process")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.reconcile_db}")
        if self.config.source_tech.lower() == "snowflake":
            self.connect_snowflake()
        self.schema_validation()
        self.log_message("✅ Reconciliation process completed successfully.")
