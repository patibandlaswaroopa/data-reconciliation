# Databricks notebook source

from pyspark.sql.types import StructType, StructField, StringType, LongType
from typing import Dict, List, Tuple, Optional


class ReconciliationConfig:
    
    def __init__(
        self,
        catalog1: str,
        catalog2: str,
        schema_pairs: List[Tuple[str, str]],
        table_keys: Dict[str, str],
        reconcile_db: str,
        schema_validation_table: str,
        row_validation_table: str,
        data_validation_table_prefix: str,
        source_tech: str,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        sfWarehouse: Optional[str] = None,
        database: Optional[str] = None
    ):
        self.catalog1 = catalog1
        self.catalog2 = catalog2
        self.schema_pairs = schema_pairs
        self.table_keys = table_keys
        self.reconcile_db = reconcile_db
        self.schema_validation_table = schema_validation_table
        self.row_validation_table = row_validation_table
        self.data_validation_table_prefix = data_validation_table_prefix
        self.source_tech = source_tech
        self.host = host
        self.user = user
        self.password = password
        self.sfWarehouse = sfWarehouse
        self.database = database

    @staticmethod
    def get_schema_validation_schema() -> StructType:
        return StructType([
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

    @staticmethod
    def get_row_validation_schema() -> StructType:
        
        return StructType([
            StructField("table_name", StringType(), True),
            StructField("validation_type", StringType(), True),
            StructField("source_count", LongType(), True),
            StructField("target_count", LongType(), True),
            StructField("result", StringType(), True),
            StructField("duplicate_count", LongType(), True),
            StructField("duplicate_keys", StringType(), True),
            StructField("notes", StringType(), True)
        ])

    @staticmethod
    def get_data_validation_schema() -> StructType:
        return StructType([
            StructField("PK_Key", StringType(), True),
            StructField("Column_Name", StringType(), True),
            StructField("Source_Value_Snowflake", StringType(), True),
            StructField("Target_Value_Databricks", StringType(), True),
            StructField("Match", StringType(), True),
            StructField("Notes", StringType(), True)
        ])
