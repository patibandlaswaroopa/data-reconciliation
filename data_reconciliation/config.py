# Databricks notebook source
# config.py

class ReconciliationConfig:
    """
    Configuration class for reconciliation process.
    Stores metadata about catalogs, schemas, tables, and connection details.
    """
    def __init__(self, catalog1, catalog2, schema_pairs, table_keys,
                 reconcile_db, schema_validation_table, row_validation_table,
                 data_validation_table_prefix, source_tech,
                 host=None, user=None, password=None,
                 sfWarehouse=None, database=None):
        self.catalog1 = catalog1
        self.catalog2 = catalog2
        self.schema_pairs = schema_pairs  # e.g., [("public", "default")]
        self.table_keys = table_keys      # e.g., {"EMPLOYEES": "EMP_ID"}
        self.reconcile_db = reconcile_db  # e.g., "reconcile_db"
        self.schema_validation_table = schema_validation_table
        self.row_validation_table = row_validation_table
        self.data_validation_table_prefix = data_validation_table_prefix
        self.source_tech = source_tech
        self.host = host
        self.user = user
        self.password = password
        self.sfWarehouse = sfWarehouse
        self.database = database
