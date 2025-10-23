"""
# Data Reconciliation Library

A PySpark-based library for reconciling data between different catalogs (e.g., Snowflake and Databricks).

## Features

- Schema validation (table counts, row counts, column counts)
- Row validation (duplicate detection, extra rows)
- Data validation (column-level value comparison)
- Support for multiple source technologies
- Extensible architecture

## Installation

```bash
pip install data-reconciliation
```

Or install from source:

```bash
git clone https://github.com/yourusername/data-reconciliation.git
cd data-reconciliation
pip install -e .
```

## Quick Start

```python
from pyspark.sql import SparkSession
from data_reconciliation import ReconciliationConfig, Reconciliation

# Initialize Spark
spark = SparkSession.builder.appName("Reconciliation").getOrCreate()

# Configure reconciliation
config = ReconciliationConfig(
    catalog1="source_catalog",
    catalog2="target_catalog",
    schema_pairs=[("source_schema", "target_schema")],
    table_keys={
        "orders": "order_id",
        "customers": "customer_id"
    },
    reconcile_db="reconcile_results",
    schema_validation_table="schema_validation",
    row_validation_table="row_validation",
    data_validation_table_prefix="data_validation_",
    source_tech="snowflake",
    host="your-snowflake-host.snowflakecomputing.com",
    user="username",
    password="password",
    sfWarehouse="COMPUTE_WH",
    database="DATABASE_NAME"
)

# Run reconciliation
reconciler = Reconciliation(config, spark)
reconciler.run_reconciliation()
```

## Documentation

See the [full documentation](https://github.com/yourusername/data-reconciliation/docs) for more details.

## License

MIT L