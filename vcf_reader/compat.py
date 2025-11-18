"""Compatibility layer for Databricks-specific PySpark features."""

try:
    from pyspark.sql.types import VariantVal, VariantType

    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

    # Mock VariantVal for testing outside Databricks
    class VariantVal:
        """Mock VariantVal for testing purposes."""

        def __init__(self, value):
            self.value = value

        @classmethod
        def parseJson(cls, json_str):
            """Parse JSON string to VariantVal."""
            return cls(json_str)

        def __repr__(self):
            return f"VariantVal({self.value})"

    # Mock VariantType for testing
    class VariantType:
        """Mock VariantType for testing purposes."""

        def __init__(self):
            """Initialize VariantType."""


# Try to import DataSource APIs (only available in Databricks Runtime 15.4+)
try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        DataSourceStreamReader,
        InputPartition,
    )

    DATASOURCE_API_AVAILABLE = True
except ImportError:
    DATASOURCE_API_AVAILABLE = False

    # Mock classes for testing outside Databricks
    class DataSource:
        """Mock DataSource for testing purposes."""

        pass

    class DataSourceReader:
        """Mock DataSourceReader for testing purposes."""

        pass

    class DataSourceStreamReader:
        """Mock DataSourceStreamReader for testing purposes."""

        pass

    class InputPartition:
        """Mock InputPartition for testing purposes."""

        pass


__all__ = [
    "VariantVal",
    "VariantType",
    "DATABRICKS_AVAILABLE",
    "DataSource",
    "DataSourceReader",
    "DataSourceStreamReader",
    "InputPartition",
    "DATASOURCE_API_AVAILABLE",
]
