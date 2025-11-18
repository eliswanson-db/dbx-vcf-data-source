"""VCF Spark Reader - PySpark custom data source for VCF files."""

__version__ = "0.1.0"

try:
    from vcf_reader.datasource import VCFDataSource
    from vcf_reader.schema import get_vcf_schema

    __all__ = ["VCFDataSource", "get_vcf_schema"]
except ImportError:
    __all__ = []

# Export Arrow reader for direct usage
try:
    from vcf_reader.arrow_reader import (
        ArrowVCFReader,
        read_vcf_with_arrow,
        ARROW_AVAILABLE,
    )

    __all__.extend(["ArrowVCFReader", "read_vcf_with_arrow", "ARROW_AVAILABLE"])
except ImportError:
    pass
