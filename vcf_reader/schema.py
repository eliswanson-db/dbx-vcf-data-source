from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
)
from vcf_reader.compat import VariantType


def get_vcf_schema() -> StructType:
    """
    Returns the schema for VCF data in bronze layer format.

    Schema preserves core fields as strongly typed columns while keeping
    INFO and genotype FORMAT fields as variants for flexibility.
    """
    genotype_struct = StructType(
        [
            StructField("sampleId", StringType(), False),
            StructField("calls", ArrayType(LongType()), True),
            StructField("data", VariantType(), True),
        ]
    )

    return StructType(
        [
            StructField("contig", StringType(), False),
            StructField("start", LongType(), False),
            StructField("end", LongType(), False),
            StructField("names", ArrayType(StringType()), True),
            StructField("referenceAllele", StringType(), False),
            StructField("alternateAlleles", ArrayType(StringType()), False),
            StructField("qual", DoubleType(), True),
            StructField("filters", ArrayType(StringType()), True),
            StructField("info", VariantType(), True),
            StructField("genotypes", ArrayType(genotype_struct), True),
            StructField("file_path", StringType(), True),
            StructField("file_name", StringType(), True),
            StructField("variant_id", StringType(), True),
        ]
    )
