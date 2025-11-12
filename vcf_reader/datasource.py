from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType
from vcf_reader.reader import VCFBatchReader
from vcf_reader.stream_reader import VCFStreamReader


class VCFDataSource(DataSource):
    """
    PySpark custom data source for reading VCF (Variant Call Format) files.

    Supports both batch and streaming reads. Designed as a bronze-layer reader
    that preserves raw data while making it queryable with a structured schema.

    Supports reading both single files and directories (recursively searches for
    .vcf and .vcf.gz files).

    Options:
        - includeSampleIds: Comma-separated list of sample IDs to include
        - excludeSampleIds: Comma-separated list of sample IDs to exclude
        - includeFileMetadata: Include file_path and file_name columns (default: true)
        - generatePrimaryKey: Generate variant_id as SHA256 hash (default: false)
        - flattenInfoFields: (stub) Future option to flatten INFO fields
        - splitMultiAllelics: (stub) Future option to split multi-allelic variants
        - includeHeader: (stub) Future option to include header metadata

    Example:
        spark.dataSource.register(VCFDataSource)
        df = spark.read.format("vcf").load("path/to/file.vcf")
        df = spark.read.format("vcf").load("path/to/directory")
        df = spark.read.format("vcf").option("generatePrimaryKey", "true").load("path/to/directory")
    """

    @classmethod
    def name(cls) -> str:
        """Returns the name of the data source."""
        return "vcf"

    def schema(self) -> str:
        """Returns the schema string for the VCF data source."""
        # Return schema as DDL string
        return """
            contig STRING,
            start BIGINT,
            end BIGINT,
            names ARRAY<STRING>,
            referenceAllele STRING,
            alternateAlleles ARRAY<STRING>,
            qual DOUBLE,
            filters ARRAY<STRING>,
            info VARIANT,
            genotypes ARRAY<STRUCT<
                sampleId: STRING,
                calls: ARRAY<BIGINT>,
                data: VARIANT
            >>,
            file_path STRING,
            file_name STRING,
            variant_id STRING
        """

    def reader(self, schema: StructType) -> VCFBatchReader:
        """
        Returns a batch reader for VCF files.

        Args:
            schema: The schema for the data source

        Returns:
            VCFBatchReader instance
        """
        return VCFBatchReader(schema, self.options)

    def streamReader(self, schema: StructType) -> VCFStreamReader:
        """
        Returns a streaming reader for VCF files.

        Args:
            schema: The schema for the data source

        Returns:
            VCFStreamReader instance
        """
        return VCFStreamReader(schema, self.options)
