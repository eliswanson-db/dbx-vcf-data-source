"""
Integration tests for Databricks Runtime.

These tests should be run in a Databricks notebook with Runtime 15.4+ using:
    %pip install -e .
    %pip install pytest
    pytest tests/test_databricks_integration.py -v

or copy test code into notebook cells.
"""

import pytest
import tempfile
import os

# Only run these tests if DataSource API is available
try:
    from pyspark.sql.datasource import DataSource
    from vcf_reader import VCFDataSource

    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DATABRICKS_AVAILABLE,
    reason="Requires Databricks Runtime 15.4+ for DataSource API",
)


@pytest.fixture(scope="module")
def spark():
    """Get or create Spark session."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark


def create_sample_vcf(path):
    """Create a sample VCF file for testing."""
    vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs001	A	T	30.0	PASS	DP=28;AF=0.5	GT:DP	0/1:14	0/0:14
chr1	200	rs002	G	C	40.5	PASS	DP=40;AF=0.3	GT:DP	0/1:20	0/1:20
chr1	300	rs003	T	G	50.0	PASS	DP=60;AF=0.6	GT:DP	1/1:30	1/1:30
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(vcf_content)


class TestVCFBatchReader:
    """Test batch reading of VCF files."""

    def test_basic_read(self, spark):
        """Test basic VCF reading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.read.format("vcf").load(vcf_path)

            assert df.count() == 3
            assert "contig" in df.columns
            assert "start" in df.columns
            assert "genotypes" in df.columns

    def test_schema_correctness(self, spark):
        """Test that schema matches expected structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.read.format("vcf").load(vcf_path)

            row = df.first()
            assert row.contig == "chr1"
            assert row.start == 99  # 0-indexed
            assert row.referenceAllele == "A"
            assert row.alternateAlleles == ["T"]
            assert len(row.genotypes) == 2

    def test_sample_filtering(self, spark):
        """Test sample inclusion filtering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = (
                spark.read.format("vcf")
                .option("includeSampleIds", "Sample1")
                .load(vcf_path)
            )

            row = df.first()
            assert len(row.genotypes) == 1
            assert row.genotypes[0].sampleId == "Sample1"


class TestVCFStreamReader:
    """Test streaming VCF reads."""

    def test_stream_initialization(self, spark):
        """Test that streaming reader initializes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.readStream.format("vcf").load(vcf_path)

            assert df.isStreaming
            assert "contig" in df.columns


class TestVCFQueries:
    """Test querying VCF data."""

    def test_filter_by_quality(self, spark):
        """Test filtering variants by quality."""
        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.read.format("vcf").load(vcf_path)

            high_qual = df.filter(df.qual > 35)
            assert high_qual.count() == 2

    def test_access_info_fields(self, spark):
        """Test accessing INFO field values."""
        from pyspark.sql.functions import col

        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.read.format("vcf").load(vcf_path)

            # Access INFO fields using variant notation
            info_df = df.select("contig", "start", col("info:DP").alias("depth"))
            assert info_df.first().depth is not None

    def test_explode_genotypes(self, spark):
        """Test exploding genotypes to one row per sample."""
        from pyspark.sql.functions import explode, col

        with tempfile.TemporaryDirectory() as tmpdir:
            vcf_path = os.path.join(tmpdir, "test.vcf")
            create_sample_vcf(vcf_path)

            spark.dataSource.register(VCFDataSource)
            df = spark.read.format("vcf").load(vcf_path)

            genotypes_df = df.select(
                "contig", "start", explode("genotypes").alias("genotype")
            )

            # Should have 3 variants × 2 samples = 6 rows
            assert genotypes_df.count() == 6

            sample_df = genotypes_df.select(col("genotype.sampleId"))
            sample_names = [row.sampleId for row in sample_df.collect()]
            assert "Sample1" in sample_names
            assert "Sample2" in sample_names


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
