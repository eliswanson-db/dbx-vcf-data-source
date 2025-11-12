"""Integration tests that require Databricks Runtime."""

import pytest

# Skip all tests in this module if running outside Databricks
pytestmark = pytest.mark.skipif(
    True,  # Always skip in local environment
    reason="Requires Databricks Runtime 15.4+ for DataSource API",
)


# These tests would run in Databricks notebooks
def test_batch_read_single_sample(spark, sample_vcf_single):
    """Test batch reading of single-sample VCF."""
    from vcf_reader import VCFDataSource

    spark.dataSource.register(VCFDataSource)

    df = spark.read.format("vcf").option("path", sample_vcf_single).load()

    assert df.count() == 4
    assert "contig" in df.columns


def test_stream_reader_basic(spark, sample_vcf_single):
    """Test basic streaming read functionality."""
    from vcf_reader import VCFDataSource

    spark.dataSource.register(VCFDataSource)

    df = spark.readStream.format("vcf").option("path", sample_vcf_single).load()

    assert df.isStreaming
    assert "contig" in df.columns
