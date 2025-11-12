import os
import gzip
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("vcf-reader-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_vcf_single(tmp_path):
    """Create a sample single-sample VCF file."""
    vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14;AF=0.5	GT:DP:GQ	0/1:14:99
chr1	200	rs002	G	C,A	40.5	PASS	DP=20;AF=0.3,0.2;DB	GT:DP:GQ	1/2:20:85
chr1	300	.	T	G	.	LowQual	DP=5	GT:DP	0/0:5
chr2	150	rs003	C	T	50.0	PASS	DP=30	GT:DP:GQ	1/1:30:99
"""
    vcf_path = tmp_path / "sample_single.vcf"
    vcf_path.write_text(vcf_content)
    return str(vcf_path)


@pytest.fixture
def sample_vcf_multi(tmp_path):
    """Create a sample multi-sample VCF file."""
    vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2	Sample3
chr1	100	rs001	A	T	30.0	PASS	DP=42	GT:DP	0/1:14	0/0:15	1/1:13
chr1	200	rs002	G	C	40.5	PASS	DP=60	GT:DP	0/0:20	0/1:22	0/1:18
chr1	300	rs003	T	G	50.0	PASS	DP=45	GT:DP	1/1:15	0/1:15	0/0:15
"""
    vcf_path = tmp_path / "sample_multi.vcf"
    vcf_path.write_text(vcf_content)
    return str(vcf_path)


@pytest.fixture
def sample_vcf_gzipped(tmp_path):
    """Create a gzipped VCF file."""
    vcf_content = b"""##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT:DP	0/1:14
chr1	200	rs002	G	C	40.5	PASS	DP=20	GT:DP	1/1:20
"""
    vcf_path = tmp_path / "sample.vcf.gz"
    with gzip.open(vcf_path, "wb") as f:
        f.write(vcf_content)
    return str(vcf_path)


@pytest.fixture
def sample_vcf_missing_values(tmp_path):
    """Create a VCF file with missing values."""
    vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	.	A	.	.	.	.	GT:DP	./.:..
chr1	200	rs002	G	C	40.5	PASS	DP=20	GT:DP	0/1:20
"""
    vcf_path = tmp_path / "sample_missing.vcf"
    vcf_path.write_text(vcf_content)
    return str(vcf_path)


@pytest.fixture
def sample_vcf_no_samples(tmp_path):
    """Create a VCF file without genotype data."""
    vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14;AF=0.5
chr1	200	rs002	G	C	40.5	PASS	DP=20;AF=0.3
"""
    vcf_path = tmp_path / "sample_no_genotypes.vcf"
    vcf_path.write_text(vcf_content)
    return str(vcf_path)
