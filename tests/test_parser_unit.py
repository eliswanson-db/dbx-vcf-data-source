"""Unit tests for parser functions that don't require Databricks Runtime."""

import pytest
import sys
import os

# Add parent directory to path to import parser directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from vcf_reader.parser import parse_header, parse_vcf_line


def test_parse_header_single_sample():
    """Test parsing header with single sample."""
    lines = [
        "##fileformat=VCFv4.2",
        "##contig=<ID=chr1,length=248956422>",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1",
    ]
    sample_names, metadata = parse_header(lines)
    assert sample_names == ["Sample1"]
    assert len(metadata) == 2


def test_parse_header_multi_sample():
    """Test parsing header with multiple samples."""
    lines = [
        "##fileformat=VCFv4.2",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1\tSample2\tSample3",
    ]
    sample_names, metadata = parse_header(lines)
    assert sample_names == ["Sample1", "Sample2", "Sample3"]


def test_parse_header_no_samples():
    """Test parsing header without samples."""
    lines = [
        "##fileformat=VCFv4.2",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
    ]
    sample_names, metadata = parse_header(lines)
    assert sample_names == []


def test_parse_vcf_line_basic_structure():
    """Test parsing basic VCF line structure (without variant types)."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14;AF=0.5\tGT:DP:GQ\t0/1:14:99"
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    # Test basic fields
    assert result[0] == "chr1"  # contig
    assert result[1] == 99  # start (0-indexed)
    assert result[2] == 100  # end
    assert result[3] == ["rs001"]  # names
    assert result[4] == "A"  # ref
    assert result[5] == ["T"]  # alt
    assert result[6] == 30.0  # qual
    assert result[7] == ["PASS"]  # filters


def test_parse_vcf_line_multi_allelic():
    """Test parsing multi-allelic VCF line."""
    line = "chr1\t200\trs002\tG\tC,A\t40.5\tPASS\tDP=20;AF=0.3,0.2\tGT:DP\t1/2:20"
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    assert result[5] == ["C", "A"]  # alt alleles
    assert result[9][0]["calls"] == [1, 2]


def test_parse_vcf_line_missing_values():
    """Test parsing VCF line with missing values."""
    line = "chr1\t100\t.\tA\t.\t.\t.\t.\tGT\t./."
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    assert result[3] == []  # empty names
    assert result[5] == []  # empty alt
    assert result[6] is None  # missing qual
    assert result[7] == []  # empty filters


def test_parse_vcf_line_no_samples():
    """Test parsing VCF line without samples."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14;AF=0.5"
    sample_names = []

    result = parse_vcf_line(line, sample_names)

    assert result[9] is None  # no genotypes


def test_parse_vcf_line_sample_filtering_include():
    """Test parsing VCF line with sample inclusion filter."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=42\tGT:DP\t0/1:14\t0/0:15\t1/1:13"
    sample_names = ["Sample1", "Sample2", "Sample3"]

    result = parse_vcf_line(line, sample_names, include_samples=["Sample1", "Sample3"])

    assert len(result[9]) == 2
    assert result[9][0]["sampleId"] == "Sample1"
    assert result[9][1]["sampleId"] == "Sample3"


def test_parse_vcf_line_sample_filtering_exclude():
    """Test parsing VCF line with sample exclusion filter."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=42\tGT:DP\t0/1:14\t0/0:15\t1/1:13"
    sample_names = ["Sample1", "Sample2", "Sample3"]

    result = parse_vcf_line(line, sample_names, exclude_samples=["Sample2"])

    assert len(result[9]) == 2
    assert result[9][0]["sampleId"] == "Sample1"
    assert result[9][1]["sampleId"] == "Sample3"


def test_parse_vcf_line_invalid():
    """Test that invalid VCF line raises error."""
    line = "chr1\t100"  # Too few fields
    sample_names = []

    with pytest.raises(ValueError):
        parse_vcf_line(line, sample_names)


def test_genotype_parsing_homozygous():
    """Test parsing homozygous genotypes."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT:DP\t1/1:20"
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    assert result[9][0]["calls"] == [1, 1]
    assert result[9][0]["sampleId"] == "Sample1"


def test_genotype_parsing_phased():
    """Test parsing phased genotypes."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT:DP\t0|1:15"
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    assert result[9][0]["calls"] == [0, 1]


def test_genotype_parsing_missing():
    """Test parsing missing genotypes."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT:DP\t./.:."
    sample_names = ["Sample1"]

    result = parse_vcf_line(line, sample_names)

    assert result[9][0]["sampleId"] == "Sample1"
    # Missing genotypes are represented as -1 for each allele
    assert result[9][0]["calls"] == [-1, -1]


def test_multiple_samples():
    """Test parsing VCF line with multiple samples."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=42\tGT:DP\t0/1:14\t0/0:15\t1/1:13"
    sample_names = ["Sample1", "Sample2", "Sample3"]

    result = parse_vcf_line(line, sample_names)

    assert len(result[9]) == 3
    assert result[9][0]["calls"] == [0, 1]
    assert result[9][1]["calls"] == [0, 0]
    assert result[9][2]["calls"] == [1, 1]
