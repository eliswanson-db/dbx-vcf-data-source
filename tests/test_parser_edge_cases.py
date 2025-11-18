"""Additional edge case tests for parser functions."""

import pytest
import json
from vcf_reader.parser import (
    parse_header,
    parse_vcf_line,
    parse_info_field,
    parse_genotype,
)


class TestParseHeaderEdgeCases:
    """Edge case tests for parse_header function."""

    def test_parse_header_with_many_metadata_lines(self):
        """Test parsing header with many metadata lines."""
        lines = [
            "##fileformat=VCFv4.2",
            "##fileDate=20230101",
            "##source=MyVariantCaller",
            "##reference=hg38",
            "##contig=<ID=chr1,length=248956422>",
            "##contig=<ID=chr2,length=242193529>",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description='Total Depth'>",
            "##INFO=<ID=AF,Number=A,Type=Float,Description='Allele Frequency'>",
            "##FORMAT=<ID=GT,Number=1,Type=String,Description='Genotype'>",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1",
        ]
        sample_names, metadata = parse_header(lines)
        assert sample_names == ["Sample1"]
        assert len(metadata) == 9

    def test_parse_header_with_special_characters_in_sample_names(self):
        """Test parsing header with special characters in sample names."""
        lines = [
            "##fileformat=VCFv4.2",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample-1\tSample_2\tSample.3",
        ]
        sample_names, metadata = parse_header(lines)
        assert sample_names == ["Sample-1", "Sample_2", "Sample.3"]

    def test_parse_header_only_metadata(self):
        """Test parsing header with only metadata lines (no column header)."""
        lines = [
            "##fileformat=VCFv4.2",
            "##contig=<ID=chr1,length=248956422>",
        ]
        sample_names, metadata = parse_header(lines)
        assert sample_names == []
        assert len(metadata) == 2

    def test_parse_header_empty_list(self):
        """Test parsing empty header list."""
        lines = []
        sample_names, metadata = parse_header(lines)
        assert sample_names == []
        assert len(metadata) == 0


class TestParseInfoFieldEdgeCases:
    """Edge case tests for parse_info_field function."""

    def test_parse_info_field_empty_string(self):
        """Test parsing empty INFO field."""
        result = parse_info_field("")
        assert result is not None

    def test_parse_info_field_dot(self):
        """Test parsing '.' (missing) INFO field."""
        result = parse_info_field(".")
        assert result is not None

    def test_parse_info_field_single_flag(self):
        """Test parsing INFO field with single flag."""
        result = parse_info_field("DB")
        assert result is not None

    def test_parse_info_field_multiple_flags(self):
        """Test parsing INFO field with multiple flags."""
        result = parse_info_field("DB;H2")
        assert result is not None

    def test_parse_info_field_mixed_flags_and_values(self):
        """Test parsing INFO field with both flags and key-value pairs."""
        result = parse_info_field("DP=14;AF=0.5;DB;H2")
        assert result is not None

    def test_parse_info_field_array_values(self):
        """Test parsing INFO field with array values."""
        result = parse_info_field("AF=0.5,0.3,0.2;AC=10,5,3")
        assert result is not None

    def test_parse_info_field_string_values(self):
        """Test parsing INFO field with string values."""
        result = parse_info_field("SVTYPE=DEL;GENE=BRCA1")
        assert result is not None

    def test_parse_info_field_special_characters(self):
        """Test parsing INFO field with special characters."""
        result = parse_info_field("NOTE=test_value;ID=rs12345")
        assert result is not None

    def test_parse_info_field_very_long(self):
        """Test parsing INFO field with many key-value pairs."""
        long_info = ";".join([f"KEY{i}={i}" for i in range(100)])
        result = parse_info_field(long_info)
        assert result is not None


class TestParseGenotypeEdgeCases:
    """Edge case tests for parse_genotype function."""

    def test_parse_genotype_missing_all(self):
        """Test parsing genotype with all missing data."""
        result = parse_genotype("GT:DP:GQ", ".:.:.", "Sample1")
        assert result["sampleId"] == "Sample1"

    def test_parse_genotype_partial_missing(self):
        """Test parsing genotype with partially missing data."""
        result = parse_genotype("GT:DP:GQ", "0/1:.:99", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [0, 1]

    def test_parse_genotype_haploid(self):
        """Test parsing haploid genotype."""
        result = parse_genotype("GT:DP", "1:10", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [1]

    def test_parse_genotype_triploid(self):
        """Test parsing triploid genotype."""
        result = parse_genotype("GT:DP", "0/1/1:15", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [0, 1, 1]

    def test_parse_genotype_phased_multiple_alleles(self):
        """Test parsing phased genotype with multiple alleles."""
        result = parse_genotype("GT:DP", "1|2:20", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [1, 2]

    def test_parse_genotype_no_gt_field(self):
        """Test parsing genotype without GT field."""
        result = parse_genotype("DP:GQ", "14:99", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] is None

    def test_parse_genotype_gt_only(self):
        """Test parsing genotype with only GT field."""
        result = parse_genotype("GT", "0/1", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [0, 1]

    def test_parse_genotype_many_format_fields(self):
        """Test parsing genotype with many FORMAT fields."""
        result = parse_genotype(
            "GT:DP:GQ:AD:PL:AF", "0/1:30:99:15,15:100,0,100:0.5", "Sample1"
        )
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [0, 1]

    def test_parse_genotype_empty_string(self):
        """Test parsing empty genotype string."""
        result = parse_genotype("GT:DP", "", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] is None

    def test_parse_genotype_array_values(self):
        """Test parsing genotype with array values in data fields."""
        result = parse_genotype("GT:AD:PL", "0/1:10,20:0,30,255", "Sample1")
        assert result["sampleId"] == "Sample1"
        assert result["calls"] == [0, 1]


class TestParseVCFLineEdgeCases:
    """Edge case tests for parse_vcf_line function."""

    def test_parse_vcf_line_very_long_ref_allele(self):
        """Test parsing VCF line with very long reference allele."""
        long_ref = "A" * 1000
        line = f"chr1\t100\trs001\t{long_ref}\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[4] == long_ref
        # end = start + len(ref), start = pos - 1
        assert result[2] == 99 + len(long_ref)

    def test_parse_vcf_line_many_alt_alleles(self):
        """Test parsing VCF line with many alternate alleles."""
        alt_alleles = ",".join([f"ALT{i}" for i in range(10)])
        line = f"chr1\t100\trs001\tA\t{alt_alleles}\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert len(result[5]) == 10

    def test_parse_vcf_line_many_filters(self):
        """Test parsing VCF line with many filters."""
        filters = ";".join([f"FILTER{i}" for i in range(10)])
        line = f"chr1\t100\trs001\tA\tT\t30.0\t{filters}\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert len(result[7]) == 10

    def test_parse_vcf_line_large_position(self):
        """Test parsing VCF line with very large position."""
        line = "chr1\t999999999\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[1] == 999999998  # 0-indexed

    def test_parse_vcf_line_special_contig_names(self):
        """Test parsing VCF line with special contig names."""
        line = "chrM\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[0] == "chrM"

    def test_parse_vcf_line_multiple_ids(self):
        """Test parsing VCF line with multiple IDs."""
        line = "chr1\t100\trs001;rs002;rs003\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[3] == ["rs001", "rs002", "rs003"]

    def test_parse_vcf_line_structural_variant(self):
        """Test parsing VCF line representing structural variant."""
        line = "chr1\t100\trs001\tA\t<DEL>\t30.0\tPASS\tSVTYPE=DEL;END=200\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[5] == ["<DEL>"]

    def test_parse_vcf_line_with_metadata_fields(self):
        """Test parsing VCF line with file metadata fields."""
        line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(
            line, sample_names, file_path="/path/to/file.vcf", file_name="file.vcf"
        )

        assert result[10] == "/path/to/file.vcf"
        assert result[11] == "file.vcf"

    def test_parse_vcf_line_with_primary_key(self):
        """Test parsing VCF line with primary key generation."""
        line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(
            line,
            sample_names,
            file_path="/path/to/file.vcf",
            file_name="file.vcf",
            generate_primary_key=True,
        )

        variant_id = result[12]
        assert variant_id is not None
        assert len(variant_id) == 64  # SHA256 hash

    def test_parse_vcf_line_exclude_all_samples(self):
        """Test parsing VCF line excluding all samples."""
        line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1\t1/1\t0/0"
        sample_names = ["Sample1", "Sample2", "Sample3"]

        result = parse_vcf_line(
            line, sample_names, exclude_samples=["Sample1", "Sample2", "Sample3"]
        )

        assert result[9] is None or len(result[9]) == 0

    def test_parse_vcf_line_include_nonexistent_sample(self):
        """Test parsing VCF line including a sample that doesn't exist."""
        line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(
            line, sample_names, include_samples=["NonexistentSample"]
        )

        assert result[9] is None or len(result[9]) == 0

    def test_parse_vcf_line_very_low_quality(self):
        """Test parsing VCF line with very low quality score."""
        line = "chr1\t100\trs001\tA\tT\t0.001\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[6] == 0.001

    def test_parse_vcf_line_very_high_quality(self):
        """Test parsing VCF line with very high quality score."""
        line = "chr1\t100\trs001\tA\tT\t9999.99\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[6] == 9999.99

    def test_parse_vcf_line_indel_insertion(self):
        """Test parsing VCF line representing an insertion."""
        line = "chr1\t100\trs001\tA\tATTT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[4] == "A"
        assert result[5] == ["ATTT"]

    def test_parse_vcf_line_indel_deletion(self):
        """Test parsing VCF line representing a deletion."""
        line = "chr1\t100\trs001\tATTT\tA\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result = parse_vcf_line(line, sample_names)
        assert result[4] == "ATTT"
        assert result[5] == ["A"]
        # end = start + len(ref), start = pos - 1 = 99
        assert result[2] == 99 + len("ATTT")  # = 103


class TestParserConsistency:
    """Tests for parser consistency and reproducibility."""

    def test_parse_vcf_line_multiple_times(self):
        """Test that parsing the same line multiple times gives same result."""
        line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
        sample_names = ["Sample1"]

        result1 = parse_vcf_line(line, sample_names)
        result2 = parse_vcf_line(line, sample_names)

        # Results should be structurally identical (compare key fields)
        # Note: VariantVal objects may not compare equal directly
        assert result1[0] == result2[0]  # contig
        assert result1[1] == result2[1]  # start
        assert result1[2] == result2[2]  # end
        assert result1[3] == result2[3]  # names
        assert result1[4] == result2[4]  # ref
        assert result1[5] == result2[5]  # alt

    def test_parse_header_multiple_times(self):
        """Test that parsing the same header multiple times gives same result."""
        lines = [
            "##fileformat=VCFv4.2",
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1",
        ]

        sample_names1, metadata1 = parse_header(lines)
        sample_names2, metadata2 = parse_header(lines)

        assert sample_names1 == sample_names2
        assert metadata1 == metadata2
