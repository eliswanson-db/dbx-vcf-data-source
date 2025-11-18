"""Unit tests for reader module (VCFBatchReader)."""

import pytest
import os
import tempfile
from vcf_reader.compat import DATASOURCE_API_AVAILABLE

# Skip all reader tests if DataSource API not available
pytestmark = pytest.mark.skipif(
    not DATASOURCE_API_AVAILABLE,
    reason="Reader tests require Databricks Runtime 15.4+ for DataSource API",
)

from vcf_reader.schema import get_vcf_schema
from vcf_reader.reader import VCFBatchReader, VCFFilePartition


class TestVCFFilePartition:
    """Tests for VCFFilePartition class."""

    def test_partition_initialization(self):
        """Test creating a VCFFilePartition."""
        partition = VCFFilePartition("/path/to/file.vcf", "file.vcf")
        assert partition.file_path == "/path/to/file.vcf"
        assert partition.file_name == "file.vcf"


class TestVCFBatchReaderInitialization:
    """Tests for VCFBatchReader initialization and option parsing."""

    def test_reader_initialization_basic(self):
        """Test basic VCFBatchReader initialization."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf"}
        reader = VCFBatchReader(schema, options)

        assert reader.schema == schema
        assert reader.options == options
        assert reader.path == "/tmp/test.vcf"

    def test_reader_parse_include_samples_single(self):
        """Test parsing includeSampleIds option with single sample."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "includeSampleIds": "Sample1"}
        reader = VCFBatchReader(schema, options)

        assert reader.include_samples == ["Sample1"]

    def test_reader_parse_include_samples_multiple(self):
        """Test parsing includeSampleIds option with multiple samples."""
        schema = get_vcf_schema()
        options = {
            "path": "/tmp/test.vcf",
            "includeSampleIds": "Sample1, Sample2, Sample3",
        }
        reader = VCFBatchReader(schema, options)

        assert reader.include_samples == ["Sample1", "Sample2", "Sample3"]

    def test_reader_parse_exclude_samples(self):
        """Test parsing excludeSampleIds option."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "excludeSampleIds": "Sample1, Sample2"}
        reader = VCFBatchReader(schema, options)

        assert reader.exclude_samples == ["Sample1", "Sample2"]

    def test_reader_parse_include_metadata_true(self):
        """Test parsing includeFileMetadata option as true."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "includeFileMetadata": "true"}
        reader = VCFBatchReader(schema, options)

        assert reader.include_metadata is True

    def test_reader_parse_include_metadata_false(self):
        """Test parsing includeFileMetadata option as false."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "includeFileMetadata": "false"}
        reader = VCFBatchReader(schema, options)

        assert reader.include_metadata is False

    def test_reader_parse_generate_primary_key_true(self):
        """Test parsing generatePrimaryKey option as true."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "generatePrimaryKey": "true"}
        reader = VCFBatchReader(schema, options)

        assert reader.generate_primary_key is True

    def test_reader_parse_generate_primary_key_false(self):
        """Test parsing generatePrimaryKey option as false."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf", "generatePrimaryKey": "false"}
        reader = VCFBatchReader(schema, options)

        assert reader.generate_primary_key is False

    def test_reader_default_options(self):
        """Test reader with default options."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf"}
        reader = VCFBatchReader(schema, options)

        assert reader.include_samples is None
        assert reader.exclude_samples is None
        assert reader.include_metadata is True
        assert reader.generate_primary_key is False


class TestVCFBatchReaderFileDiscovery:
    """Tests for VCF file discovery."""

    def test_discover_single_vcf_file(self, tmp_path):
        """Test discovering a single VCF file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 1
        assert reader.vcf_files[0] == str(vcf_file)

    def test_discover_gzipped_vcf_file(self, tmp_path):
        """Test discovering a gzipped VCF file."""
        import gzip

        vcf_file = tmp_path / "test.vcf.gz"
        with gzip.open(vcf_file, "wt") as f:
            f.write("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 1
        assert reader.vcf_files[0] == str(vcf_file)

    def test_discover_multiple_vcf_files_in_directory(self, tmp_path):
        """Test discovering multiple VCF files in a directory."""
        (tmp_path / "test1.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test2.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test3.vcf.gz").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "other.txt").write_text("not a vcf")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 3
        # Should be sorted
        assert all("test" in f for f in reader.vcf_files)

    def test_discover_vcf_files_nested_directories(self, tmp_path):
        """Test discovering VCF files in nested directories."""
        subdir1 = tmp_path / "subdir1"
        subdir2 = tmp_path / "subdir2"
        subdir1.mkdir()
        subdir2.mkdir()

        (subdir1 / "test1.vcf").write_text("##fileformat=VCFv4.2\n")
        (subdir2 / "test2.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test3.vcf").write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 3

    def test_discover_no_vcf_files(self, tmp_path):
        """Test discovering with no VCF files present."""
        (tmp_path / "test.txt").write_text("not a vcf")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 0

    def test_discover_nonexistent_path(self):
        """Test discovering with nonexistent path."""
        schema = get_vcf_schema()
        options = {"path": "/nonexistent/path"}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 0


class TestVCFBatchReaderPartitions:
    """Tests for partition creation."""

    def test_partitions_single_file(self, tmp_path):
        """Test creating partitions for a single file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        assert len(partitions) == 1
        assert partitions[0].file_path == str(vcf_file)
        assert partitions[0].file_name == "test.vcf"

    def test_partitions_multiple_files(self, tmp_path):
        """Test creating partitions for multiple files."""
        (tmp_path / "test1.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test2.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test3.vcf").write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        assert len(partitions) == 3

        # Each partition should correspond to one file
        file_names = [p.file_name for p in partitions]
        assert "test1.vcf" in file_names
        assert "test2.vcf" in file_names
        assert "test3.vcf" in file_names


class TestVCFBatchReaderRead:
    """Tests for reading VCF data."""

    def test_read_single_variant(self, sample_vcf_single):
        """Test reading a VCF file with variants."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_single}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        assert len(partitions) == 1

        rows = list(reader.read(partitions[0]))
        assert len(rows) > 0

        # Check first row structure
        row = rows[0]
        assert len(row) == 13  # Should have 13 fields
        assert isinstance(row[0], str)  # contig
        assert isinstance(row[1], int)  # start
        assert isinstance(row[2], int)  # end

    def test_read_with_sample_filtering(self, sample_vcf_multi):
        """Test reading with sample filtering."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_multi, "includeSampleIds": "Sample1"}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Check that genotypes are filtered
        for row in rows:
            genotypes = row[9]
            if genotypes:
                assert len(genotypes) == 1
                assert genotypes[0]["sampleId"] == "Sample1"

    def test_read_with_metadata(self, sample_vcf_single):
        """Test reading with file metadata included."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_single, "includeFileMetadata": "true"}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        row = rows[0]
        file_path = row[10]
        file_name = row[11]

        assert file_path == sample_vcf_single
        assert file_name == os.path.basename(sample_vcf_single)

    def test_read_without_metadata(self, sample_vcf_single):
        """Test reading without file metadata."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_single, "includeFileMetadata": "false"}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        row = rows[0]
        file_path = row[10]
        file_name = row[11]

        assert file_path == ""
        assert file_name == ""

    def test_read_with_primary_key_generation(self, sample_vcf_single):
        """Test reading with primary key generation."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_single, "generatePrimaryKey": "true"}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        row = rows[0]
        variant_id = row[12]

        assert variant_id is not None
        assert len(variant_id) == 64  # SHA256 hash length

    def test_read_gzipped_file(self, sample_vcf_gzipped):
        """Test reading gzipped VCF file."""
        schema = get_vcf_schema()
        options = {"path": sample_vcf_gzipped}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) > 0

    def test_read_skips_malformed_lines(self, tmp_path):
        """Test that malformed lines are skipped."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
chr1	200
chr1	300	rs003	G	C	40.0	PASS	DP=20	GT	0/0
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Should have 2 valid rows (malformed line skipped)
        assert len(rows) == 2

    def test_read_empty_file(self, tmp_path):
        """Test reading an empty VCF file."""
        vcf_file = tmp_path / "empty.vcf"
        vcf_file.write_text("")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFBatchReader(schema, options)

        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) == 0
