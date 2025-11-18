"""Unit tests for stream_reader module (VCFStreamReader)."""

import pytest
import os
import gzip
from vcf_reader.compat import DATASOURCE_API_AVAILABLE

# Skip all stream reader tests if DataSource API not available
pytestmark = pytest.mark.skipif(
    not DATASOURCE_API_AVAILABLE,
    reason="Stream reader tests require Databricks Runtime 15.4+ for DataSource API",
)

from vcf_reader.schema import get_vcf_schema
from vcf_reader.stream_reader import VCFStreamReader, VCFPartition


class TestVCFPartition:
    """Tests for VCFPartition class."""

    def test_partition_initialization(self):
        """Test creating a VCFPartition."""
        partition = VCFPartition("/path/to/file.vcf", 0, 1000, "file.vcf")
        assert partition.path == "/path/to/file.vcf"
        assert partition.start_offset == 0
        assert partition.end_offset == 1000
        assert partition.file_name == "file.vcf"


class TestVCFStreamReaderInitialization:
    """Tests for VCFStreamReader initialization."""

    def test_reader_initialization_basic(self):
        """Test basic VCFStreamReader initialization."""
        schema = get_vcf_schema()
        options = {"path": "/tmp/test.vcf"}
        reader = VCFStreamReader(schema, options)

        assert reader.schema == schema
        assert reader.options == options
        assert reader.path == "/tmp/test.vcf"

    def test_reader_parse_options(self):
        """Test parsing various options."""
        schema = get_vcf_schema()
        options = {
            "path": "/tmp/test.vcf",
            "includeSampleIds": "Sample1, Sample2",
            "excludeSampleIds": "Sample3",
            "includeFileMetadata": "false",
            "generatePrimaryKey": "true",
        }
        reader = VCFStreamReader(schema, options)

        assert reader.include_samples == ["Sample1", "Sample2"]
        assert reader.exclude_samples == ["Sample3"]
        assert reader.include_metadata is False
        assert reader.generate_primary_key is True

    def test_reader_default_chunk_size(self):
        """Test that reader has a default chunk size."""
        assert VCFStreamReader.CHUNK_SIZE == 64 * 1024 * 1024


class TestVCFStreamReaderFileDiscovery:
    """Tests for file discovery in streaming context."""

    def test_discover_single_file(self, tmp_path):
        """Test discovering a single VCF file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        assert len(reader.vcf_files) == 1
        assert reader.vcf_files[0] == str(vcf_file)

    def test_discover_multiple_files(self, tmp_path):
        """Test discovering multiple VCF files."""
        (tmp_path / "test1.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test2.vcf").write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFStreamReader(schema, options)

        assert len(reader.vcf_files) == 2


class TestVCFStreamReaderHeaderInfo:
    """Tests for header information caching."""

    def test_get_header_info_plain_file(self, tmp_path):
        """Test getting header info from plain VCF file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=chr1,length=248956422>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1	0/0
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        sample_names, header_size = reader._get_header_info(str(vcf_file))

        assert sample_names == ["Sample1", "Sample2"]
        assert header_size > 0

    def test_get_header_info_gzipped_file(self, tmp_path):
        """Test getting header info from gzipped VCF file."""
        vcf_file = tmp_path / "test.vcf.gz"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
"""
        with gzip.open(vcf_file, "wt") as f:
            f.write(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        sample_names, header_size = reader._get_header_info(str(vcf_file))

        assert sample_names == ["Sample1"]
        assert header_size > 0

    def test_get_header_info_caching(self, tmp_path):
        """Test that header info is cached."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        # First call
        sample_names1, header_size1 = reader._get_header_info(str(vcf_file))

        # Second call (should use cache)
        sample_names2, header_size2 = reader._get_header_info(str(vcf_file))

        assert sample_names1 == sample_names2
        assert header_size1 == header_size2

        # Check cache was actually used
        assert str(vcf_file) in reader._sample_names_cache
        assert str(vcf_file) in reader._header_size_cache

    def test_get_header_info_no_samples(self, tmp_path):
        """Test getting header info from VCF with no samples."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        sample_names, header_size = reader._get_header_info(str(vcf_file))

        assert sample_names == []
        assert header_size > 0


class TestVCFStreamReaderOffsets:
    """Tests for offset tracking."""

    def test_initial_offset_single_file(self, tmp_path):
        """Test initial offset for single file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        offset = reader.initialOffset()

        assert "offset" in offset
        assert offset["offset"] > 0  # Should be past the header

    def test_initial_offset_multiple_files(self, tmp_path):
        """Test initial offset for multiple files."""
        (tmp_path / "test1.vcf").write_text(
            "##fileformat=VCFv4.2\n#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"
        )
        (tmp_path / "test2.vcf").write_text(
            "##fileformat=VCFv4.2\n#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"
        )

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFStreamReader(schema, options)

        offset = reader.initialOffset()

        assert "file_index" in offset
        assert "offset" in offset
        assert offset["file_index"] == 0

    def test_latest_offset_single_file(self, tmp_path):
        """Test latest offset for single file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        offset = reader.latestOffset()

        assert "offset" in offset
        assert offset["offset"] == os.path.getsize(str(vcf_file))

    def test_latest_offset_multiple_files(self, tmp_path):
        """Test latest offset for multiple files."""
        (tmp_path / "test1.vcf").write_text("##fileformat=VCFv4.2\n")
        (tmp_path / "test2.vcf").write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFStreamReader(schema, options)

        offset = reader.latestOffset()

        assert "file_index" in offset
        assert offset["file_index"] == len(reader.vcf_files) - 1


class TestVCFStreamReaderPartitions:
    """Tests for partition creation in streaming context."""

    def test_partitions_single_file_small(self, tmp_path):
        """Test partitions for a small single file."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        start = reader.initialOffset()
        end = reader.latestOffset()
        partitions = reader.partitions(start, end)

        # Small file should have one partition
        assert len(partitions) >= 1
        assert all(isinstance(p, VCFPartition) for p in partitions)

    def test_partitions_multiple_files(self, tmp_path):
        """Test partitions for multiple files."""
        vcf1 = tmp_path / "test1.vcf"
        vcf2 = tmp_path / "test2.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
        vcf1.write_text(vcf_content)
        vcf2.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(tmp_path)}
        reader = VCFStreamReader(schema, options)

        start = reader.initialOffset()
        end = reader.latestOffset()
        partitions = reader.partitions(start, end)

        # Should have at least one partition per file
        assert len(partitions) >= 2

    def test_partitions_empty_range(self, tmp_path):
        """Test partitions with empty range (start == end)."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        # Same start and end
        start = {"path": str(vcf_file), "offset": 100}
        end = {"path": str(vcf_file), "offset": 100}
        partitions = reader.partitions(start, end)

        assert len(partitions) == 0


class TestVCFStreamReaderRead:
    """Tests for reading data in streaming mode."""

    def test_read_plain_partition(self, tmp_path):
        """Test reading from a plain text VCF partition."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
chr1	200	rs002	G	C	40.0	PASS	DP=20	GT	1/1
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        # Get header size
        _, header_size = reader._get_header_info(str(vcf_file))
        file_size = os.path.getsize(str(vcf_file))

        # Create partition covering the data
        partition = VCFPartition(str(vcf_file), header_size, file_size, "test.vcf")

        rows = list(reader.read(partition))
        assert len(rows) == 2

    def test_read_gzipped_partition(self, tmp_path):
        """Test reading from a gzipped VCF partition."""
        vcf_file = tmp_path / "test.vcf.gz"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
"""
        with gzip.open(vcf_file, "wt") as f:
            f.write(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        # For gzipped files, offsets are line numbers
        _, header_size = reader._get_header_info(str(vcf_file))

        partition = VCFPartition(
            str(vcf_file), header_size, header_size + 100, "test.vcf.gz"
        )

        rows = list(reader.read(partition))
        assert len(rows) >= 1

    def test_read_with_sample_filtering(self, tmp_path):
        """Test reading with sample filtering in streaming mode."""
        vcf_file = tmp_path / "test.vcf"
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2	Sample3
chr1	100	rs001	A	T	30.0	PASS	DP=42	GT	0/1	0/0	1/1
"""
        vcf_file.write_text(vcf_content)

        schema = get_vcf_schema()
        options = {"path": str(vcf_file), "includeSampleIds": "Sample1, Sample3"}
        reader = VCFStreamReader(schema, options)

        _, header_size = reader._get_header_info(str(vcf_file))
        file_size = os.path.getsize(str(vcf_file))
        partition = VCFPartition(str(vcf_file), header_size, file_size, "test.vcf")

        rows = list(reader.read(partition))

        # Check genotype filtering
        for row in rows:
            genotypes = row[9]
            if genotypes:
                assert len(genotypes) == 2
                assert genotypes[0]["sampleId"] == "Sample1"
                assert genotypes[1]["sampleId"] == "Sample3"

    def test_commit_no_op(self, tmp_path):
        """Test that commit does nothing (no-op)."""
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text("##fileformat=VCFv4.2\n")

        schema = get_vcf_schema()
        options = {"path": str(vcf_file)}
        reader = VCFStreamReader(schema, options)

        # commit should not raise any errors
        reader.commit({"path": str(vcf_file), "offset": 100})
