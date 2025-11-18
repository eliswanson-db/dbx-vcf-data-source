"""Test Arrow reader with directory of VCF files."""

import pytest
import os
from pyspark.sql.types import StructType

try:
    from vcf_reader.arrow_reader import ArrowVCFReader, ARROW_AVAILABLE
    from vcf_reader.reader import VCFBatchReader

    pytestmark = pytest.mark.skipif(
        not ARROW_AVAILABLE,
        reason="Arrow tests require pyarrow",
    )
except ImportError:
    ARROW_AVAILABLE = False
    pytestmark = pytest.mark.skip(reason="Arrow reader not available")


class TestArrowDirectoryReading:
    """Test reading directories of VCF files with Arrow."""

    def test_batch_reader_with_directory(self, tmp_path):
        """Test VCFBatchReader discovers and reads multiple files."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        # Create multiple VCF files in directory
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
chr1	100	rs001	A	T	30.0	PASS	DP=14	GT	0/1
chr1	200	rs002	G	C	40.0	PASS	DP=20	GT	1/1
"""

        (tmp_path / "file1.vcf").write_text(vcf_content)
        (tmp_path / "file2.vcf").write_text(vcf_content)
        (tmp_path / "file3.vcf").write_text(vcf_content)

        # Create reader with directory path
        # Use empty StructType - we're just testing file discovery
        schema = StructType([])
        options = {"path": str(tmp_path), "useArrow": "true"}
        reader = VCFBatchReader(schema, options)

        # Should discover 3 files
        assert len(reader.vcf_files) == 3

        # Create partitions (one per file)
        partitions = reader.partitions()
        assert len(partitions) == 3

        # Verify all partitions are valid
        for partition in partitions:
            assert partition is not None
            assert partition.file_path
            assert partition.file_name

    def test_batch_reader_with_nested_directories(self, tmp_path):
        """Test VCFBatchReader with nested directory structure."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""

        # Create nested structure
        (tmp_path / "subdir1").mkdir()
        (tmp_path / "subdir2").mkdir()

        (tmp_path / "file1.vcf").write_text(vcf_content)
        (tmp_path / "subdir1" / "file2.vcf").write_text(vcf_content)
        (tmp_path / "subdir2" / "file3.vcf").write_text(vcf_content)

        schema = StructType([])
        options = {"path": str(tmp_path), "useArrow": "true"}
        reader = VCFBatchReader(schema, options)

        # Should discover all 3 files
        assert len(reader.vcf_files) == 3

        partitions = reader.partitions()
        assert len(partitions) == 3

        # Verify partitions are valid
        for partition in partitions:
            assert partition is not None
            assert os.path.exists(partition.file_path)

    def test_batch_reader_with_mixed_file_types(self, tmp_path):
        """Test VCFBatchReader ignores non-VCF files."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""

        (tmp_path / "file1.vcf").write_text(vcf_content)
        (tmp_path / "file2.txt").write_text("not a vcf")
        (tmp_path / "file3.csv").write_text("also not a vcf")
        (tmp_path / "file4.vcf.gz").write_bytes(b"gzipped vcf")

        schema = StructType([])
        options = {"path": str(tmp_path), "useArrow": "true"}
        reader = VCFBatchReader(schema, options)

        # Should only discover .vcf and .vcf.gz files
        assert len(reader.vcf_files) == 2
        assert any("file1.vcf" in f for f in reader.vcf_files)
        assert any("file4.vcf.gz" in f for f in reader.vcf_files)

    def test_batch_reader_with_empty_directory(self, tmp_path):
        """Test VCFBatchReader with directory containing no VCF files."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        (tmp_path / "file1.txt").write_text("not a vcf")

        schema = StructType([])
        options = {"path": str(tmp_path), "useArrow": "true"}
        reader = VCFBatchReader(schema, options)

        # Should find no files
        assert len(reader.vcf_files) == 0

        partitions = reader.partitions()
        assert len(partitions) == 0

    def test_batch_reader_none_partition_handling(self):
        """Test that None partition is handled gracefully."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        schema = StructType([])
        options = {"path": "/tmp/test.vcf", "useArrow": "true"}
        reader = VCFBatchReader(schema, options)

        # Reading None partition should not crash
        rows = list(reader.read(None))
        assert len(rows) == 0

    def test_batch_reader_arrow_disabled_with_directory(self, tmp_path):
        """Test directory reading works with Arrow disabled."""
        vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""

        (tmp_path / "file1.vcf").write_text(vcf_content)
        (tmp_path / "file2.vcf").write_text(vcf_content)

        schema = StructType([])
        options = {"path": str(tmp_path), "useArrow": "false"}
        reader = VCFBatchReader(schema, options)

        assert len(reader.vcf_files) == 2

        partitions = reader.partitions()
        assert len(partitions) == 2

        # Verify partitions are valid
        for partition in partitions:
            assert partition is not None
