"""Tests for Arrow-based VCF reader."""

import pytest
import os

try:
    from vcf_reader.arrow_reader import (
        ArrowVCFReader,
        read_vcf_with_arrow,
        ARROW_AVAILABLE,
    )

    pytestmark = pytest.mark.skipif(
        not ARROW_AVAILABLE,
        reason="Arrow tests require pyarrow",
    )
except ImportError:
    ARROW_AVAILABLE = False
    pytestmark = pytest.mark.skip(reason="Arrow reader not available")


class TestArrowVCFReader:
    """Tests for Arrow-based VCF reader."""

    def test_arrow_reader_initialization(self):
        """Test creating an Arrow VCF reader."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(file_path="/tmp/test.vcf", file_name="test.vcf")
        assert reader.file_path == "/tmp/test.vcf"
        assert reader.file_name == "test.vcf"
        assert reader.batch_size == ArrowVCFReader.DEFAULT_BATCH_SIZE

    def test_arrow_reader_with_options(self):
        """Test Arrow reader with custom options."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path="/tmp/test.vcf",
            file_name="test.vcf",
            include_samples=["Sample1"],
            batch_size=5000,
        )
        assert reader.include_samples == ["Sample1"]
        assert reader.batch_size == 5000

    def test_arrow_reader_read_single_sample(self, sample_vcf_single):
        """Test reading VCF file with Arrow reader."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path=sample_vcf_single, file_name=os.path.basename(sample_vcf_single)
        )

        rows = list(reader.read_batched())
        assert len(rows) > 0

        # Check first row structure
        row = rows[0]
        assert len(row) == 13  # Should have 13 fields
        assert isinstance(row[0], str)  # contig
        assert isinstance(row[1], int)  # start

    def test_arrow_reader_read_multi_sample(self, sample_vcf_multi):
        """Test reading multi-sample VCF with Arrow."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path=sample_vcf_multi, file_name=os.path.basename(sample_vcf_multi)
        )

        rows = list(reader.read_batched())
        assert len(rows) > 0

        # Check genotypes
        for row in rows:
            genotypes = row[9]
            if genotypes:
                assert len(genotypes) >= 1

    def test_arrow_reader_with_sample_filter(self, sample_vcf_multi):
        """Test Arrow reader with sample filtering."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path=sample_vcf_multi,
            file_name=os.path.basename(sample_vcf_multi),
            include_samples=["Sample1"],
        )

        rows = list(reader.read_batched())

        for row in rows:
            genotypes = row[9]
            if genotypes:
                assert len(genotypes) == 1
                assert genotypes[0]["sampleId"] == "Sample1"

    def test_arrow_reader_gzipped(self, sample_vcf_gzipped):
        """Test reading gzipped VCF with Arrow."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path=sample_vcf_gzipped, file_name=os.path.basename(sample_vcf_gzipped)
        )

        rows = list(reader.read_batched())
        assert len(rows) > 0

    def test_read_vcf_with_arrow_convenience_function(self, sample_vcf_single):
        """Test convenience function for Arrow reading."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        rows = list(
            read_vcf_with_arrow(
                file_path=sample_vcf_single,
                file_name=os.path.basename(sample_vcf_single),
            )
        )

        assert len(rows) > 0

    def test_arrow_reader_small_batch_size(self, sample_vcf_single):
        """Test Arrow reader with small batch size."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        reader = ArrowVCFReader(
            file_path=sample_vcf_single,
            file_name=os.path.basename(sample_vcf_single),
            batch_size=2,
        )

        rows = list(reader.read_batched())
        assert len(rows) > 0

    def test_arrow_reader_streaming_chunks(self, tmp_path):
        """Test Arrow reader with small chunk size to force streaming."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        # Create a VCF file with enough variants to span multiple chunks
        vcf_file = tmp_path / "large.vcf"
        with open(vcf_file, "w", encoding="utf-8") as f:
            # Write header
            f.write("##fileformat=VCFv4.2\n")
            f.write("##contig=<ID=chr1,length=248956422>\n")
            f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1\n")

            # Write 100 variants
            for i in range(100):
                pos = 1000 + (i * 100)
                f.write(f"chr1\t{pos}\t.\tA\tT\t30\tPASS\tDP=50\tGT:DP\t0/1:{50+i}\n")

        # Read with small chunk size to force multiple chunks (1KB chunks)
        reader = ArrowVCFReader(
            file_path=str(vcf_file),
            file_name="large.vcf",
            stream_chunk_size=1024,  # 1KB to force chunking
        )

        rows = list(reader.read_batched())
        assert len(rows) == 100, f"Expected 100 rows, got {len(rows)}"

        # Verify data integrity across chunk boundaries
        # Note: row[1] is 0-based start position, VCF POS is 1-based
        for i, row in enumerate(rows):
            expected_start = 999 + (i * 100)  # 0-based start = VCF POS - 1
            assert (
                row[1] == expected_start
            ), f"Row {i}: expected start {expected_start}, got {row[1]}"

    def test_arrow_reader_streaming_chunks_gzipped(self, tmp_path):
        """Test Arrow reader streaming with gzipped files."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")

        import gzip

        # Create a gzipped VCF file
        vcf_file = tmp_path / "large.vcf.gz"
        with gzip.open(vcf_file, "wt", encoding="utf-8") as f:
            # Write header
            f.write("##fileformat=VCFv4.2\n")
            f.write("##contig=<ID=chr1,length=248956422>\n")
            f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1\n")

            # Write 50 variants
            for i in range(50):
                pos = 1000 + (i * 100)
                f.write(f"chr1\t{pos}\t.\tA\tT\t30\tPASS\tDP=50\tGT:DP\t0/1:{50+i}\n")

        # Read with small chunk size
        reader = ArrowVCFReader(
            file_path=str(vcf_file),
            file_name="large.vcf.gz",
            stream_chunk_size=1024,  # 1KB chunks
        )

        rows = list(reader.read_batched())
        assert len(rows) == 50, f"Expected 50 rows, got {len(rows)}"
