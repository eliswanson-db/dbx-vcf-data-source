"""Tests for Arrow-based VCF reader."""

import pytest
import os

try:
    from vcf_reader.arrow_reader import ArrowVCFReader, read_vcf_with_arrow, ARROW_AVAILABLE
    
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
        
        reader = ArrowVCFReader(
            file_path="/tmp/test.vcf",
            file_name="test.vcf"
        )
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
            file_path=sample_vcf_single,
            file_name=os.path.basename(sample_vcf_single)
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
            file_path=sample_vcf_multi,
            file_name=os.path.basename(sample_vcf_multi)
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
            include_samples=["Sample1"]
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
            file_path=sample_vcf_gzipped,
            file_name=os.path.basename(sample_vcf_gzipped)
        )
        
        rows = list(reader.read_batched())
        assert len(rows) > 0
    
    def test_read_vcf_with_arrow_convenience_function(self, sample_vcf_single):
        """Test convenience function for Arrow reading."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")
        
        rows = list(read_vcf_with_arrow(
            file_path=sample_vcf_single,
            file_name=os.path.basename(sample_vcf_single)
        ))
        
        assert len(rows) > 0
    
    def test_arrow_reader_small_batch_size(self, sample_vcf_single):
        """Test Arrow reader with small batch size."""
        if not ARROW_AVAILABLE:
            pytest.skip("PyArrow not available")
        
        reader = ArrowVCFReader(
            file_path=sample_vcf_single,
            file_name=os.path.basename(sample_vcf_single),
            batch_size=2
        )
        
        rows = list(reader.read_batched())
        assert len(rows) > 0

