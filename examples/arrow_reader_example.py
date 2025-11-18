"""
Example of using Arrow-based VCF reader for improved performance.

This example demonstrates the performance benefits of using Apache Arrow
for reading large VCF files.
"""

import time
from vcf_reader.arrow_reader import read_vcf_with_arrow, ARROW_AVAILABLE
from vcf_reader.reader import VCFBatchReader
from vcf_reader.schema import get_vcf_schema


def read_vcf_with_arrow_reader(vcf_path: str):
    """Read VCF using Arrow-based reader."""
    if not ARROW_AVAILABLE:
        print("PyArrow not available. Install with: pip install pyarrow")
        return None
    
    print(f"Reading {vcf_path} with Arrow reader...")
    start = time.time()
    
    rows = []
    for row in read_vcf_with_arrow(
        file_path=vcf_path,
        batch_size=10000
    ):
        rows.append(row)
    
    elapsed = time.time() - start
    print(f"Arrow reader: Read {len(rows)} variants in {elapsed:.2f} seconds")
    return rows, elapsed


def read_vcf_line_by_line(vcf_path: str):
    """Read VCF using traditional line-by-line reading."""
    from vcf_reader.parser import parse_header, parse_vcf_line
    import gzip
    
    print(f"Reading {vcf_path} with line-by-line reader...")
    start = time.time()
    
    is_gzipped = vcf_path.endswith(".gz")
    if is_gzipped:
        file_handle = gzip.open(vcf_path, "rt", encoding="utf-8")
    else:
        file_handle = open(vcf_path, "r", encoding="utf-8")
    
    rows = []
    try:
        # Read header
        header_lines = []
        for line in file_handle:
            if line.startswith("##"):
                header_lines.append(line)
            elif line.startswith("#CHROM"):
                header_lines.append(line)
                sample_names, _ = parse_header(header_lines)
                break
        
        # Read data
        for line in file_handle:
            if line.startswith("#") or not line.strip():
                continue
            try:
                row = parse_vcf_line(line, sample_names)
                rows.append(row)
            except (ValueError, IndexError):
                continue
    finally:
        file_handle.close()
    
    elapsed = time.time() - start
    print(f"Line-by-line reader: Read {len(rows)} variants in {elapsed:.2f} seconds")
    return rows, elapsed


def compare_readers(vcf_path: str):
    """Compare Arrow and line-by-line readers."""
    print("=" * 60)
    print("VCF Reader Performance Comparison")
    print("=" * 60)
    print()
    
    # Test Arrow reader
    arrow_rows, arrow_time = read_vcf_with_arrow_reader(vcf_path)
    print()
    
    # Test line-by-line reader
    line_rows, line_time = read_vcf_line_by_line(vcf_path)
    print()
    
    # Compare results
    if arrow_rows and line_rows:
        print("=" * 60)
        print("Results:")
        print(f"  Arrow reader: {len(arrow_rows)} variants in {arrow_time:.2f}s")
        print(f"  Line reader:  {len(line_rows)} variants in {line_time:.2f}s")
        print(f"  Speedup:      {line_time / arrow_time:.2f}x faster")
        print("=" * 60)
    
    return arrow_rows, line_rows


# Example usage with Spark DataSource
def read_with_spark_datasource(vcf_path: str, use_arrow: bool = True):
    """
    Example of using VCF DataSource with Arrow enabled.
    
    Note: This requires Databricks Runtime 15.4+
    """
    try:
        from pyspark.sql import SparkSession
        from vcf_reader import VCFDataSource
        
        spark = SparkSession.builder.getOrCreate()
        spark.dataSource.register(VCFDataSource)
        
        # Read with Arrow enabled (default)
        df = (
            spark.read
            .format("vcf")
            .option("useArrow", str(use_arrow).lower())
            .option("batchSize", "10000")
            .load(vcf_path)
        )
        
        count = df.count()
        print(f"Read {count} variants with Spark (Arrow={'enabled' if use_arrow else 'disabled'})")
        
        return df
    except ImportError:
        print("PySpark not available or DataSource API not supported")
        return None


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python arrow_reader_example.py <vcf_file>")
        print()
        print("Example:")
        print("  python arrow_reader_example.py /path/to/file.vcf")
        print("  python arrow_reader_example.py /path/to/file.vcf.gz")
        sys.exit(1)
    
    vcf_file = sys.argv[1]
    
    # Run comparison
    compare_readers(vcf_file)

