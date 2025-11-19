# VCF Spark Reader

A PySpark custom data source for reading VCF (Variant Call Format) files with both batch and streaming support.

**Note**: This data source uses the PySpark DataSource API which is only available in **Databricks Runtime 15.4 LTS and above**. It will not work with open-source Apache Spark.

## Features

- **Batch and Streaming Support**: Read VCF files in both batch and streaming modes
- **Apache Arrow Performance**: Uses PyArrow for high-performance batch processing of large files (1GB+)
- **Memory-Efficient Streaming**: Processes large files in 64MB chunks to avoid memory issues
- **Directory Support**: Read single files or entire directories (recursively finds all .vcf and .vcf.gz files)
- **File Metadata**: Automatically includes file_path and file_name columns
- **Compound Primary Key**: Optional SHA256 hash generation for unique variant identification
- **Bronze Layer Design**: Preserves raw data while providing structured schema
- **Multi-Sample Support**: Handles both single and multi-sample VCF files
- **Sample Filtering**: Include or exclude specific samples
- **Compressed Files**: Supports gzip-compressed VCF files (.vcf.gz)
- **Variant Type Support**: Uses PySpark VariantType for flexible INFO and FORMAT fields

## Requirements

- **Databricks Runtime 15.4 LTS or above** (for the PySpark DataSource API)
- Python >= 3.9
- PySpark >= 3.5.0 (included in Databricks Runtime - **not** installed as a package dependency)

## Installation

### In Databricks

Install directly from PyPI:

```python
%pip install vcf-spark-reader
```

Or install from source:

```bash
git clone <repository-url>
cd spark-ls-readers
poetry build
# Upload the wheel file to Databricks and install
```

### Local Development

```bash
git clone <repository-url>
cd spark-ls-readers
poetry install  # Installs PySpark as dev dependency for testing
```

**Note**: 
- The package has **no runtime dependencies** - it relies on PySpark being available in the Databricks Runtime
- PySpark is only installed as a dev dependency for local testing
- Unit tests use a compatibility layer to mock Databricks-specific types

## Quick Start

### Batch Reading - Single File

```python
from pyspark.sql import SparkSession
from vcf_reader import VCFDataSource

# Create Spark session
spark = SparkSession.builder.appName("vcf-example").getOrCreate()

# Register the data source
spark.dataSource.register(VCFDataSource)

# Read single VCF file
df = spark.read.format("vcf").load("path/to/file.vcf")

# Show results
df.show()
```

### Batch Reading - Directory

```python
# Read all VCF files in a directory (recursive)
df_dir = spark.read.format("vcf").load("path/to/vcf_directory")

# File metadata is included by default
df_dir.select("file_name", "file_path", "contig", "start").show()

# Count variants per file
df_dir.groupBy("file_name").count().show()
```

### Generate Compound Primary Key

```python
# Generate SHA256 hash as variant_id
df_with_pk = spark.read.format("vcf") \
    .option("generatePrimaryKey", "true") \
    .load("path/to/vcf_directory")

# variant_id is a SHA256 hash of: file_path|file_name|contig|start|end
df_with_pk.select("variant_id", "file_name", "contig", "start", "end").show()
```

### Streaming Reading

```python
# Read VCF file(s) as a stream (works with directories too)
stream_df = spark.readStream.format("vcf").load("path/to/file.vcf")

# Write to console
query = stream_df.writeStream.format("console").start()
query.awaitTermination()
```

## Schema

The VCF data source provides the following schema:

| Field | Type | Description |
|-------|------|-------------|
| `contig` | string | Chromosome/contig name (CHROM) |
| `start` | long | 0-indexed start position (POS - 1) |
| `end` | long | End position (start + len(REF)) |
| `names` | array\<string\> | Variant IDs (ID field, semicolon-split) |
| `referenceAllele` | string | Reference allele (REF) |
| `alternateAlleles` | array\<string\> | Alternate alleles (ALT, comma-split) |
| `qual` | double | Quality score (QUAL) |
| `filters` | array\<string\> | Filter values (FILTER, semicolon-split) |
| `info` | variant | INFO field as structured variant |
| `genotypes` | array\<struct\> | Genotype information per sample |
| `file_path` | string | Full path to the source VCF file |
| `file_name` | string | Name of the source VCF file |
| `variant_id` | string | SHA256 hash (if generatePrimaryKey=true) |

### Genotype Structure

Each genotype in the `genotypes` array has:

- `sampleId`: string - Sample identifier
- `calls`: array\<long\> - Genotype calls (e.g., [0, 1] for 0/1)
- `data`: variant - FORMAT fields as structured variant

## Options

### Sample Filtering

```python
# Include only specific samples
df = spark.read.format("vcf") \
    .option("includeSampleIds", "Sample1,Sample2") \
    .load("file.vcf")

# Exclude specific samples
df = spark.read.format("vcf") \
    .option("excludeSampleIds", "Sample3") \
    .load("file.vcf")

# Generate compound primary key (SHA256 hash)
df = spark.read.format("vcf") \
    .option("generatePrimaryKey", "true") \
    .load("file.vcf")

# Exclude file metadata columns
df = spark.read.format("vcf") \
    .option("includeFileMetadata", "false") \
    .load("file.vcf")
```

### Available Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `includeSampleIds` | string | None | Comma-separated list of sample IDs to include |
| `excludeSampleIds` | string | None | Comma-separated list of sample IDs to exclude |
| `includeFileMetadata` | boolean | true | Include file_path and file_name columns |
| `generatePrimaryKey` | boolean | false | Generate variant_id as SHA256 hash of file_path\|file_name\|contig\|start\|end |
| `useArrow` | boolean | true | Use Apache Arrow for fast batch processing (recommended for large files) |
| `batchSize` | integer | 10000 | Number of rows per batch when using Arrow |
| `streamChunkSize` | integer | 67108864 | Chunk size in bytes for streaming large files (64MB default) |

## Performance

### Apache Arrow Acceleration

By default, the VCF reader uses Apache Arrow for high-performance batch processing:

```python
# Arrow is enabled by default
df = spark.read.format("vcf").load("large_file.vcf")

# Disable Arrow if needed (falls back to line-by-line)
df = spark.read.format("vcf") \
    .option("useArrow", "false") \
    .load("file.vcf")

# Tune chunk size for very large files (default 64MB)
df = spark.read.format("vcf") \
    .option("streamChunkSize", "134217728") \  # 128MB chunks
    .load("huge_file.vcf")
```

**Memory Usage:**
- With Arrow streaming: ~64MB per file regardless of file size
- Without Arrow: Memory usage scales with file size

**Performance:**
- Arrow is significantly faster than line-by-line parsing for large files (1GB+)
- Gzip-compressed files are automatically decompressed on the fly
- Arrow handles chunk boundaries correctly to avoid data loss

### Future Options (Stubbed)

The following options are planned for future releases:

- `flattenInfoFields`: Flatten INFO fields into separate columns
- `splitMultiAllelics`: Split multi-allelic variants into separate rows
- `includeHeader`: Include VCF header metadata

## Examples

### Query Variants in a Region

```python
df = spark.read.format("vcf").load("file.vcf")

# Filter by genomic region
region_df = df.filter(
    (df.contig == "chr1") & 
    (df.start >= 1000000) & 
    (df.end <= 2000000)
)

region_df.show()
```

### Extract INFO Fields

```python
# Access INFO fields using variant functions
from pyspark.sql.functions import col

df.select(
    "contig",
    "start",
    col("info:DP").alias("depth"),
    col("info:AF").alias("allele_frequency")
).show()
```

### Work with Genotypes

```python
from pyspark.sql.functions import explode

# Explode genotypes to one row per sample
genotypes_df = df.select(
    "contig",
    "start",
    "referenceAllele",
    "alternateAlleles",
    explode("genotypes").alias("genotype")
)

genotypes_df.select(
    "contig",
    "start",
    col("genotype.sampleId"),
    col("genotype.calls")
).show()
```

## Comparison with Project Glow

This reader is inspired by [Project Glow](https://projectglow.io/), a Scala-based genomics library for Spark. Key differences:

- **Python-Native**: Written entirely in Python using PySpark DataSource API
- **Bronze Layer Focus**: Preserves raw data with minimal transformation
- **Variant Types**: Uses PySpark VariantType for flexible schema
- **Simpler**: Focused on reading VCF files, not full genomics pipeline

For production genomics workflows, consider using Project Glow which provides:
- More comprehensive VCF parsing options
- Variant normalization and quality control
- Integration with other genomics tools
- Optimized performance for large-scale genomics

## Development

### Running Tests

```bash
# Install dev dependencies
poetry install

# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=vcf_reader --cov-report=html
```

### Building Package

```bash
# Build wheel
poetry build

# Install locally
pip install dist/vcf_spark_reader-*.whl
```

## VCF Format Reference

For more information about the VCF format, see:
- [VCF Specification](https://samtools.github.io/hts-specs/VCFv4.2.pdf)
- [VCF Wikipedia](https://en.wikipedia.org/wiki/Variant_Call_Format)

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please submit issues and pull requests.

