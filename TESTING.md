# Testing Guide

## Overview

The VCF Spark Reader uses a two-tier testing approach:

1. **Unit Tests** - Test core parsing logic without requiring Databricks
2. **Integration Tests** - Test full DataSource functionality in Databricks Runtime

## Running Tests Locally

### Prerequisites

```bash
poetry install
```

### Unit Tests

Unit tests for the parser module can run locally:

```bash
# Run all unit tests
poetry run pytest tests/test_parser_unit.py -v

# Run with coverage
poetry run pytest tests/test_parser_unit.py --cov=vcf_reader --cov-report=html

# Run specific test
poetry run pytest tests/test_parser_unit.py::test_parse_vcf_line_basic_structure -v
```

### Coverage Report

Current coverage (parser module only):

```
Name                     Stmts   Miss  Cover
--------------------------------------------
vcf_reader/parser.py       97     13    87%
```

The parser module has 87% test coverage. Untested lines are primarily error handling paths that require malformed VCF files.

## Running Tests in Databricks

### Setup

1. Upload the project to Databricks workspace or DBFS
2. Create a notebook in Databricks Runtime 15.4+
3. Install the package:

```python
%pip install /path/to/vcf-spark-reader
# OR install from local directory
%pip install -e /Workspace/path/to/spark-ls-readers
```

### Integration Tests

#### Option 1: Run pytest

```python
%pip install pytest
```

```python
import pytest
pytest.main(["/Workspace/path/to/spark-ls-readers/tests/test_databricks_integration.py", "-v"])
```

#### Option 2: Copy test code to notebook

See `tests/test_databricks_integration.py` for complete test suite. You can copy individual test functions into notebook cells.

#### Option 3: Use example notebook

Run the examples in `examples/databricks_example.py` to manually verify functionality.

## Test Structure

### Unit Tests (`test_parser_unit.py`)

Tests the VCF parsing logic:
- Header parsing
- VCF line parsing
- INFO field parsing
- Genotype parsing
- Sample filtering
- Missing value handling
- Multi-allelic variants
- Edge cases

These tests use a compatibility layer (`compat.py`) that mocks Databricks-specific types (`VariantVal`, `VariantType`) to enable local testing.

### Integration Tests (`test_databricks_integration.py`)

Tests the full DataSource implementation:
- Batch reading
- Streaming reading
- Schema validation
- Sample filtering
- SQL queries
- INFO field access
- Genotype operations

**These tests require Databricks Runtime 15.4+ and will be skipped locally.**

## Continuous Integration

For CI/CD pipelines:

```bash
# Run local tests only
poetry run pytest tests/test_parser_unit.py -v

# Skip integration tests
poetry run pytest -v -m "not integration"
```

Integration tests should be run in Databricks as part of deployment validation.

## Writing New Tests

### For Parser Logic

Add tests to `test_parser_unit.py`:

```python
def test_new_parser_feature():
    """Test description."""
    line = "chr1\t100\trs001\tA\tT\t30.0\tPASS\tDP=14\tGT\t0/1"
    result = parse_vcf_line(line, ["Sample1"])
    assert result[0] == "chr1"
```

### For DataSource Features

Add tests to `test_databricks_integration.py`:

```python
def test_new_datasource_feature(spark):
    """Test description."""
    spark.dataSource.register(VCFDataSource)
    df = spark.read.format("vcf").load("test.vcf")
    # Your assertions here
```

## Test Data

Test VCF files are generated in fixtures (see `conftest.py`). To add new test data:

```python
@pytest.fixture
def sample_vcf_custom(tmp_path):
    """Create custom VCF for testing."""
    vcf_content = """##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	100	rs001	A	T	30.0	PASS	DP=14
"""
    vcf_path = tmp_path / "custom.vcf"
    vcf_path.write_text(vcf_content)
    return str(vcf_path)
```

## Troubleshooting

### Import Errors Locally

If you see `ModuleNotFoundError: No module named 'pyspark.sql.datasource'`:
- This is expected - the DataSource API is Databricks-only
- Unit tests should still work via the compatibility layer
- Integration tests will be automatically skipped

### Tests Fail in Databricks

1. Verify Databricks Runtime version (must be 15.4+)
2. Check that package is installed correctly: `%pip list | grep vcf`
3. Restart Python kernel: `dbutils.library.restartPython()`
4. Verify VCF file paths are accessible

### Variant Type Issues

If you see errors with VariantVal or VariantType:
- In Databricks: These should be available automatically
- Locally: The compat layer should provide mocks
- Check that imports use `from vcf_reader.compat import VariantVal`

