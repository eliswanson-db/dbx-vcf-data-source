"""Unit tests for schema module."""

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
)
from vcf_reader.compat import DATABRICKS_AVAILABLE

# Skip all schema tests if not in Databricks (VariantType not available)
pytestmark = pytest.mark.skipif(
    not DATABRICKS_AVAILABLE,
    reason="Schema tests require Databricks Runtime for VariantType",
)

from vcf_reader.schema import get_vcf_schema
from vcf_reader.compat import VariantType


def test_get_vcf_schema_returns_struct_type():
    """Test that get_vcf_schema returns a StructType."""
    schema = get_vcf_schema()
    assert isinstance(schema, StructType)


def test_get_vcf_schema_has_required_fields():
    """Test that schema contains all required fields."""
    schema = get_vcf_schema()
    field_names = [field.name for field in schema.fields]

    required_fields = [
        "contig",
        "start",
        "end",
        "names",
        "referenceAllele",
        "alternateAlleles",
        "qual",
        "filters",
        "info",
        "genotypes",
        "file_path",
        "file_name",
        "variant_id",
    ]

    for field in required_fields:
        assert field in field_names, f"Missing required field: {field}"


def test_get_vcf_schema_field_types():
    """Test that schema fields have correct types."""
    schema = get_vcf_schema()
    fields = {field.name: field for field in schema.fields}

    # Test scalar fields
    assert isinstance(fields["contig"].dataType, StringType)
    assert isinstance(fields["start"].dataType, LongType)
    assert isinstance(fields["end"].dataType, LongType)
    assert isinstance(fields["referenceAllele"].dataType, StringType)
    assert isinstance(fields["qual"].dataType, DoubleType)
    assert isinstance(fields["file_path"].dataType, StringType)
    assert isinstance(fields["file_name"].dataType, StringType)
    assert isinstance(fields["variant_id"].dataType, StringType)

    # Test array fields
    assert isinstance(fields["names"].dataType, ArrayType)
    assert isinstance(fields["names"].dataType.elementType, StringType)

    assert isinstance(fields["alternateAlleles"].dataType, ArrayType)
    assert isinstance(fields["alternateAlleles"].dataType.elementType, StringType)

    assert isinstance(fields["filters"].dataType, ArrayType)
    assert isinstance(fields["filters"].dataType.elementType, StringType)

    # Test variant field
    assert isinstance(fields["info"].dataType, VariantType)

    # Test genotypes array field
    assert isinstance(fields["genotypes"].dataType, ArrayType)
    genotype_struct = fields["genotypes"].dataType.elementType
    assert isinstance(genotype_struct, StructType)


def test_get_vcf_schema_genotype_structure():
    """Test that genotype field has correct structure."""
    schema = get_vcf_schema()
    fields = {field.name: field for field in schema.fields}

    genotype_struct = fields["genotypes"].dataType.elementType
    genotype_fields = {field.name: field for field in genotype_struct.fields}

    # Check genotype has required subfields
    assert "sampleId" in genotype_fields
    assert "calls" in genotype_fields
    assert "data" in genotype_fields

    # Check genotype field types
    assert isinstance(genotype_fields["sampleId"].dataType, StringType)
    assert isinstance(genotype_fields["calls"].dataType, ArrayType)
    assert isinstance(genotype_fields["calls"].dataType.elementType, LongType)
    assert isinstance(genotype_fields["data"].dataType, VariantType)


def test_get_vcf_schema_nullability():
    """Test that schema fields have correct nullability."""
    schema = get_vcf_schema()
    fields = {field.name: field for field in schema.fields}

    # Non-nullable fields
    assert not fields["contig"].nullable
    assert not fields["start"].nullable
    assert not fields["end"].nullable
    assert not fields["referenceAllele"].nullable
    assert not fields["alternateAlleles"].nullable

    # Nullable fields
    assert fields["names"].nullable
    assert fields["qual"].nullable
    assert fields["filters"].nullable
    assert fields["info"].nullable
    assert fields["genotypes"].nullable
    assert fields["file_path"].nullable
    assert fields["file_name"].nullable
    assert fields["variant_id"].nullable


def test_get_vcf_schema_genotype_nullability():
    """Test that genotype subfields have correct nullability."""
    schema = get_vcf_schema()
    fields = {field.name: field for field in schema.fields}

    genotype_struct = fields["genotypes"].dataType.elementType
    genotype_fields = {field.name: field for field in genotype_struct.fields}

    # sampleId should not be nullable
    assert not genotype_fields["sampleId"].nullable

    # calls and data can be nullable
    assert genotype_fields["calls"].nullable
    assert genotype_fields["data"].nullable


def test_schema_consistency():
    """Test that calling get_vcf_schema multiple times returns consistent schema."""
    schema1 = get_vcf_schema()
    schema2 = get_vcf_schema()

    # Should be structurally equal
    assert schema1 == schema2
