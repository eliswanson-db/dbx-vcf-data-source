"""Unit tests for compat module (compatibility layer)."""

import pytest
import json
from vcf_reader.compat import VariantVal, VariantType, DATABRICKS_AVAILABLE


def test_databricks_available_flag():
    """Test that DATABRICKS_AVAILABLE is a boolean."""
    assert isinstance(DATABRICKS_AVAILABLE, bool)


def test_variant_val_exists():
    """Test that VariantVal class is available."""
    assert VariantVal is not None


def test_variant_type_exists():
    """Test that VariantType class is available."""
    assert VariantType is not None


def test_variant_val_parse_json():
    """Test VariantVal.parseJson method."""
    json_str = '{"key": "value", "number": 42}'
    result = VariantVal.parseJson(json_str)
    assert result is not None


def test_variant_val_parse_json_empty():
    """Test VariantVal.parseJson with empty object."""
    json_str = '{}'
    result = VariantVal.parseJson(json_str)
    assert result is not None


def test_variant_val_parse_json_nested():
    """Test VariantVal.parseJson with nested object."""
    json_str = '{"outer": {"inner": "value"}, "array": [1, 2, 3]}'
    result = VariantVal.parseJson(json_str)
    assert result is not None


def test_variant_type_instantiation():
    """Test that VariantType can be instantiated."""
    vt = VariantType()
    assert vt is not None


class TestMockVariantVal:
    """Tests specific to mock VariantVal implementation (when not in Databricks)."""
    
    def test_mock_has_value_attribute(self):
        """Test that mock VariantVal stores value."""
        if not DATABRICKS_AVAILABLE:
            json_str = '{"test": "data"}'
            result = VariantVal.parseJson(json_str)
            assert hasattr(result, 'value')
            assert result.value == json_str
    
    def test_mock_repr(self):
        """Test that mock VariantVal has string representation."""
        if not DATABRICKS_AVAILABLE:
            json_str = '{"test": "data"}'
            result = VariantVal.parseJson(json_str)
            repr_str = repr(result)
            assert "VariantVal" in repr_str


def test_variant_val_parse_various_types():
    """Test VariantVal.parseJson with various JSON types."""
    test_cases = [
        '{"string": "value"}',
        '{"number": 123}',
        '{"float": 123.456}',
        '{"boolean": true}',
        '{"null": null}',
        '{"array": [1, 2, 3]}',
        '{"nested": {"deep": {"value": "here"}}}',
    ]
    
    for json_str in test_cases:
        result = VariantVal.parseJson(json_str)
        assert result is not None, f"Failed to parse: {json_str}"


def test_variant_val_info_field_example():
    """Test VariantVal with typical INFO field data."""
    info_dict = {
        "DP": 14,
        "AF": [0.5, 0.3],
        "DB": True,
    }
    json_str = json.dumps(info_dict)
    result = VariantVal.parseJson(json_str)
    assert result is not None


def test_variant_val_genotype_data_example():
    """Test VariantVal with typical genotype data."""
    genotype_dict = {
        "DP": 14,
        "GQ": 99,
        "PL": [0, 30, 255],
    }
    json_str = json.dumps(genotype_dict)
    result = VariantVal.parseJson(json_str)
    assert result is not None

