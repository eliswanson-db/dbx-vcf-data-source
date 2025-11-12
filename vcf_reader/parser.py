import json
import hashlib
from typing import List, Dict, Any, Tuple, Optional
from vcf_reader.compat import VariantVal


def parse_header(lines: List[str]) -> Tuple[List[str], Dict[str, Any]]:
    """
    Parse VCF header lines to extract sample names and metadata.

    Args:
        lines: List of header lines starting with ##

    Returns:
        Tuple of (sample_names, metadata_dict)
    """
    sample_names = []
    metadata = {}

    for line in lines:
        line = line.strip()
        if line.startswith("#CHROM"):
            parts = line.split("\t")
            if len(parts) > 9:
                sample_names = parts[9:]
            break
        elif line.startswith("##"):
            metadata[line] = True

    return sample_names, metadata


def parse_info_field(info_str: str) -> VariantVal:
    """
    Parse INFO field into a variant value.

    Args:
        info_str: INFO field string (e.g., "DP=14;AF=0.5;DB")

    Returns:
        VariantVal representing the parsed INFO data
    """
    if info_str == "." or not info_str:
        return VariantVal.parseJson("{}")

    info_dict = {}
    for item in info_str.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            # Try to preserve numeric types
            if "," in value:
                # Array value
                parts = value.split(",")
                try:
                    info_dict[key] = [float(p) if "." in p else int(p) for p in parts]
                except ValueError:
                    info_dict[key] = parts
            else:
                try:
                    info_dict[key] = float(value) if "." in value else int(value)
                except ValueError:
                    info_dict[key] = value
        else:
            # Flag field (present without value)
            info_dict[item] = True

    return VariantVal.parseJson(json.dumps(info_dict))


def parse_genotype(
    format_str: str, genotype_str: str, sample_id: str
) -> Dict[str, Any]:
    """
    Parse a single sample's genotype data.

    Args:
        format_str: FORMAT field (e.g., "GT:DP:GQ")
        genotype_str: Sample genotype string (e.g., "0/1:14:99")
        sample_id: Sample identifier

    Returns:
        Dictionary with sampleId, calls, and data fields
    """
    if genotype_str == "." or not genotype_str:
        return {
            "sampleId": sample_id,
            "calls": None,
            "data": VariantVal.parseJson("{}"),
        }

    format_keys = format_str.split(":")
    genotype_values = genotype_str.split(":")

    gt_calls = None
    data_dict = {}

    for i, key in enumerate(format_keys):
        if i >= len(genotype_values):
            break

        value = genotype_values[i]

        if key == "GT":
            # Parse genotype calls
            if value != ".":
                # Handle both / and | separators
                call_str = value.replace("|", "/")
                try:
                    gt_calls = [int(c) if c != "." else -1 for c in call_str.split("/")]
                except ValueError:
                    gt_calls = None
        else:
            # Store other format fields
            if value != ".":
                try:
                    # Try numeric conversion
                    if "," in value:
                        parts = value.split(",")
                        data_dict[key] = [
                            float(p) if "." in p else int(p) for p in parts
                        ]
                    else:
                        data_dict[key] = float(value) if "." in value else int(value)
                except ValueError:
                    data_dict[key] = value

    return {
        "sampleId": sample_id,
        "calls": gt_calls,
        "data": VariantVal.parseJson(json.dumps(data_dict)),
    }


def parse_vcf_line(
    line: str,
    sample_names: List[str],
    include_samples: Optional[List[str]] = None,
    exclude_samples: Optional[List[str]] = None,
    file_path: str = "",
    file_name: str = "",
    generate_primary_key: bool = False,
) -> Tuple:
    """
    Parse a single VCF data line into a tuple matching the VCF schema.

    Args:
        line: VCF data line (tab-separated)
        sample_names: List of sample names from header
        include_samples: Optional list of samples to include
        exclude_samples: Optional list of samples to exclude
        file_path: Path to the source VCF file
        file_name: Name of the source VCF file
        generate_primary_key: Whether to generate a compound primary key hash

    Returns:
        Tuple matching VCF schema fields including metadata
    """
    fields = line.strip().split("\t")
    if len(fields) < 8:
        raise ValueError(f"Invalid VCF line: {line}")

    chrom = fields[0]
    pos = int(fields[1])
    id_field = fields[2]
    ref = fields[3]
    alt = fields[4]
    qual = fields[5]
    filter_field = fields[6]
    info = fields[7]

    # Parse basic fields
    start = pos - 1  # Convert to 0-indexed
    end = start + len(ref)

    names = id_field.split(";") if id_field != "." else []
    alt_alleles = alt.split(",") if alt != "." else []
    qual_value = float(qual) if qual != "." else None
    filters = filter_field.split(";") if filter_field != "." else []

    info_variant = parse_info_field(info)

    # Parse genotypes if present
    genotypes = []
    if len(fields) > 8 and len(sample_names) > 0:
        format_str = fields[8]

        # Determine which samples to include
        samples_to_process = []
        for i, sample_name in enumerate(sample_names):
            if include_samples and sample_name not in include_samples:
                continue
            if exclude_samples and sample_name in exclude_samples:
                continue
            samples_to_process.append((i, sample_name))

        for sample_idx, sample_name in samples_to_process:
            field_idx = 9 + sample_idx
            if field_idx < len(fields):
                genotype = parse_genotype(format_str, fields[field_idx], sample_name)
                genotypes.append(genotype)

    # Generate variant ID if requested
    variant_id = None
    if generate_primary_key:
        hash_string = f"{file_path}|{file_name}|{chrom}|{start}|{end}"
        variant_id = hashlib.sha256(hash_string.encode()).hexdigest()

    return (
        chrom,
        start,
        end,
        names,
        ref,
        alt_alleles,
        qual_value,
        filters,
        info_variant,
        genotypes if genotypes else None,
        file_path,
        file_name,
        variant_id,
    )
