import gzip
import os
from typing import Iterator, Tuple, List
from pyspark.sql.types import StructType
from vcf_reader.compat import DataSourceReader, InputPartition
from vcf_reader.parser import parse_header, parse_vcf_line

try:
    from vcf_reader.arrow_reader import ArrowVCFReader, ARROW_AVAILABLE
except ImportError:
    ARROW_AVAILABLE = False
    ArrowVCFReader = None


class VCFFilePartition(InputPartition):
    """Represents a single VCF file to be processed as a partition."""

    def __init__(self, file_path: str, file_name: str):
        self.file_path = file_path
        self.file_name = file_name


class VCFBatchReader(DataSourceReader):
    """
    Batch reader for VCF files.

    Reads VCF files and yields rows matching the VCF schema.
    Supports both plain and gzip-compressed VCF files.
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.path = options.get("path")

        # Parse sample filtering options
        include_str = options.get("includeSampleIds", "")
        exclude_str = options.get("excludeSampleIds", "")

        self.include_samples = (
            [s.strip() for s in include_str.split(",") if s.strip()]
            if include_str
            else None
        )
        self.exclude_samples = (
            [s.strip() for s in exclude_str.split(",") if s.strip()]
            if exclude_str
            else None
        )

        # Parse metadata options
        self.include_metadata = (
            options.get("includeFileMetadata", "true").lower() == "true"
        )
        self.generate_primary_key = (
            options.get("generatePrimaryKey", "false").lower() == "true"
        )

        # Parse Arrow options
        self.use_arrow = (
            options.get("useArrow", "true").lower() == "true" and ARROW_AVAILABLE
        )
        self.batch_size = int(options.get("batchSize", "10000"))
        self.stream_chunk_size = int(
            options.get("streamChunkSize", "67108864")
        )  # 64MB default

        # Discover VCF files
        self.vcf_files = self._discover_vcf_files(self.path) if self.path else []

    def partitions(self) -> List[VCFFilePartition]:
        """
        Create partitions for parallel processing.

        Returns one partition per VCF file for distribution across executors.

        Returns:
            List of VCFFilePartition objects
        """
        partitions = []
        for file_path in self.vcf_files:
            file_name = os.path.basename(file_path)
            partitions.append(VCFFilePartition(file_path, file_name))
        return partitions

    def _discover_vcf_files(self, path: str) -> List[str]:
        """
        Discover all VCF files in the given path.

        Args:
            path: File path or directory path

        Returns:
            List of VCF file paths
        """
        if not path:
            return []

        if not os.path.exists(path):
            # Path doesn't exist - might be a cloud path in Databricks
            # Try to treat it as a single file path
            if path.endswith(".vcf") or path.endswith(".vcf.gz"):
                return [path]
            return []

        # If it's a file, return it directly
        if os.path.isfile(path):
            return [path]

        # If it's a directory, recursively find all VCF files
        vcf_files = []
        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith(".vcf") or file.endswith(".vcf.gz"):
                    vcf_files.append(os.path.join(root, file))

        return sorted(vcf_files)

    def read(self, partition: VCFFilePartition) -> Iterator[Tuple]:
        """
        Read VCF file from partition and yield rows as tuples.

        Args:
            partition: VCFFilePartition specifying which file to read

        Yields:
            Tuples matching VCF schema
        """
        # Handle None partition gracefully
        if partition is None:
            # No partition provided, return empty
            return

        # Get file info from partition
        file_path = partition.file_path
        file_name = partition.file_name

        # Use Arrow-based reader if enabled and available
        if self.use_arrow and ARROW_AVAILABLE and ArrowVCFReader:
            reader = ArrowVCFReader(
                file_path=file_path,
                file_name=file_name,
                include_samples=self.include_samples,
                exclude_samples=self.exclude_samples,
                include_metadata=self.include_metadata,
                generate_primary_key=self.generate_primary_key,
                batch_size=self.batch_size,
                stream_chunk_size=self.stream_chunk_size,
            )
            yield from reader.read_batched()
            return

        # Fallback to line-by-line reading
        # Determine if file is gzipped
        is_gzipped = file_path.endswith(".gz")

        # Open file appropriately
        if is_gzipped:
            file_handle = gzip.open(file_path, "rt", encoding="utf-8")
        else:
            file_handle = open(file_path, "r", encoding="utf-8")

        try:
            # Read and parse header
            header_lines = []
            sample_names = []

            for line in file_handle:
                if line.startswith("##"):
                    header_lines.append(line)
                elif line.startswith("#CHROM"):
                    header_lines.append(line)
                    sample_names, _ = parse_header(header_lines)
                    break

            # Read and parse data lines
            for line in file_handle:
                if line.startswith("#"):
                    continue
                if not line.strip():
                    continue

                try:
                    row = parse_vcf_line(
                        line,
                        sample_names,
                        self.include_samples,
                        self.exclude_samples,
                        file_path=file_path if self.include_metadata else "",
                        file_name=file_name if self.include_metadata else "",
                        generate_primary_key=self.generate_primary_key,
                    )
                    yield row
                except (ValueError, IndexError):
                    # Skip malformed lines
                    continue

        finally:
            file_handle.close()
