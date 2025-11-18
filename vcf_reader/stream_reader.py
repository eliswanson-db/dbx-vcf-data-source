import gzip
import os
from typing import Iterator, Tuple, List
from pyspark.sql.types import StructType
from vcf_reader.compat import DataSourceStreamReader, InputPartition
from vcf_reader.parser import parse_header, parse_vcf_line


class VCFPartition(InputPartition):
    """Represents a byte range partition of a VCF file."""

    def __init__(
        self, path: str, start_offset: int, end_offset: int, file_name: str = ""
    ):
        self.path = path
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.file_name = file_name


class VCFStreamReader(DataSourceStreamReader):
    """
    Streaming reader for VCF files using chunk-based approach.

    Uses file byte offsets to track progress and divides files into
    partitions for parallel processing.
    """

    CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks

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

        # Discover VCF files
        self.vcf_files = self._discover_vcf_files(self.path) if self.path else []

        # Cache header information per file
        self._sample_names_cache = {}
        self._header_size_cache = {}

    def _discover_vcf_files(self, path: str) -> List[str]:
        """
        Discover all VCF files in the given path.

        Args:
            path: File path or directory path

        Returns:
            List of VCF file paths
        """
        if not path or not os.path.exists(path):
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

    def _get_header_info(self, file_path: str) -> Tuple[List[str], int]:
        """Read header and return sample names and header byte size for a specific file."""
        # Check cache first
        if file_path in self._sample_names_cache:
            return (
                self._sample_names_cache[file_path],
                self._header_size_cache[file_path],
            )

        if not file_path or not os.path.exists(file_path):
            return [], 0

        is_gzipped = file_path.endswith(".gz")

        if is_gzipped:
            file_handle = gzip.open(file_path, "rt", encoding="utf-8")
        else:
            file_handle = open(file_path, "r", encoding="utf-8")

        try:
            header_lines = []
            header_size = 0

            for line in file_handle:
                if is_gzipped:
                    # For gzipped files, we can't easily track byte offsets
                    # so we'll use line-based offsets instead
                    header_size += 1
                else:
                    header_size = file_handle.tell()

                if line.startswith("##"):
                    header_lines.append(line)
                elif line.startswith("#CHROM"):
                    header_lines.append(line)
                    sample_names, _ = parse_header(header_lines)
                    # Cache the results
                    self._sample_names_cache[file_path] = sample_names
                    self._header_size_cache[file_path] = header_size
                    return sample_names, header_size
                else:
                    break

            # Cache empty results
            self._sample_names_cache[file_path] = []
            self._header_size_cache[file_path] = header_size
            return [], header_size

        finally:
            file_handle.close()

    def initialOffset(self) -> dict:
        """Returns the initial start offset."""
        if not self.vcf_files:
            return {"file_index": 0, "offset": 0}

        # For single file, use simple structure for compatibility
        if len(self.vcf_files) == 1:
            file_path = self.vcf_files[0]
            _, header_size = self._get_header_info(file_path)
            return {"path": file_path, "offset": header_size}

        # For multiple files, use file index tracking
        _, header_size = self._get_header_info(self.vcf_files[0])
        return {"file_index": 0, "offset": header_size}

    def latestOffset(self) -> dict:
        """Returns the current latest offset (end of file(s))."""
        if not self.vcf_files:
            return {"file_index": 0, "offset": 0}

        # For single file, use simple structure
        if len(self.vcf_files) == 1:
            file_path = self.vcf_files[0]
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                return {"path": file_path, "offset": file_size}
            return {"path": file_path, "offset": 0}

        # For multiple files, return last file index and its size
        last_file = self.vcf_files[-1]
        if os.path.exists(last_file):
            file_size = os.path.getsize(last_file)
            return {"file_index": len(self.vcf_files) - 1, "offset": file_size}
        return {"file_index": len(self.vcf_files) - 1, "offset": 0}

    def partitions(self, start: dict, end: dict) -> List[VCFPartition]:
        """
        Divides the byte range into partitions.

        Args:
            start: Start offset dict
            end: End offset dict

        Returns:
            List of VCFPartition objects
        """
        partitions = []

        # Single file case (simple offset structure)
        if "path" in start and "path" in end:
            path = start["path"]
            file_name = os.path.basename(path)
            start_offset = start["offset"]
            end_offset = end["offset"]

            if start_offset >= end_offset:
                return []

            # Create byte-range partitions for the file
            current_offset = start_offset
            while current_offset < end_offset:
                partition_end = min(current_offset + self.CHUNK_SIZE, end_offset)
                partitions.append(
                    VCFPartition(path, current_offset, partition_end, file_name)
                )
                current_offset = partition_end

            return partitions

        # Multiple files case (file_index structure)
        start_idx = start.get("file_index", 0)
        end_idx = end.get("file_index", 0)
        start_offset = start.get("offset", 0)
        end_offset = end.get("offset", 0)

        # Process files from start_idx to end_idx
        for file_idx in range(start_idx, end_idx + 1):
            if file_idx >= len(self.vcf_files):
                break

            file_path = self.vcf_files[file_idx]
            file_name = os.path.basename(file_path)

            # Determine offset range for this file
            if file_idx == start_idx:
                file_start = start_offset
            else:
                _, file_start = self._get_header_info(file_path)

            if file_idx == end_idx:
                file_end = end_offset
            else:
                file_end = (
                    os.path.getsize(file_path) if os.path.exists(file_path) else 0
                )

            if file_start >= file_end:
                continue

            # Create partitions for this file
            current_offset = file_start
            while current_offset < file_end:
                partition_end = min(current_offset + self.CHUNK_SIZE, file_end)
                partitions.append(
                    VCFPartition(file_path, current_offset, partition_end, file_name)
                )
                current_offset = partition_end

        return partitions

    def commit(self, end: dict) -> None:
        """Called when query has finished processing data before end offset."""
        # No cleanup needed for file-based streaming

    def read(self, partition: VCFPartition) -> Iterator[Tuple]:
        """
        Read data from a partition.

        Args:
            partition: VCFPartition with byte range to read

        Yields:
            Tuples matching VCF schema
        """
        if not os.path.exists(partition.path):
            return

        is_gzipped = partition.path.endswith(".gz")

        # For gzipped files, we need to read from the beginning
        # This is a limitation of gzip format
        if is_gzipped:
            yield from self._read_gzipped_partition(partition)
        else:
            yield from self._read_plain_partition(partition)

    def _read_plain_partition(self, partition: VCFPartition) -> Iterator[Tuple]:
        """Read from a plain text VCF file partition."""
        sample_names, _ = self._get_header_info(partition.path)
        file_name = partition.file_name or os.path.basename(partition.path)

        with open(partition.path, "r", encoding="utf-8") as f:
            f.seek(partition.start_offset)

            # Skip partial line at start if not at file beginning
            if partition.start_offset > 0:
                f.readline()

            while f.tell() < partition.end_offset:
                line = f.readline()
                if not line:
                    break

                if line.startswith("#") or not line.strip():
                    continue

                try:
                    row = parse_vcf_line(
                        line,
                        sample_names,
                        self.include_samples,
                        self.exclude_samples,
                        file_path=partition.path if self.include_metadata else "",
                        file_name=file_name if self.include_metadata else "",
                        generate_primary_key=self.generate_primary_key,
                    )
                    yield row
                except (ValueError, IndexError):
                    continue

    def _read_gzipped_partition(self, partition: VCFPartition) -> Iterator[Tuple]:
        """Read from a gzipped VCF file partition."""
        sample_names, header_size = self._get_header_info(partition.path)
        file_name = partition.file_name or os.path.basename(partition.path)

        with gzip.open(partition.path, "rt") as f:
            # Skip header
            for _ in range(header_size):
                f.readline()

            # Read lines in partition range
            # Note: For gzipped files, offsets are line numbers
            current_line = header_size

            while current_line < partition.end_offset:
                line = f.readline()
                if not line:
                    break

                current_line += 1

                if current_line <= partition.start_offset:
                    continue

                if line.startswith("#") or not line.strip():
                    continue

                try:
                    row = parse_vcf_line(
                        line,
                        sample_names,
                        self.include_samples,
                        self.exclude_samples,
                        file_path=partition.path if self.include_metadata else "",
                        file_name=file_name if self.include_metadata else "",
                        generate_primary_key=self.generate_primary_key,
                    )
                    yield row
                except (ValueError, IndexError):
                    continue
