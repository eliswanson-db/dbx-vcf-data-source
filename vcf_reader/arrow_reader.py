"""Arrow-based batch reader for VCF files for improved performance."""

import gzip
import io
from typing import Iterator, Tuple, List, Optional, TYPE_CHECKING

try:
    import pyarrow as pa
    import pyarrow.csv as csv

    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    pa = None  # type: ignore
    csv = None  # type: ignore

if TYPE_CHECKING:
    import pyarrow as pa

from vcf_reader.parser import parse_header, parse_vcf_line


class ArrowVCFReader:
    """
    Arrow-based VCF reader for high-performance batch reading.

    Uses native Python for header parsing, then switches to PyArrow
    for fast batch reading of data lines.
    """

    # Batch size for Arrow reading (rows per batch)
    DEFAULT_BATCH_SIZE = 10000

    def __init__(
        self,
        file_path: str,
        file_name: str = "",
        include_samples: Optional[List[str]] = None,
        exclude_samples: Optional[List[str]] = None,
        include_metadata: bool = True,
        generate_primary_key: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """
        Initialize Arrow-based VCF reader.

        Args:
            file_path: Path to VCF file
            file_name: Name of VCF file
            include_samples: Optional list of samples to include
            exclude_samples: Optional list of samples to exclude
            include_metadata: Whether to include file metadata
            generate_primary_key: Whether to generate variant IDs
            batch_size: Number of rows to read per batch
        """
        self.file_path = file_path
        self.file_name = file_name
        self.include_samples = include_samples
        self.exclude_samples = exclude_samples
        self.include_metadata = include_metadata
        self.generate_primary_key = generate_primary_key
        self.batch_size = batch_size

        self.is_gzipped = file_path.endswith(".gz")
        self.sample_names = []
        self.header_end_position = 0

    def _read_header(self, file_handle) -> Tuple[List[str], int]:
        """
        Read VCF header using native Python.

        Args:
            file_handle: Open file handle

        Returns:
            Tuple of (sample_names, header_end_byte_position)
        """
        header_lines = []
        start_pos = file_handle.tell()

        while True:
            line = file_handle.readline()
            if not line:
                # End of file
                break

            if line.startswith("##"):
                header_lines.append(line)
            elif line.startswith("#CHROM"):
                header_lines.append(line)
                sample_names, _ = parse_header(header_lines)
                header_end = file_handle.tell()
                return sample_names, header_end
            else:
                # Data line encountered without proper header
                break

        # No proper header found
        return [], start_pos

    def read_batched(self) -> Iterator[Tuple]:
        """
        Read VCF file in batches using Arrow for performance.

        Yields:
            Parsed VCF row tuples
        """
        if not ARROW_AVAILABLE:
            # Fall back to line-by-line reading
            yield from self._read_line_by_line()
            return

        # For gzipped files, we need to decompress first
        if self.is_gzipped:
            yield from self._read_gzipped_batched()
        else:
            yield from self._read_plain_batched()

    def _read_plain_batched(self) -> Iterator[Tuple]:
        """Read plain text VCF in batches using Arrow."""
        with open(self.file_path, "r", encoding="utf-8") as f:
            # Read header with Python
            self.sample_names, self.header_end_position = self._read_header(f)

            # Read data portion with Arrow
            # Arrow will start reading from current file position
            remaining_data = f.read()

            if not remaining_data.strip():
                return

            # Use Arrow CSV reader with tab delimiter
            try:
                # Create a CSV read options with tab delimiter
                read_options = csv.ReadOptions(
                    use_threads=True,
                    block_size=self.batch_size * 1024,  # Approximate batch size
                    skip_rows=0,
                    column_names=None,
                )

                parse_options = csv.ParseOptions(
                    delimiter="\t",
                    quote_char=False,
                    escape_char=False,
                    newlines_in_values=False,
                )

                # Read CSV into Arrow table in batches
                reader = csv.read_csv(
                    io.BytesIO(remaining_data.encode("utf-8")),
                    read_options=read_options,
                    parse_options=parse_options,
                )

                # Process the table in batches
                yield from self._process_arrow_table(reader)

            except Exception:
                # Fall back to line-by-line if Arrow fails
                f.seek(self.header_end_position)
                for line in f:
                    if line.startswith("#") or not line.strip():
                        continue
                    try:
                        row = parse_vcf_line(
                            line,
                            self.sample_names,
                            self.include_samples,
                            self.exclude_samples,
                            file_path=self.file_path if self.include_metadata else "",
                            file_name=self.file_name if self.include_metadata else "",
                            generate_primary_key=self.generate_primary_key,
                        )
                        yield row
                    except (ValueError, IndexError):
                        continue

    def _read_gzipped_batched(self) -> Iterator[Tuple]:
        """Read gzipped VCF file with Arrow."""
        with gzip.open(self.file_path, "rt", encoding="utf-8") as f:
            # Read header with Python
            self.sample_names, _ = self._read_header(f)

            # For gzipped files, read remaining data into memory
            # (decompression is needed anyway)
            remaining_data = f.read()

            if not remaining_data.strip():
                return

            try:
                # Use Arrow CSV reader
                read_options = csv.ReadOptions(
                    use_threads=True,
                    block_size=self.batch_size * 1024,
                    skip_rows=0,
                    column_names=None,
                )

                parse_options = csv.ParseOptions(
                    delimiter="\t",
                    quote_char=False,
                    escape_char=False,
                    newlines_in_values=False,
                )

                reader = csv.read_csv(
                    io.BytesIO(remaining_data.encode("utf-8")),
                    read_options=read_options,
                    parse_options=parse_options,
                )

                yield from self._process_arrow_table(reader)

            except Exception:
                # Fall back to line-by-line
                for line in remaining_data.split("\n"):
                    if not line or line.startswith("#"):
                        continue
                    try:
                        row = parse_vcf_line(
                            line,
                            self.sample_names,
                            self.include_samples,
                            self.exclude_samples,
                            file_path=self.file_path if self.include_metadata else "",
                            file_name=self.file_name if self.include_metadata else "",
                            generate_primary_key=self.generate_primary_key,
                        )
                        yield row
                    except (ValueError, IndexError):
                        continue

    def _process_arrow_table(self, table: pa.Table) -> Iterator[Tuple]:
        """
        Process Arrow table and convert to VCF row tuples.

        Args:
            table: PyArrow table containing VCF data

        Yields:
            Parsed VCF row tuples
        """
        # Convert to batches for memory efficiency
        for batch in table.to_batches(max_chunksize=self.batch_size):
            # Convert batch to Python for parsing
            # This is still faster than line-by-line file I/O
            columns = [batch.column(i).to_pylist() for i in range(batch.num_columns)]

            # Each row is a list of column values
            for row_idx in range(batch.num_rows):
                row_data = [col[row_idx] for col in columns]

                # Skip comment lines that might have slipped through
                if (
                    row_data
                    and isinstance(row_data[0], str)
                    and row_data[0].startswith("#")
                ):
                    continue

                # Convert row to tab-delimited string for existing parser
                # (This allows reuse of existing parsing logic)
                line = "\t".join(
                    str(val) if val is not None else "." for val in row_data
                )

                try:
                    row = parse_vcf_line(
                        line,
                        self.sample_names,
                        self.include_samples,
                        self.exclude_samples,
                        file_path=self.file_path if self.include_metadata else "",
                        file_name=self.file_name if self.include_metadata else "",
                        generate_primary_key=self.generate_primary_key,
                    )
                    yield row
                except (ValueError, IndexError):
                    # Skip malformed lines
                    continue

    def _read_line_by_line(self) -> Iterator[Tuple]:
        """
        Fallback line-by-line reader when Arrow is not available.

        Yields:
            Parsed VCF row tuples
        """
        if self.is_gzipped:
            file_handle = gzip.open(self.file_path, "rt", encoding="utf-8")
        else:
            file_handle = open(self.file_path, "r", encoding="utf-8")

        try:
            # Read header
            self.sample_names, _ = self._read_header(file_handle)

            # Read data lines
            for line in file_handle:
                if line.startswith("#") or not line.strip():
                    continue

                try:
                    row = parse_vcf_line(
                        line,
                        self.sample_names,
                        self.include_samples,
                        self.exclude_samples,
                        file_path=self.file_path if self.include_metadata else "",
                        file_name=self.file_name if self.include_metadata else "",
                        generate_primary_key=self.generate_primary_key,
                    )
                    yield row
                except (ValueError, IndexError):
                    continue
        finally:
            file_handle.close()


def read_vcf_with_arrow(
    file_path: str,
    file_name: str = "",
    include_samples: Optional[List[str]] = None,
    exclude_samples: Optional[List[str]] = None,
    include_metadata: bool = True,
    generate_primary_key: bool = False,
    batch_size: int = ArrowVCFReader.DEFAULT_BATCH_SIZE,
) -> Iterator[Tuple]:
    """
    Convenience function to read VCF file using Arrow-based reader.

    Args:
        file_path: Path to VCF file
        file_name: Name of VCF file
        include_samples: Optional list of samples to include
        exclude_samples: Optional list of samples to exclude
        include_metadata: Whether to include file metadata
        generate_primary_key: Whether to generate variant IDs
        batch_size: Number of rows per batch

    Yields:
        Parsed VCF row tuples
    """
    reader = ArrowVCFReader(
        file_path=file_path,
        file_name=file_name,
        include_samples=include_samples,
        exclude_samples=exclude_samples,
        include_metadata=include_metadata,
        generate_primary_key=generate_primary_key,
        batch_size=batch_size,
    )

    yield from reader.read_batched()
