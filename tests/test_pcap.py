"""Unit tests for the PCAP data source."""

import gzip
import io
import socket
import tempfile
from datetime import datetime, timezone

import dpkt
import pytest
from pyspark.sql.types import StructType

from cyber_connectors import PcapDataSource
from cyber_connectors.Pcap import (
    PCAP_SCHEMA,
    FileEntry,
    PcapBatchReader,
    _filter_file_entries,
    _open_pcap_bytes,
    _parse_pcap_content,
)


def _pcap_bytes(packets: list[tuple[float, bytes]]) -> bytes:
    """Build a minimal PCAP file in memory."""
    buffer = io.BytesIO()
    writer = dpkt.pcap.Writer(buffer)
    for ts, raw in packets:
        writer.writepkt(raw, ts=ts)
    return buffer.getvalue()


def _eth_ip_tcp(src_ip: str, dst_ip: str, src_port: int, dst_port: int, payload: bytes = b"") -> bytes:
    tcp = dpkt.tcp.TCP(sport=src_port, dport=dst_port, data=payload, off=5)
    ip = dpkt.ip.IP(src=socket.inet_aton(src_ip), dst=socket.inet_aton(dst_ip), p=dpkt.ip.IP_PROTO_TCP, data=tcp)
    ip.len = len(ip)
    return bytes(dpkt.ethernet.Ethernet(data=ip))


@pytest.fixture
def sample_schema():
    """Return the PCAP schema for reader tests."""
    return PCAP_SCHEMA


@pytest.fixture
def gzip_pcap_bytes():
    """Build a gzipped single-packet PCAP payload."""
    raw_pcap = _pcap_bytes([(1700000000.0, _eth_ip_tcp("10.0.0.1", "10.0.0.2", 1234, 80))])
    return gzip.compress(raw_pcap)


class TestPcapDataSource:
    """Test PcapDataSource registration and schema."""

    def test_name(self):
        """Test the registered source name."""
        assert PcapDataSource.name() == "pcap"

    def test_schema_is_struct_type(self):
        """Test the source schema type."""
        ds = PcapDataSource(options={})
        assert isinstance(ds.schema(), StructType)


class TestPcapParser:
    """Test parser helper functions."""

    def test_parse_empty_content_returns_no_rows(self):
        """Test empty content handling."""
        assert list(_parse_pcap_content(b"")) == []

    def test_open_pcap_bytes_handles_gzip(self, gzip_pcap_bytes):
        """Test gzipped PCAP parsing."""
        rows = list(_parse_pcap_content(gzip_pcap_bytes))
        assert rows
        assert rows[0][0].tzinfo == timezone.utc
        assert rows[0][1] == "10.0.0.1"
        assert rows[0][2] == "10.0.0.2"

    def test_open_pcap_bytes_returns_reader_for_valid_content(self):
        """Test reader creation for valid bytes."""
        content = _pcap_bytes([(1700000000.0, _eth_ip_tcp("1.1.1.1", "2.2.2.2", 1000, 80))])
        reader = _open_pcap_bytes(content)
        assert reader is not None


class TestPcapBatchReader:
    """Test batch-reader helper behavior."""

    def test_filter_file_entries_respects_modified_after(self):
        """Test the modifiedAfter filter."""
        entries = [
            FileEntry(path="file:///a.pcap", modification_time_ms=1000, length=10),
            FileEntry(path="file:///b.pcap", modification_time_ms=2000, length=10),
        ]
        filtered = _filter_file_entries(
            entries,
            modified_after=datetime.fromtimestamp(1.5, tz=timezone.utc),
            modified_before=None,
        )
        assert [entry.path for entry in filtered] == ["file:///b.pcap"]

    def test_batch_reader_parses_common_file_options(self, sample_schema):
        """Test common option parsing."""
        reader = PcapBatchReader(
            {
                "path": "/tmp/pcaps",
                "recursiveFileLookup": "true",
                "pathGlobFilter": "*.pcap",
                "fileNamePattern": "*.pcap.gz",
                "modifiedAfter": "2024-01-01T00:00:00Z",
                "ignoreCorruptFiles": "true",
                "ignoreMissingFiles": "true",
            },
            sample_schema,
        )
        assert reader.recursive_file_lookup is True
        assert reader.file_pattern == "*.pcap.gz"
        assert reader.ignore_corrupt_files is True
        assert reader.ignore_missing_files is True

    def test_batch_reader_reads_local_files(self, spark_session):
        """Test local batch reading through Spark."""
        spark_session.dataSource.register(PcapDataSource)
        content = _pcap_bytes([(1700000000.0, _eth_ip_tcp("172.16.0.1", "172.16.0.2", 9999, 80))])

        with tempfile.TemporaryDirectory() as temp_dir:
            with open(f"{temp_dir}/sample.pcap", "wb") as handle:
                handle.write(content)

            rows = spark_session.read.format("pcap").load(temp_dir).collect()

        assert len(rows) == 1
        assert rows[0].src_ip == "172.16.0.1"
        assert rows[0].dst_ip == "172.16.0.2"
        assert rows[0].protocol == "tcp"
