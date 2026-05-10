"""Unit tests for the PCAP stream reader."""

import io
import socket
from unittest.mock import patch

import dpkt

from cyber_connectors import PcapDataSource
from cyber_connectors.Pcap import (
    PCAP_SCHEMA,
    FileEntry,
    PcapStreamOffset,
    PcapStreamReader,
    _select_stream_batch,
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


def test_stream_offset_roundtrip():
    offset = PcapStreamOffset(1704067200000, "file:///b.pcap")
    restored = PcapStreamOffset.from_json(offset.json())
    assert restored.modification_time_ms == 1704067200000
    assert restored.path == "file:///b.pcap"


def test_select_stream_batch_uses_modification_time_then_path():
    entries = [
        FileEntry(path="file:///b.pcap", modification_time_ms=2000, length=10),
        FileEntry(path="file:///a.pcap", modification_time_ms=2000, length=10),
        FileEntry(path="file:///c.pcap", modification_time_ms=3000, length=10),
    ]
    start = PcapStreamOffset(2000, "file:///a.pcap")
    selected = _select_stream_batch(entries, start, max_files_per_batch=1)
    assert [entry.path for entry in selected] == ["file:///b.pcap"]


@patch("cyber_connectors.Pcap._load_file_content")
@patch("cyber_connectors.Pcap._discover_file_entries")
def test_stream_reader_read_returns_rows_and_next_offset(mock_discover, mock_load):
    mock_discover.return_value = [
        FileEntry(path="file:///a.pcap", modification_time_ms=2000, length=10),
        FileEntry(path="file:///b.pcap", modification_time_ms=3000, length=10),
    ]
    mock_load.side_effect = [
        _pcap_bytes([(1700000000.0, _eth_ip_tcp("10.0.0.1", "10.0.0.2", 1111, 80))]),
    ]

    reader = PcapStreamReader({"path": "/tmp/pcaps", "maxFilesPerBatch": "1"}, PCAP_SCHEMA)
    rows_iter, end = reader.read(PcapStreamOffset(0, "").json())

    rows = list(rows_iter)
    restored = PcapStreamOffset.from_json(end)

    assert len(rows) == 1
    assert rows[0][1] == "10.0.0.1"
    assert restored.modification_time_ms == 2000
    assert restored.path == "file:///a.pcap"


@patch("cyber_connectors.Pcap._load_file_content")
@patch("cyber_connectors.Pcap._discover_file_entries")
def test_stream_reader_read_between_offsets_is_deterministic(mock_discover, mock_load):
    mock_discover.return_value = [
        FileEntry(path="file:///a.pcap", modification_time_ms=1000, length=10),
        FileEntry(path="file:///b.pcap", modification_time_ms=2000, length=10),
        FileEntry(path="file:///c.pcap", modification_time_ms=3000, length=10),
    ]
    mock_load.side_effect = [
        _pcap_bytes([(1700000001.0, _eth_ip_tcp("10.0.0.2", "10.0.0.3", 2222, 443))]),
        _pcap_bytes([(1700000002.0, _eth_ip_tcp("10.0.0.3", "10.0.0.4", 3333, 53))]),
    ]

    reader = PcapStreamReader({"path": "/tmp/pcaps"}, PCAP_SCHEMA)
    rows = list(
        reader.readBetweenOffsets(
            PcapStreamOffset(1000, "file:///a.pcap").json(),
            PcapStreamOffset(3000, "file:///c.pcap").json(),
        )
    )

    assert len(rows) == 2
    assert rows[0][1] == "10.0.0.2"
    assert rows[1][1] == "10.0.0.3"


def test_stream_available_now_does_not_reprocess_files(spark_session, tmp_path):
    spark_session.dataSource.register(PcapDataSource)

    input_dir = tmp_path / "pcaps"
    output_dir = tmp_path / "output"
    checkpoint_dir = tmp_path / "checkpoint"
    input_dir.mkdir()

    content = _pcap_bytes([(1700000000.0, _eth_ip_tcp("10.0.0.1", "10.0.0.2", 4444, 80))])
    (input_dir / "sample-1.pcap").write_bytes(content)

    stream_df = spark_session.readStream.format("pcap").option("maxFilesPerBatch", "1").load(str(input_dir))

    query = (
        stream_df.writeStream.format("json")
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start(str(output_dir))
    )
    query.awaitTermination(30)
    assert query.exception() is None

    first_count = spark_session.read.json(str(output_dir)).count()

    query = (
        stream_df.writeStream.format("json")
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start(str(output_dir))
    )
    query.awaitTermination(30)
    assert query.exception() is None

    second_count = spark_session.read.json(str(output_dir)).count()
    assert first_count == 1
    assert second_count == 1
