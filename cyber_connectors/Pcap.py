"""PCAP data source for batch and micro-batch Spark reads."""
# ruff: noqa: N999

import gzip
import io
import json
import socket
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional, cast

import dpkt  # type: ignore[import-untyped]
import dpkt.loopback  # type: ignore[import-untyped]
import dpkt.sll  # type: ignore[import-untyped]
from pyspark.sql import Row
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

PCAP_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("src_ip", StringType(), nullable=False),
        StructField("dst_ip", StringType(), nullable=False),
        StructField("src_port", IntegerType(), nullable=True),
        StructField("dst_port", IntegerType(), nullable=True),
        StructField("protocol", StringType(), nullable=False),
        StructField("pkt_len", IntegerType(), nullable=False),
        StructField(
            "http",
            StructType(
                [
                    StructField("method", StringType(), nullable=True),
                    StructField("uri", StringType(), nullable=True),
                    StructField("host", StringType(), nullable=True),
                    StructField("status_code", IntegerType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "dns",
            StructType(
                [
                    StructField("query", StringType(), nullable=True),
                    StructField("qtype", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField("tls", StructType([StructField("sni", StringType(), nullable=True)]), nullable=True),
        StructField(
            "icmp",
            StructType(
                [
                    StructField("type_code", IntegerType(), nullable=True),
                    StructField("type_name", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "dhcp",
            StructType(
                [
                    StructField("msg_type", StringType(), nullable=True),
                    StructField("client_mac", StringType(), nullable=True),
                    StructField("hostname", StringType(), nullable=True),
                    StructField("requested_ip", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "ntp",
            StructType(
                [
                    StructField("version", IntegerType(), nullable=True),
                    StructField("mode", StringType(), nullable=True),
                    StructField("stratum", IntegerType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)

_DNS_QTYPE_NAMES = {
    1: "A",
    2: "NS",
    5: "CNAME",
    6: "SOA",
    12: "PTR",
    15: "MX",
    16: "TXT",
    28: "AAAA",
    33: "SRV",
    255: "ANY",
}

_ICMP_TYPE_NAMES = {
    0: "echo-reply",
    3: "dest-unreachable",
    4: "source-quench",
    5: "redirect",
    8: "echo-request",
    9: "router-advertisement",
    10: "router-solicitation",
    11: "time-exceeded",
    12: "parameter-problem",
    13: "timestamp-request",
    14: "timestamp-reply",
}

_NTP_MODE_NAMES = {
    1: "sym-active",
    2: "sym-passive",
    3: "client",
    4: "server",
    5: "broadcast",
    6: "control",
    7: "private",
}

_DHCP_MSG_TYPE_NAMES = {
    1: "DISCOVER",
    2: "OFFER",
    3: "REQUEST",
    4: "DECLINE",
    5: "ACK",
    6: "NAK",
    7: "RELEASE",
    8: "INFORM",
}

_LINK_LOOPBACK = 0
_LINK_ETHERNET = 1
_LINK_RAW = 101
_LINK_SLL = 113
_LINK_IPV4 = 228
_LINK_IPV6 = 229


@dataclass(frozen=True)
class FileEntry:
    """Metadata for a discovered file."""

    path: str
    modification_time_ms: int
    length: int


@dataclass(frozen=True)
class PcapInputPartition(InputPartition):
    """A single PCAP input partition with file content embedded."""

    path: str
    modification_time_ms: int
    length: int
    content: bytes


@dataclass(frozen=True)
class PcapStreamOffset:
    """Offset watermark for PCAP streaming."""

    modification_time_ms: int
    path: str
    version: int = 1

    def json(self) -> str:
        """Serialize the offset to JSON."""
        return json.dumps(
            {
                "version": self.version,
                "modification_time_ms": self.modification_time_ms,
                "path": self.path,
            }
        )

    @staticmethod
    def from_json(value: str | dict) -> "PcapStreamOffset":
        """Deserialize an offset from JSON or a dict."""
        data = json.loads(value) if isinstance(value, str) else value
        return PcapStreamOffset(
            modification_time_ms=int(data["modification_time_ms"]),
            path=data["path"],
            version=int(data.get("version", 1)),
        )


def _extract_ip(raw: bytes, link_type: int):
    """Return the IP/IPv6 layer from a raw frame, or None if unsupported/malformed."""
    try:
        if link_type == _LINK_ETHERNET:
            eth = dpkt.ethernet.Ethernet(raw)
            ip = eth.data
        elif link_type == _LINK_LOOPBACK:
            loopback = dpkt.loopback.Loopback(raw)
            ip = loopback.data
        elif link_type == _LINK_SLL:
            sll = dpkt.sll.SLL(raw)
            ip = sll.data
        elif link_type in (_LINK_RAW, _LINK_IPV4):
            ip = dpkt.ip.IP(raw)
        elif link_type == _LINK_IPV6:
            ip = dpkt.ip6.IP6(raw)
        else:
            return None
        return ip if isinstance(ip, (dpkt.ip.IP, dpkt.ip6.IP6)) else None
    except (dpkt.UnpackError, ValueError, IndexError, struct.error):
        return None


def _ip_to_str(raw: bytes) -> str:
    if len(raw) == 4:
        return socket.inet_ntop(socket.AF_INET, raw)
    return socket.inet_ntop(socket.AF_INET6, raw)


def _extract_http(payload: bytes) -> tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """Try to parse HTTP request or response from TCP payload."""
    try:
        req = dpkt.http.Request(payload)
        host = req.headers.get("host")
        if isinstance(host, list):
            host = host[0] if host else None
        return req.method, req.uri, host, None
    except (dpkt.UnpackError, ValueError):
        pass

    try:
        resp = dpkt.http.Response(payload)
        return None, None, None, int(resp.status)
    except (dpkt.UnpackError, ValueError, TypeError):
        return None, None, None, None


def _extract_dns(payload: bytes) -> tuple[Optional[str], Optional[str]]:
    """Try to extract the first DNS question name and type."""
    try:
        msg = dpkt.dns.DNS(payload)
        if msg.qr == dpkt.dns.DNS_Q and msg.qd:
            question = msg.qd[0]
            qtype = _DNS_QTYPE_NAMES.get(question.type, str(question.type))
            return question.name, qtype
    except (dpkt.UnpackError, ValueError):
        pass
    return None, None


def _extract_tls_sni(payload: bytes) -> Optional[str]:
    """Extract SNI from a TLS ClientHello without full TLS parsing."""
    try:
        if len(payload) < 5 or payload[0] != 22:
            return None
        record_len = struct.unpack("!H", payload[3:5])[0]
        handshake = payload[5 : 5 + record_len]
        if not handshake or handshake[0] != 1:
            return None
        offset = 4 + 2 + 32
        if offset >= len(handshake):
            return None
        sid_len = handshake[offset]
        offset += 1 + sid_len
        if offset + 2 > len(handshake):
            return None
        cipher_len = struct.unpack("!H", handshake[offset : offset + 2])[0]
        offset += 2 + cipher_len
        if offset + 1 > len(handshake):
            return None
        comp_len = handshake[offset]
        offset += 1 + comp_len
        if offset + 2 > len(handshake):
            return None
        ext_total = struct.unpack("!H", handshake[offset : offset + 2])[0]
        offset += 2
        end = offset + ext_total
        while offset + 4 <= end and offset + 4 <= len(handshake):
            ext_type = struct.unpack("!H", handshake[offset : offset + 2])[0]
            ext_len = struct.unpack("!H", handshake[offset + 2 : offset + 4])[0]
            offset += 4
            if ext_type == 0 and offset + 5 <= len(handshake):
                name_len = struct.unpack("!H", handshake[offset + 3 : offset + 5])[0]
                return handshake[offset + 5 : offset + 5 + name_len].decode("ascii", errors="ignore")
            offset += ext_len
    except (IndexError, struct.error):
        return None
    return None


def _extract_dhcp(payload: bytes) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extract DHCP message type, client MAC, hostname, and requested IP from a DHCP payload."""
    try:
        pkt = dpkt.dhcp.DHCP(payload)
        mac = ":".join(f"{byte:02x}" for byte in pkt.chaddr[:6]) if pkt.chaddr else None
        msg_type = hostname = requested_ip = None
        for opt_type, opt_data in pkt.opts:
            if opt_type == dpkt.dhcp.DHCP_OPT_MSGTYPE and opt_data:
                msg_type = _DHCP_MSG_TYPE_NAMES.get(opt_data[0], str(opt_data[0]))
            elif opt_type == dpkt.dhcp.DHCP_OPT_HOSTNAME:
                hostname = opt_data.decode("utf-8", errors="ignore")
            elif opt_type == dpkt.dhcp.DHCP_OPT_REQ_IP and len(opt_data) == 4:
                requested_ip = socket.inet_ntoa(opt_data)
        return msg_type, mac, hostname, requested_ip
    except (dpkt.UnpackError, ValueError):
        return None, None, None, None


def _extract_ntp(payload: bytes) -> tuple[Optional[int], Optional[str], Optional[int]]:
    """Extract NTP version, mode name, and stratum from a NTP payload."""
    try:
        pkt = dpkt.ntp.NTP(payload)
        mode = _NTP_MODE_NAMES.get(pkt.mode, str(pkt.mode))
        return pkt.v, mode, pkt.stratum
    except (dpkt.UnpackError, ValueError):
        return None, None, None


def _parse_pcap_reader(reader: dpkt.pcap.Reader) -> Iterator[tuple]:
    """Yield one tuple per IP packet from a dpkt PCAP reader."""
    link_type = reader.datalink()
    iterator = iter(reader)

    while True:
        try:
            ts, raw = next(iterator)
        except StopIteration:
            break
        except (EOFError, dpkt.UnpackError, ValueError):
            break

        ip = _extract_ip(raw, link_type)
        if ip is None:
            continue

        src_ip = _ip_to_str(ip.src)
        dst_ip = _ip_to_str(ip.dst)
        pkt_len = len(raw)
        timestamp = datetime.fromtimestamp(ts, tz=timezone.utc)

        transport = ip.data
        src_port: Optional[int] = None
        dst_port: Optional[int] = None
        protocol = "other"
        http: Optional[Row] = None
        dns: Optional[Row] = None
        tls: Optional[Row] = None
        icmp: Optional[Row] = None
        dhcp: Optional[Row] = None
        ntp: Optional[Row] = None

        if isinstance(transport, dpkt.tcp.TCP):
            protocol = "tcp"
            src_port = transport.sport
            dst_port = transport.dport
            payload = bytes(transport.data)
            if payload:
                if dst_port == 443 or src_port == 443:
                    sni = _extract_tls_sni(payload)
                    if sni is not None:
                        tls = Row(sni=sni)
                else:
                    method, uri, host, status_code = _extract_http(payload)
                    if any(value is not None for value in (method, uri, host, status_code)):
                        http = Row(method=method, uri=uri, host=host, status_code=status_code)
        elif isinstance(transport, dpkt.udp.UDP):
            protocol = "udp"
            src_port = transport.sport
            dst_port = transport.dport
            payload = bytes(transport.data)
            if dst_port == 53 or src_port == 53:
                query, qtype = _extract_dns(payload)
                if query is not None:
                    dns = Row(query=query, qtype=qtype)
            elif dst_port in (67, 68) or src_port in (67, 68):
                msg_type, client_mac, hostname, requested_ip = _extract_dhcp(payload)
                if msg_type is not None:
                    dhcp = Row(
                        msg_type=msg_type,
                        client_mac=client_mac,
                        hostname=hostname,
                        requested_ip=requested_ip,
                    )
            elif dst_port == 123 or src_port == 123:
                version, mode, stratum = _extract_ntp(payload)
                if version is not None:
                    ntp = Row(version=version, mode=mode, stratum=stratum)
        elif isinstance(transport, dpkt.icmp.ICMP):
            protocol = "icmp"
            icmp = Row(type_code=transport.type, type_name=_ICMP_TYPE_NAMES.get(transport.type, str(transport.type)))

        yield (
            timestamp,
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            protocol,
            pkt_len,
            http,
            dns,
            tls,
            icmp,
            dhcp,
            ntp,
        )


def _open_pcap_bytes(content: bytes):
    """Open raw or gzipped PCAP content and return a reader."""
    if not content:
        return None
    raw = gzip.decompress(content) if content[:2] == b"\x1f\x8b" else content
    return dpkt.pcap.Reader(io.BytesIO(raw))


def _parse_pcap_content(content: bytes) -> Iterator[tuple]:
    """Parse PCAP bytes into row tuples."""
    reader = _open_pcap_bytes(content)
    if reader is None:
        return iter(())
    return _parse_pcap_reader(reader)


def _bool_option(options: dict, key: str, default: bool = False) -> bool:
    return options.get(key, str(default)).lower() == "true"


def _parse_optional_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an optional ISO 8601 timestamp."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"Invalid timestamp '{value}'. Expected ISO 8601 format.") from error


def _get_active_spark_session():
    """Return the active Spark session or raise."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession() or SparkSession._instantiatedSession
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    if spark is None:
        raise RuntimeError("An active SparkSession is required for PCAP file discovery")
    return spark


def _file_pattern(options: dict[str, str]) -> Optional[str]:
    return options.get("fileNamePattern") or options.get("pathGlobFilter")


def _build_binary_file_reader(options: dict[str, str]):
    """Build a binaryFile reader with discovery options applied."""
    spark = _get_active_spark_session()
    reader = spark.read.format("binaryFile")

    if options.get("recursiveFileLookup") is not None:
        reader = reader.option("recursiveFileLookup", options["recursiveFileLookup"])

    pattern = _file_pattern(options)
    if pattern:
        reader = reader.option("pathGlobFilter", pattern)

    return reader


def _discover_file_entries(options: dict[str, str]) -> list[FileEntry]:
    """Discover candidate files using Spark's binaryFile source."""
    path = options.get("path")
    if not path:
        raise ValueError("Option 'path' is required")

    rows = (
        _build_binary_file_reader(options)
        .load(path)
        .select("path", "modificationTime", "length")
        .collect()
    )
    return [
        FileEntry(
            path=row.path,
            modification_time_ms=int(row.modificationTime.timestamp() * 1000),
            length=int(row.length),
        )
        for row in rows
    ]


def _filter_file_entries(
    entries: list[FileEntry],
    modified_after: Optional[datetime],
    modified_before: Optional[datetime],
) -> list[FileEntry]:
    """Filter discovered entries by modification time."""
    filtered = []
    modified_after_ms = int(modified_after.timestamp() * 1000) if modified_after else None
    modified_before_ms = int(modified_before.timestamp() * 1000) if modified_before else None

    for entry in entries:
        if modified_after_ms is not None and entry.modification_time_ms <= modified_after_ms:
            continue
        if modified_before_ms is not None and entry.modification_time_ms >= modified_before_ms:
            continue
        filtered.append(entry)
    return filtered


def _apply_settling_delay(entries: list[FileEntry], settling_delay_seconds: int) -> list[FileEntry]:
    """Filter out files that are too new for streaming."""
    if settling_delay_seconds <= 0:
        return entries
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settling_delay_seconds)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    return [entry for entry in entries if entry.modification_time_ms <= cutoff_ms]


def _entry_sort_key(entry: FileEntry) -> tuple[int, str]:
    return entry.modification_time_ms, entry.path


def _select_stream_batch(
    entries: list[FileEntry],
    start_offset: PcapStreamOffset,
    max_files_per_batch: Optional[int],
) -> list[FileEntry]:
    """Select the next streaming batch using stable file ordering."""
    selected = [
        entry
        for entry in sorted(entries, key=_entry_sort_key)
        if _entry_sort_key(entry) > (start_offset.modification_time_ms, start_offset.path)
    ]
    if max_files_per_batch is not None:
        return selected[:max_files_per_batch]
    return selected


def _entries_between_offsets(
    entries: list[FileEntry],
    start_offset: PcapStreamOffset,
    end_offset: PcapStreamOffset,
) -> list[FileEntry]:
    """Return entries inside a deterministic offset range."""
    start_key = (start_offset.modification_time_ms, start_offset.path)
    end_key = (end_offset.modification_time_ms, end_offset.path)
    return [
        entry
        for entry in sorted(entries, key=_entry_sort_key)
        if start_key < _entry_sort_key(entry) <= end_key
    ]


def _load_file_content(path: str, ignore_missing_files: bool) -> Optional[bytes]:
    """Load a single file's binary content with Spark."""
    try:
        rows = _get_active_spark_session().read.format("binaryFile").load(path).select("content").collect()
    except Exception:
        if ignore_missing_files:
            return None
        raise

    if not rows:
        if ignore_missing_files:
            return None
        raise FileNotFoundError(path)

    return bytes(rows[0].content)


def _materialize_partitions(
    entries: list[FileEntry],
    ignore_missing_files: bool,
) -> list[PcapInputPartition]:
    """Load selected files and convert them into input partitions."""
    partitions = []
    for entry in entries:
        content = _load_file_content(entry.path, ignore_missing_files)
        if content is None:
            continue
        partitions.append(
            PcapInputPartition(
                path=entry.path,
                modification_time_ms=entry.modification_time_ms,
                length=entry.length,
                content=content,
            )
        )
    return partitions


def _parse_partition_content(content: bytes, ignore_corrupt_files: bool) -> Iterator[tuple]:
    """Parse partition content into output rows."""
    try:
        yield from _parse_pcap_content(content)
    except (OSError, EOFError, ValueError, dpkt.Error) as error:
        if ignore_corrupt_files:
            return
        raise ValueError("Failed to parse PCAP content") from error


class PcapBatchReader(DataSourceReader):
    """Stateless batch reader for PCAP files."""

    def __init__(self, options, schema: StructType):
        """Initialize the batch reader."""
        self.options = options
        self.schema = schema
        self.ignore_corrupt_files = _bool_option(options, "ignoreCorruptFiles")
        self.ignore_missing_files = _bool_option(options, "ignoreMissingFiles")
        self.mode = options.get("mode", "FAILFAST").upper()
        self.recursive_file_lookup = _bool_option(options, "recursiveFileLookup")
        self.file_pattern = _file_pattern(options)
        self.modified_after = _parse_optional_timestamp(options.get("modifiedAfter"))
        self.modified_before = _parse_optional_timestamp(options.get("modifiedBefore"))

        if self.mode == "PERMISSIVE":
            self.ignore_corrupt_files = True

    def partitions(self):
        """Plan batch partitions from discovered files."""
        entries = _discover_file_entries(self.options)
        selected = _filter_file_entries(entries, self.modified_after, self.modified_before)
        return _materialize_partitions(selected, self.ignore_missing_files)

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        """Read rows from a single batch partition."""
        pcap_partition = cast(PcapInputPartition, partition)
        yield from _parse_partition_content(pcap_partition.content, self.ignore_corrupt_files)


class PcapStreamReader(SimpleDataSourceStreamReader):
    """Micro-batch stream reader with offset-only progress tracking."""

    def __init__(self, options, schema: StructType):
        """Initialize the stream reader."""
        self.options = options
        self.schema = schema
        self.ignore_corrupt_files = _bool_option(options, "ignoreCorruptFiles")
        self.ignore_missing_files = _bool_option(options, "ignoreMissingFiles")
        self.mode = options.get("mode", "FAILFAST").upper()
        self.max_files_per_batch = int(options["maxFilesPerBatch"]) if options.get("maxFilesPerBatch") else None
        self.modified_after = _parse_optional_timestamp(options.get("modifiedAfter"))
        self.modified_before = _parse_optional_timestamp(options.get("modifiedBefore"))
        self.settling_delay_seconds = int(options.get("settling_delay_seconds", "0"))

        if self.mode == "PERMISSIVE":
            self.ignore_corrupt_files = True

    def initialOffset(self):  # noqa: N802
        """Return the initial streaming offset."""
        return PcapStreamOffset(0, "").json()

    def read(self, start):
        """Read the next available micro-batch from the current offset."""
        start_offset = PcapStreamOffset.from_json(start)
        entries = _discover_file_entries(self.options)
        filtered = _filter_file_entries(entries, self.modified_after, self.modified_before)
        eligible = _apply_settling_delay(filtered, self.settling_delay_seconds)
        selected = _select_stream_batch(eligible, start_offset, self.max_files_per_batch)
        partitions = _materialize_partitions(selected, self.ignore_missing_files)
        rows = []
        for partition in partitions:
            rows.extend(_parse_partition_content(partition.content, self.ignore_corrupt_files))

        if not selected:
            return iter(rows), start_offset.json()

        end_offset = PcapStreamOffset(
            modification_time_ms=selected[-1].modification_time_ms,
            path=selected[-1].path,
        )
        return iter(rows), end_offset.json()

    def readBetweenOffsets(self, start, end):  # noqa: N802
        """Replay a deterministic range of files between two offsets."""
        start_offset = PcapStreamOffset.from_json(start)
        end_offset = PcapStreamOffset.from_json(end)
        entries = _discover_file_entries(self.options)
        filtered = _filter_file_entries(entries, self.modified_after, self.modified_before)
        selected = _entries_between_offsets(filtered, start_offset, end_offset)
        partitions = _materialize_partitions(selected, self.ignore_missing_files)
        rows = []
        for partition in partitions:
            rows.extend(_parse_partition_content(partition.content, self.ignore_corrupt_files))
        return iter(rows)

    def commit(self, end):
        """No-op; Spark persists the source offset in the query checkpoint."""
        return None


class PcapDataSource(DataSource):
    """Data source that reads PCAP files into packet rows."""

    @classmethod
    def name(cls) -> str:
        """Return the registered source name."""
        return "pcap"

    def schema(self) -> StructType:
        """Return the fixed PCAP output schema."""
        return PCAP_SCHEMA

    def reader(self, schema: StructType) -> DataSourceReader:
        """Build the batch reader."""
        return PcapBatchReader(self.options, schema)

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:  # noqa: N802
        """Build the simple streaming reader."""
        return PcapStreamReader(self.options, schema)
