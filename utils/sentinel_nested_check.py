"""Check that nested columns reach a Log Analytics `dynamic` column as JSON objects.

Writes a few rows with a struct (with a timestamp, an array of structs and a nested struct inside) to the
`Details` (dynamic) column of the `SentinelTIAlerts_CL` table through the `ms-sentinel` writer, then queries
the table back with KQL and checks that the nested fields can be read by name (`Details.ip`, `Details.tags[0].k`).

Run the `write` step once per connector version, then `check`:

    # released 0.0.8 (expected: nested values arrive as arrays, check FAILS for this label)
    uv run --isolated --no-project --with cyber-spark-data-connectors==0.0.8 --with pyspark==4.2.1 \
        --with pyarrow python utils/sentinel_nested_check.py write --label v0.0.8 --tf-dir <terraform dir>
    # the current checkout (expected: JSON objects, check PASSES)
    uv run python utils/sentinel_nested_check.py write --label dev --tf-dir <terraform dir>
    # 2-15 minutes later
    uv run python utils/sentinel_nested_check.py check --tf-dir <terraform dir>

Configuration (DCE endpoint, DCR immutable ID, Log Analytics workspace ID) is read from the Terraform state in
`--tf-dir` (only these non-secret attributes), or given with `--dce`, `--dcr-id` and `--workspace-id`.

Authentication:
- `write` uses DefaultAzureCredential.  Set AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET for the
  service principal that has "Monitoring Metrics Publisher" on the DCR and DCE.
- `check` uses the Azure CLI login (`az login`), which needs read access to the Log Analytics workspace.

The test rows use MatchType = "ConnectorTest", so the "Historical"/"Current" IoC match rules never select them.
"""

import argparse
import json
import sys
import uuid
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path

STREAM = "Custom-SentinelTIAlerts_CL"
TABLE = "SentinelTIAlerts_CL"
MATCH_TYPE = "ConnectorTest"


def tf_config(tf_dir: str) -> dict:
    """Read DCE endpoint, DCR immutable ID and workspace ID from the Terraform state."""
    state = json.loads((Path(tf_dir).expanduser() / "terraform.tfstate").read_text())
    wanted = {
        ("azurerm_monitor_data_collection_endpoint", "this"): ("dce", "logs_ingestion_endpoint"),
        ("azurerm_monitor_data_collection_rule", "this"): ("dcr_id", "immutable_id"),
        ("azurerm_log_analytics_workspace", "this"): ("workspace_id", "workspace_id"),
    }
    config = {}
    for resource in state["resources"]:
        key = (resource["type"], resource["name"])
        if resource.get("mode") == "managed" and key in wanted:
            name, attribute = wanted[key]
            config[name] = resource["instances"][0]["attributes"][attribute]
    return config


def get_config(args) -> dict:
    config = tf_config(args.tf_dir) if args.tf_dir else {}
    for name in ("dce", "dcr_id", "workspace_id"):
        if getattr(args, name):
            config[name] = getattr(args, name)
    return config


def write(args, config: dict):
    from pyspark.sql import SparkSession

    import cyber_connectors
    from cyber_connectors import MicrosoftSentinelDataSource

    connector = f"cyber-spark-data-connectors {version('cyber-spark-data-connectors')} ({cyber_connectors.__file__})"
    print(f"Using {connector}")
    for name in ("dce", "dcr_id"):
        if not config.get(name):
            sys.exit(f"missing {name}: pass --tf-dir or --{name.replace('_', '-')}")

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.dataSource.register(MicrosoftSentinelDataSource)

    run_id = f"{args.label}-{uuid.uuid4().hex[:8]}"
    # Same schema for all rows: a full struct, a struct with empty/null nested parts, and a null struct.
    df = spark.sql(f"""
      SELECT
        current_timestamp()                       AS TimeGenerated,
        timestamp'2026-11-03 10:00:00'            AS OriginTimestamp,
        id                                        AS ConfidenceScore,
        concat('{run_id}-', id)                   AS ExternalIndicatorId,
        case_name                                 AS IndicatorType,
        '{args.label}'                            AS Indicator,
        '{MATCH_TYPE}'                            AS MatchType,
        details                                   AS Details
      FROM VALUES
        (1, 'full', named_struct(
            'ip', '10.0.0.1', 'port', 443, 'seen', timestamp'2026-11-03 10:00:00',
            'tags', array(named_struct('k', 'x'), named_struct('k', 'y')),
            'inner', named_struct('a', 'b'))),
        (2, 'empty_parts', named_struct(
            'ip', '10.0.0.2', 'port', 80, 'seen', CAST(NULL AS timestamp),
            'tags', CAST(array() AS array<struct<k: string>>),
            'inner', CAST(NULL AS struct<a: string>))),
        (3, 'null_struct', CAST(NULL AS struct<ip: string, port: int, seen: timestamp,
            tags: array<struct<k: string>>, inner: struct<a: string>>))
      AS t(id, case_name, details)
    """)
    df.write.format("ms-sentinel").mode("append").options(
        dce=config["dce"], dcr_id=config["dcr_id"], dcs=STREAM, azure_default_credential="true"
    ).save()
    print(f"Wrote {df.count()} rows to {STREAM} with Indicator = '{args.label}', ExternalIndicatorId prefix '{run_id}'")
    print("Run the `check` step in a few minutes (the first rows can take up to ~15 minutes to appear).")


# Expected KQL values per test case: (gettype(Details), ip, port, seen, first tag, inner.a)
EXPECTED = {
    "full": ("dictionary", "10.0.0.1", 443, "2026-11-03T10:00:00", "x", "b"),
    "empty_parts": ("dictionary", "10.0.0.2", 80, None, None, None),
    "null_struct": (None, None, None, None, None, None),
}


def check(args, config: dict):
    from azure.identity import AzureCliCredential
    from azure.monitor.query import LogsQueryClient, LogsQueryStatus

    if not config.get("workspace_id"):
        sys.exit("missing workspace_id: pass --tf-dir or --workspace-id")
    query = f"""
{TABLE}
| where MatchType == "{MATCH_TYPE}"
| summarize arg_max(TimeGenerated, *) by Indicator, IndicatorType
| extend dtype = gettype(Details), ip = tostring(Details.ip), port = toint(Details.port),
         seen = todatetime(Details.seen), tag = tostring(Details.tags[0].k), inner_a = tostring(Details.inner.a)
| project TimeGenerated, Indicator, IndicatorType, dtype, ip, port, seen, tag, inner_a, Details
| order by Indicator asc, IndicatorType asc
"""
    client = LogsQueryClient(AzureCliCredential())
    response = client.query_workspace(config["workspace_id"], query, timespan=timedelta(days=args.days))
    if response.status != LogsQueryStatus.SUCCESS:
        sys.exit(f"query failed: {response}")
    rows = [dict(zip(response.tables[0].columns, r, strict=True)) for r in response.tables[0].rows]
    if not rows:
        sys.exit(f"no {MATCH_TYPE} rows in the last {args.days} day(s) yet; ingestion can take up to ~15 minutes")

    results = {}
    for r in rows:
        dtype = r["dtype"] if r["dtype"] not in ("null", "") else None
        seen = r["seen"].strftime("%Y-%m-%dT%H:%M:%S") if r["seen"] else None
        actual = (dtype, r["ip"] or None, r["port"], seen, r["tag"] or None, r["inner_a"] or None)
        ok = actual == EXPECTED.get(r["IndicatorType"])
        results.setdefault(r["Indicator"], []).append(ok)
        status = "PASS" if ok else "FAIL"
        print(f"{status}  {r['Indicator']:<12} {r['IndicatorType']:<12} Details={json.dumps(r['Details'])}")
        if not ok:
            print(f"      expected {EXPECTED.get(r['IndicatorType'])}\n      actual   {actual}")
    print()
    for label, oks in sorted(results.items()):
        print(f"{label:<12} {'PASS' if all(oks) else 'FAIL'} ({sum(oks)}/{len(oks)} cases)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", choices=["write", "check"])
    parser.add_argument("--label", default="dev", help="label for this run, e.g. the connector version")
    parser.add_argument("--tf-dir", help="Terraform directory with terraform.tfstate")
    parser.add_argument("--dce", help="DCE logs ingestion endpoint (overrides Terraform state)")
    parser.add_argument("--dcr-id", help="DCR immutable ID (overrides Terraform state)")
    parser.add_argument("--workspace-id", help="Log Analytics workspace (customer) ID (overrides Terraform state)")
    parser.add_argument("--days", type=int, default=1, help="how far back `check` looks (default: 1 day)")
    args = parser.parse_args()
    config = get_config(args)
    if args.step == "write":
        write(args, config)
    else:
        check(args, config)


if __name__ == "__main__":
    main()
