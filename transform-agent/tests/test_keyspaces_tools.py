"""Offline tests for the Keyspaces tool layer. Requires ./build.sh and Node.js; no AWS access."""

import importlib
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path

import pytest

AGENT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = AGENT_DIR.parent
FIXTURES = REPO_ROOT / "test-fixtures"
sys.path.insert(0, str(AGENT_DIR / "src"))


@pytest.fixture()
def tools(tmp_path, monkeypatch):
    monkeypatch.setenv("KEYSPACES_WORK_DIR", str(tmp_path))
    import keyspaces_tools

    return importlib.reload(keyspaces_tools)


def test_manual_estimate(tools):
    res = tools.estimate_keyspaces_cost(
        region="us-east-1", reads_per_second=1000, writes_per_second=500, storage_gb=100
    )
    assert "error" not in res, res
    assert Path(res["output_path"]).exists()
    assert "report_data" not in res["result"]
    assert res["result"]["on_demand"]["total"] > res["result"]["provisioned"]["total"] > 0


def test_multi_row_reads_aggregate_bytes(tools):
    def read_units(row_bytes, rows):
        res = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=100, writes_per_second=1,
                                            storage_gb=1, row_size_bytes=row_bytes, rows_per_read=rows,
                                            output_name=f"rr-{row_bytes}-{rows}.json")
        assert "error" not in res, res
        return res["result"]["units_per_operation"]["read"], res["result"]["on_demand"]["reads_strong"]

    one_units, one_cost = read_units(1024, 1)
    assert one_units == 1
    assert read_units(1024, 4) == (1, one_cost)        # 4 KB across 4 rows is still 1 unit
    assert read_units(1500, 3)[0] == 2                  # 4,500 B -> 2 units (per-row would be 3)
    assert read_units(1024, 10)[0] == 3                 # 10 KB -> 3 units
    assert read_units(1024, 1100)[0] == 256 + 19        # 1.07 MB: one full 1 MB page + 76 KB
    # Each 1 MB page is a new request: 1 MB + 100 B is a full page (256) plus 1 unit for the 100 B page.
    assert read_units(524_338, 2)[0] == 256 + 1


def _formula_total(formulas: dict, prefix: str) -> float:
    total = 0.0
    for by_ks in formulas.values():
        for lines in by_ks.values():
            line = next(l for l in lines if l.startswith(prefix))
            total += float(re.search(r"= \$([\d,.]+)$", line).group(1).replace(",", ""))
    return total


def test_explain_formulas_add_up_to_estimate_totals(tools):
    manual = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=1000, writes_per_second=500,
                                           storage_gb=100, ttl_deletes_per_second=20, rows_per_read=10,
                                           dc_count=2, explain=True, output_name="explain-manual.json")
    cluster = tools.estimate_cost_from_cassandra_diagnostics(
        region="us-east-1", directory=str(MULTI_DC), dc_regions={"dc-eu": "eu-west-1"}, rows_per_read=5,
        explain=True, output_name="explain-cluster.json")
    for res in (manual, cluster):
        assert "error" not in res, res
        result = res["result"]
        f = result["formulas"]
        assert abs(_formula_total(f, "On-demand total") - result["on_demand"]["total"]) < 0.01 * len(f) + 0.01
        assert abs(_formula_total(f, "Provisioned total") - result["provisioned"]["total"]) < 0.01 * len(f) + 0.01
    lines = manual["result"]["formulas"]["dc-1"]["default"]
    assert "Read units per read = ceil(1,024 × 10 rows / 4,096) = 3" in lines
    assert any(l.startswith("PITR = 100 GB × $") for l in lines)  # multi-DC defaults PITR on
    conv = cluster["result"]["formulas"]["us-east-1"]["bench"]
    assert conv[0] == "Conversion: N = 3 nodes, RF = 3" and conv[1].startswith("Writes/s = ")


def test_workload_spec_conversion_and_formulas(tools):
    spec = {"source": "mongodb", "regions": ["us-east-1", "eu-west-1"], "keyspaces": [
        {"name": "catalog", "tables": [
            {"name": "products", "reads_per_second": 900, "writes_per_second": 30, "avg_row_bytes": 2048,
             "rows_per_read": 20, "stored_bytes": 3221225472000, "compression_ratio": 0.5, "storage_copies": 3,
             "request_copies": 3}]},
        {"name": "events", "pitr": False, "tables": [
            {"name": "clicks", "reads_per_second": 10, "writes_per_second": 400, "avg_row_bytes": 300,
             "ttl_deletes_per_second": 400, "rows": 5_000_000_000}]},
    ]}
    res = tools.estimate_from_workload_spec(spec=spec, explain=True)
    assert "error" not in res, res
    data = json.loads(Path(res["output_path"]).read_text())
    eu = data["report_data"]["estimateResults"]["eu-west-1"]
    assert eu["catalog"]["reads_per_second"] == 300 and eu["catalog"]["writes_per_second"] == 10
    assert eu["catalog"]["uncompressed_single_replica_gb"] == 2000
    assert eu["catalog"]["use_backup"] is True and eu["events"]["use_backup"] is False  # multi-Region default / override
    lines = res["result"]["formulas"]["eu-west-1"]["catalog"]
    assert "catalog.products: reads/s = 900 observed ÷ 3 request copies = 300" in lines
    assert any(l.startswith("catalog.products: storage = 3,221,225,472,000 B stored ÷ 0.5") for l in lines)
    assert "Read units per read = ceil(2,048 × 20 rows / 4,096) = 10" in lines
    assert abs(_formula_total(res["result"]["formulas"], "On-demand total") - res["result"]["on_demand"]["total"]) < 0.05
    # The estimate feeds CloudFormation like any other, with the spec's keyspace names.
    cql = tools.save_input_file(file_name="spec/model.cql", content=(
        "CREATE KEYSPACE catalog WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east-1': '3', 'eu-west-1': '3'};\n"
        "CREATE TABLE catalog.products (id uuid PRIMARY KEY, name text);\n"))["path"]
    cfn = tools.generate_keyspaces_cloudformation(schema_path=cql, estimate_path=res["output_path"],
                                                  billing_mode="provisioned", output_name="spec.yaml")
    text = Path(cfn["output_path"]).read_text()
    assert "ReadCapacityUnits: 4286" in text and "- eu-west-1" in text  # 300 x 10 units / 0.70
    _cfn_lint(cfn["output_path"])


def test_reads_and_pitr_can_differ_per_region(tools):
    def by_dc(res):
        return {dc: next(iter(ks.values())) for dc, ks in
                json.loads(Path(res["output_path"]).read_text())["report_data"]["estimateResults"].items()}

    # Workload spec: eu-west-1 reads less and has PITR off; writes and storage are replicated.
    spec = {"regions": ["us-east-1", {"name": "eu-west-1", "pitr": False}], "keyspaces": [{"name": "shop", "tables": [
        {"name": "orders", "reads_per_second": 1000, "reads_per_second_by_region": {"eu-west-1": 200},
         "writes_per_second": 100, "avg_row_bytes": 1024, "rows": 104857600}]}]}
    res = tools.estimate_from_workload_spec(spec=spec, output_name="per-region-spec.json")
    assert "error" not in res, res
    d = by_dc(res)
    assert (d["us-east-1"]["reads_per_second"], d["eu-west-1"]["reads_per_second"]) == (1000, 200)
    assert d["us-east-1"]["writes_per_second"] == d["eu-west-1"]["writes_per_second"] == 100
    assert d["us-east-1"]["uncompressed_single_replica_gb"] == d["eu-west-1"]["uncompressed_single_replica_gb"] == 100
    assert res["result"]["pitr_by_datacenter"] == {"us-east-1": True, "eu-west-1": False}

    # calculate.ts: --reads-override + --pitr-override.
    res = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=1000, writes_per_second=100, storage_gb=100,
                                        dc_count=2, reads_override="dc-2=200", pitr_by_region={"dc-2": False},
                                        output_name="per-region-manual.json")
    assert "error" not in res, res
    assert res["result"]["inputs"]["pitr_by_datacenter"] == {"dc-1": True, "dc-2": False}
    assert by_dc(res)["dc-2"]["reads_per_second"] == 200

    # Cassandra: own sample per DC already differs; PITR per DC via pitr_by_region.
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=str(MULTI_DC),
                                                         dc_regions={"dc-eu": "eu-west-1"},
                                                         pitr_by_region={"dc-eu": False, "us-east-1": True},
                                                         output_name="per-region-cass.json")
    assert "error" not in res, res
    assert res["result"]["pitr_by_datacenter"] == {"us-east-1": True, "dc-eu": False}


def test_datacenters_in_one_region_are_priced_once(tools):
    two_regions = tools.estimate_cost_from_cassandra_diagnostics(
        region="us-east-1", directory=str(MULTI_DC), dc_regions={"dc-eu": "eu-west-1"}, output_name="two.json")
    one_region = tools.estimate_cost_from_cassandra_diagnostics(
        region="us-east-1", directory=str(MULTI_DC), dc_regions={"dc-eu": "us-east-1"}, explain=True,
        output_name="one.json")
    assert "error" not in one_region, one_region
    r = one_region["result"]
    assert r["merged_datacenters"] == {"us-east-1": ["us-east-1", "dc-eu"]}
    assert [d["name"] for d in r["datacenters"]] == ["us-east-1"] and r["datacenters"][0]["nodeCount"] == 6
    two = _estimates_by_dc(two_regions["output_path"])
    one = _estimates_by_dc(one_region["output_path"])["us-east-1"]
    # Reads add up; replicated writes and storage count once (not doubled).
    assert one["reads_per_second"] == two["us-east-1"]["reads_per_second"] + two["dc-eu"]["reads_per_second"]
    assert one["writes_per_second"] == max(two["us-east-1"]["writes_per_second"], two["dc-eu"]["writes_per_second"])
    assert one["uncompressed_single_replica_gb"] == two["us-east-1"]["uncompressed_single_replica_gb"]
    assert "replicated to the same Region twice" in r["formulas"]["us-east-1"]["bench"][0]


def _mv_cluster(tmp_path, view_space_bytes: int) -> str:
    cluster = tmp_path / "mv"
    (cluster / "us-east-1").mkdir(parents=True)
    (cluster / "schema.cql").write_text(
        "CREATE KEYSPACE shop WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east-1': '3'};\n"
        "CREATE TABLE shop.orders (id uuid PRIMARY KEY, customer_id uuid, total decimal);\n"
        "CREATE MATERIALIZED VIEW shop.orders_by_customer AS SELECT customer_id, id, total FROM shop.orders "
        "WHERE customer_id IS NOT NULL AND id IS NOT NULL PRIMARY KEY (customer_id, id);\n")
    nodes = "\n".join(f"UN  10.0.0.{i}  1 GiB  256  ?  0000000{i}-0000-0000-0000-000000000000  rack1" for i in (1, 2, 3))
    (cluster / "status.txt").write_text("Datacenter: us-east-1\n=====================\nStatus=Up/Down\n"
                                        "|/ State=Normal/Leaving/Joining/Moving\n--  Address  Load  Tokens  Owns  Host ID  Rack\n" + nodes + "\n")
    (cluster / "us-east-1" / "tablestats.txt").write_text(
        "Total number of tables: 2\n----------------\nKeyspace : shop\n\tRead Count: 3942000\n\tWrite Count: 5256000\n"
        "\t\tTable: orders\n\t\tSpace used (live): 10737418240\n\t\tSSTable Compression Ratio: 0.5\n"
        "\t\tLocal read count: 2628000\n\t\tLocal write count: 2628000\n"
        f"\t\tTable: orders_by_customer\n\t\tSpace used (live): {view_space_bytes}\n\t\tSSTable Compression Ratio: 0.5\n"
        "\t\tLocal read count: 1314000\n\t\tLocal write count: 1314000\n")
    (cluster / "us-east-1" / "info.txt").write_text(
        "ID                     : 00000001-0000-0000-0000-000000000000\nUptime (seconds)       : 2628000\n"
        "Data Center            : us-east-1\n")
    return str(cluster)


def test_materialized_views_priced_from_own_counters_by_default(tools, tmp_path):
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=_mv_cluster(tmp_path, 5368709120),
                                                         explain=True)
    assert "error" not in res, res
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    # base: 1 write/s, 1.5 reads/s, 20 GB; view's own counters: 0.5 writes/s, 0.75 reads/s, 10 GB.
    assert (ks["reads_per_second"], ks["writes_per_second"], ks["uncompressed_single_replica_gb"]) == (2.25, 1.5, 30)
    mv = res["result"]["materialized_views"]["shop.orders_by_customer"]
    assert mv == {"base": "shop.orders", "priced": True, "writes_source": "view tablestats", "storage_source": "view tablestats"}
    assert any("straight writes" in l for l in res["result"]["formulas"]["us-east-1"]["shop"])


def test_materialized_view_unlogged_batch_uses_base_writes(tools, tmp_path):
    cluster = _mv_cluster(tmp_path, 5368709120)
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=cluster, view_writes="unlogged-batch")
    assert "error" not in res, res
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    # writes = base 1 + view at the base's rate 1; reads and storage unchanged.
    assert (ks["reads_per_second"], ks["writes_per_second"], ks["uncompressed_single_replica_gb"]) == (2.25, 2, 30)
    assert res["result"]["materialized_views"]["shop.orders_by_customer"]["writes_source"] == "base table (unlogged batch)"
    cfn = tools.generate_keyspaces_cloudformation(schema_path=str(Path(cluster) / "schema.cql"),
                                                  estimate_path=res["output_path"], output_name="mv.yaml")
    assert Path(cfn["output_path"]).read_text().count("AWS::Cassandra::Table") == 2


def test_materialized_views_can_be_left_out(tools, tmp_path):
    cluster = _mv_cluster(tmp_path, 5368709120)
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=cluster, views=["shop.other_view"])
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    assert (ks["reads_per_second"], ks["writes_per_second"], ks["uncompressed_single_replica_gb"]) == (1.5, 1, 20)
    assert res["result"]["materialized_views"]["shop.orders_by_customer"]["priced"] is False
    cfn = tools.generate_keyspaces_cloudformation(schema_path=str(Path(cluster) / "schema.cql"),
                                                  estimate_path=res["output_path"], output_name="mv-out.yaml")
    assert Path(cfn["output_path"]).read_text().count("AWS::Cassandra::Table") == 1


def test_materialized_view_storage_fallback_scaled_by_schema_row_ratio(tools, tmp_path):
    # No view size and no row-size samples: base storage × schema ratio. View (uuid, uuid, decimal) = 40 B vs
    # base (uuid, uuid, decimal) = 40 B in the fixture -> ratio 1 -> 20 GB + 20 GB.
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=_mv_cluster(tmp_path, 0))
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    assert ks["uncompressed_single_replica_gb"] == 40
    mv = res["result"]["materialized_views"]["shop.orders_by_customer"]
    assert mv["storage_source"] == "base table (fallback)"
    assert (mv["row_size_ratio"], mv["row_size_ratio_source"]) == (1, "schema column types")


def test_materialized_view_storage_fallback_scaled_by_sampled_row_ratio(tools, tmp_path):
    cluster = Path(_mv_cluster(tmp_path, 0))
    # View rows 20% smaller than base rows -> the view gets 80% of the base table's storage.
    (cluster / "us-east-1" / "rowsize.txt").write_text(
        "shop.orders = { lines: 100, columns: 3, average: 1000 bytes, stdev: 0 bytes, min: 900 bytes, max: 1100 bytes, "
        "blob: n, default-ttl: n, static: n}\n"
        "shop.orders_by_customer = { lines: 100, columns: 3, average: 800 bytes, stdev: 0 bytes, min: 700 bytes, "
        "max: 900 bytes, blob: n, default-ttl: n, static: n}\n")
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=str(cluster))
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    assert ks["uncompressed_single_replica_gb"] == 20 + 20 * 0.8
    mv = res["result"]["materialized_views"]["shop.orders_by_customer"]
    assert (mv["row_size_ratio"], mv["row_size_ratio_source"]) == (0.8, "row-size samples")


def test_workload_spec_rejects_unknown_region_override(tools):
    spec = {"regions": ["us-east-1"], "keyspaces": [{"name": "k", "tables": [
        {"name": "t", "reads_per_second": 1, "reads_per_second_by_region": {"eu-west-1": 1}, "rows": 1}]}]}
    res = tools.estimate_from_workload_spec(spec=spec, output_name="bad-region.json")
    assert "error" in res and "eu-west-1" in res["error"]


def test_workload_spec_row_size_from_column_types(tools):
    spec = {"regions": ["us-east-1"], "keyspaces": [{"name": "shop", "tables": [
        {"name": "orders", "columns": ["UUID", "BIGINT", "VARCHAR(100)", "TEXT", "DECIMAL(10,2)", "BOOLEAN"],
         "rows": 1_000_000, "reads_per_second": 10, "writes_per_second": 5}]}]}
    res = tools.estimate_from_workload_spec(spec=spec, explain=True, output_name="columns.json")
    assert "error" not in res, res
    ks = _estimates_by_dc_ks(res["output_path"])["us-east-1"]["shop"]
    assert ks["avg_write_row_size_bytes"] == 16 + 8 + 100 + 64 + 8 + 1
    assert ks["uncompressed_single_replica_gb"] == 1_000_000 * 197 / 1024 ** 3
    assert ("shop.orders: row size = UUID 16 + BIGINT 8 + VARCHAR(100) 100 + TEXT 64 + DECIMAL(10,2) 8 + "
            "BOOLEAN 1 = 197 B") in res["result"]["formulas"]["us-east-1"]["shop"]


def test_skill_routes_to_scripts_first():
    skill = (AGENT_DIR / "skills" / "amazon-keyspaces-skill" / "SKILL.md").read_text()
    assert "### Scripts first; rules for reference" in skill
    assert skill.index("### Scripts first") < skill.index("### Mode 1")
    # Mode 4 hands the arithmetic to the script.
    mode4 = skill[skill.index("### Mode 4"):]
    assert "estimate-workload.ts" in mode4 and "storage_gb` = `(" not in mode4


def test_workload_spec_requires_storage(tools):
    res = tools.estimate_from_workload_spec(spec={"keyspaces": [{"name": "k", "tables": [{"name": "t", "reads_per_second": 1}]}]})
    assert "error" in res and "rows" in res["error"]


def test_explain_is_off_by_default(tools):
    res = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=1, writes_per_second=1, storage_gb=1)
    assert "formulas" not in res["result"]


def test_multi_dc_estimate_enables_pitr_by_default(tools):
    res = tools.estimate_keyspaces_cost(
        region="us-east-1", reads_per_second=100, writes_per_second=100, storage_gb=50, dc_count=3
    )
    assert res["result"]["dc_count"] == 3
    assert res["result"]["on_demand"]["backup"] > 0


def test_diagnostics_estimate_includes_compatibility(tools):
    res = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=str(FIXTURES / "m2"))
    assert "error" not in res, res
    assert res["result"]["source"]
    assert res["result"]["compatibility"]["has_issues"] is True


def test_diagnostics_requires_tablestats(tools):
    assert "error" in tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1")


def test_compatibility_schema_and_prepared(tools):
    res = tools.check_keyspaces_compatibility(
        schema_path=str(FIXTURES / "schema-with-udf.cql"), prepared=str(FIXTURES / "prepared_statements.ndjson")
    )
    assert "error" not in res, res
    assert res["result"]["has_issues"] is True
    assert res["result"]["summary"]["total_issues"] > 0


def test_prepared_statements_with_cqlsh_doubled_backslashes(tools):
    """Captured from a real cqlsh `SELECT JSON` run: quoted identifiers arrive as \\\\"C0\\\\" (invalid JSON)."""
    path = FIXTURES / "prepared-escaped" / "prepared_statements.ndjson"
    raw = path.read_text().splitlines()[0]
    with pytest.raises(json.JSONDecodeError):
        json.loads(raw)
    res = tools.check_keyspaces_compatibility(prepared=str(path))
    assert "error" not in res, res
    qp = res["result"]["details"]["query_patterns"]
    assert qp is not None and qp["aggregations"] == [] and qp["lwt_in_unlogged_batch"] == []


def test_compatibility_inline_cql_clean(tools):
    cql = (
        "CREATE KEYSPACE app WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};\n"
        "CREATE TABLE app.users (id uuid PRIMARY KEY, name text);\n"
    )
    res = tools.check_keyspaces_compatibility(schema_cql=cql)
    assert "error" not in res, res
    assert res["result"]["has_issues"] is False


def test_save_input_file_stays_in_work_dir(tools, tmp_path):
    res = tools.save_input_file(file_name="../../etc/cluster-a/schema.cql", content="x")
    assert Path(res["path"]).resolve().is_relative_to(tmp_path.resolve())
    listing = tools.list_work_files()
    assert any(f["path"] == res["path"] for f in listing["files"])


def test_cloudformation(tools):
    res = tools.generate_keyspaces_cloudformation(schema_path=str(FIXTURES / "m2" / "schema.cql"))
    assert "error" not in res, res
    assert res["resource_count"] > 0
    assert "AWS::Cassandra::Table" in Path(res["output_path"]).read_text()


SQL_OPTION_CQL = """
CREATE KEYSPACE shop WITH replication = {'class': 'SingleRegionStrategy'};
CREATE TABLE shop.orders (customer_id uuid, order_id timeuuid, total decimal,
  PRIMARY KEY (customer_id, order_id)) WITH CLUSTERING ORDER BY (order_id DESC);
CREATE TABLE shop.orders_by_id (order_id timeuuid PRIMARY KEY, customer_id uuid);
"""


def _cfn_lint(path: str) -> None:
    lint = shutil.which("cfn-lint")
    if lint:
        import subprocess

        proc = subprocess.run([lint, path], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stdout + proc.stderr


def test_cloudformation_from_sql_model_estimate(tools):
    cql = tools.save_input_file(file_name="sql-model/option-b.cql", content=SQL_OPTION_CQL)["path"]
    est = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=1000, writes_per_second=500,
                                        storage_gb=100, output_name="option-b.json")
    assert est["result"]["provisioned"]["total"] < est["result"]["on_demand"]["total"]
    # Default stays on-demand even though provisioned is cheaper; provisioned only when asked.
    default = tools.generate_keyspaces_cloudformation(schema_path=cql, estimate_path=est["output_path"],
                                                      output_name="option-b-default.yaml")
    assert default["billing_mode"] == "on_demand"
    assert "PROVISIONED" not in Path(default["output_path"]).read_text()
    res = tools.generate_keyspaces_cloudformation(schema_path=cql, estimate_path=est["output_path"],
                                                  billing_mode="provisioned", output_name="option-b.yaml")
    assert "error" not in res, res
    assert res["billing_mode"] == "provisioned"
    text = Path(res["output_path"]).read_text()
    # 1000 reads/s and 500 writes/s at 70% headroom, split across 2 tables.
    assert text.count("ReadCapacityUnits: 715") == 2 and text.count("WriteCapacityUnits: 358") == 2
    assert "AutoScalingSpecifications" in text
    assert any("split evenly" in w for w in res["warnings"])
    _cfn_lint(res["output_path"])


def test_cloudformation_sizes_reads_from_rows_per_read(tools):
    cql = tools.save_input_file(file_name="rr/model.cql", content=SQL_OPTION_CQL)["path"]
    est = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=1000, writes_per_second=500,
                                        storage_gb=100, rows_per_read=10, output_name="rr-est.json")
    res = tools.generate_keyspaces_cloudformation(schema_path=cql, estimate_path=est["output_path"],
                                                  output_name="rr.yaml", billing_mode="provisioned")
    # 1000 reads/s x 3 units (10 x 1 KB) / 0.70 = 4286 RCU, split across 2 tables.
    assert "ReadCapacityUnits: 2143" in Path(res["output_path"]).read_text()


def test_cloudformation_multi_region_from_diagnostics(tools):
    est = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=str(MULTI_DC),
                                                         dc_regions={"dc-eu": "eu-west-1"})
    res = tools.generate_keyspaces_cloudformation(schema_path=str(MULTI_DC / "schema.cql"),
                                                  estimate_path=est["output_path"])
    assert "error" not in res, res
    text = Path(res["output_path"]).read_text()
    assert "MULTI_REGION" in text and "- eu-west-1" in text and "dc-eu" not in text.split("RegionList")[1][:60]
    _cfn_lint(res["output_path"])


def test_cloudformation_unmapped_datacenter_stays_single_region(tools):
    res = tools.generate_keyspaces_cloudformation(schema_path=str(MULTI_DC / "schema.cql"))
    assert "MULTI_REGION" not in Path(res["output_path"]).read_text()
    assert any("dc-eu" in w for w in res["warnings"])


def test_pdf_comparison(tools):
    a = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=10, writes_per_second=10,
                                      storage_gb=1, output_name="a.json")
    b = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=20, writes_per_second=20,
                                      storage_gb=2, output_name="b.json")
    res = tools.generate_cost_report_pdf(estimate_paths=[a["output_path"], b["output_path"]],
                                         labels=["Option A", "Option B"], output_name="cmp.pdf")
    assert "error" not in res, res
    assert Path(res["output_path"]).read_bytes()[:4] == b"%PDF"


def test_full_evaluation_from_diagnostics(tools):
    res = tools.evaluate_keyspaces_migration(region="us-east-1", directory=str(FIXTURES / "m2"))
    assert res["errors"] == [], res["errors"]
    assert res["cost"]["recommended_capacity_mode"] in ("on_demand", "provisioned")
    assert res["compatibility"]["has_issues"] is True
    assert res["functional_readiness"]["ready_without_changes"] is False
    assert res["functional_readiness"]["cloudformation_resources"] > 0
    assert res["functional_readiness"]["cloudformation_billing_mode"] == "on_demand"  # default, whatever is cheaper
    # Compatibility comes from the diagnostics estimate (schema + prepared inside the directory).
    assert set(res["artifacts"]) == {"estimate_json", "pdf_report", "cloudformation_template"}
    json.dumps(res)  # tool results must be JSON-serializable


def test_full_evaluation_manual_inputs(tools):
    res = tools.evaluate_keyspaces_migration(region="us-east-1", reads_per_second=100, writes_per_second=50,
                                             storage_gb=10, generate_pdf=False)
    assert res["errors"] == []
    assert res["compatibility"] is None
    assert res["functional_readiness"]["compatibility_checked"] is False


MULTI_DC = FIXTURES / "multi-dc"


def _zip_dir(src: Path, dest: Path, prefix: str = "") -> Path:
    with zipfile.ZipFile(dest, "w") as zf:
        for p in sorted(src.rglob("*")):
            if p.is_file():
                zf.write(p, f"{prefix}{p.relative_to(src)}")
    return dest


def test_topology_from_status(tools):
    res = tools.read_cluster_topology(status_path=str(MULTI_DC / "status.txt"))
    assert res["datacenter_count"] == 2
    by_name = {d["name"]: d for d in res["datacenters"]}
    assert by_name["us-east-1"] == {"name": "us-east-1", "node_count": 3, "region_from_name": "us-east-1"}
    assert by_name["dc-eu"]["region_from_name"] is None


def test_topology_rejects_non_status_file(tools):
    assert "error" in tools.read_cluster_topology(status_path=str(MULTI_DC / "schema.cql"))


def test_collection_progress_one_dc_at_a_time(tools, tmp_path):
    cluster = tmp_path / "acme"
    cluster.mkdir()
    shutil.copy(MULTI_DC / "status.txt", cluster / "status.txt")
    res = tools.check_cluster_collection(cluster_dir="acme")
    assert res["pending_datacenters"] == ["us-east-1", "dc-eu"] and not res["ready_to_estimate"]

    shutil.copytree(MULTI_DC / "us-east-1", cluster / "us-east-1")
    res = tools.check_cluster_collection(cluster_dir="acme")
    assert res["collected_datacenters"] == ["us-east-1"]
    assert res["pending_datacenters"] == ["dc-eu"]
    assert res["ready_to_estimate"] and not res["all_datacenters_collected"]
    assert res["estimate_coverage"] == "shared_sample"

    # Second DC arrives as a zip of loose files.
    dc_zip = _zip_dir(MULTI_DC / "dc-eu", tmp_path / "dc-eu.zip")
    res = tools.extract_diagnostics_zip(zip_path=str(dc_zip), cluster_name="acme", datacenter="dc-eu")
    assert res["collection"]["all_datacenters_collected"] is True
    assert res["collection"]["estimate_coverage"] == "per_datacenter"


def test_extract_whole_cluster_zip_with_wrapper_folder(tools, tmp_path):
    cluster_zip = _zip_dir(MULTI_DC, tmp_path / "cluster.zip", prefix="acme-prod/")
    res = tools.extract_diagnostics_zip(zip_path=str(cluster_zip), cluster_name="prod")
    assert res["collection"]["all_datacenters_collected"] is True
    assert res["collection"]["schema_file"].endswith("schema.cql")


def test_extract_zip_blocks_path_traversal(tools, tmp_path):
    bad = tmp_path / "bad.zip"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("../../escape.txt", "x")
        zf.writestr("dc1/info.txt", "x")
    res = tools.extract_diagnostics_zip(zip_path=str(bad), cluster_name="c")
    for f in res["extracted_files"]:
        assert Path(f).resolve().is_relative_to((tmp_path / "c").resolve())
    assert not (tmp_path.parent / "escape.txt").exists()


def test_per_dc_estimate_with_regions(tools):
    res = tools.estimate_cost_from_cassandra_diagnostics(
        region="us-east-1", directory=str(MULTI_DC), dc_regions={"dc-eu": "eu-west-1"}
    )
    assert "error" not in res, res
    regions = res["result"]["regions_by_datacenter"]
    assert regions["us-east-1"]["short"] == "us-east-1"
    assert regions["dc-eu"]["short"] == "eu-west-1"
    assert res["result"]["bundle_mode"] == "perDc"
    # Prepared statements from the us-east-1 folder feed compatibility.
    assert res["result"]["compatibility"]["summary"]["query_patterns"]["aggregations"] > 0


def _estimates_by_dc_ks(estimate_path: str) -> dict:
    return json.loads(Path(estimate_path).read_text())["report_data"]["estimateResults"]


def _estimates_by_dc(estimate_path: str) -> dict:
    data = json.loads(Path(estimate_path).read_text())
    return {dc: ks["bench"] for dc, ks in data["report_data"]["estimateResults"].items()}


def test_single_sample_applies_to_every_replicating_datacenter(tools, tmp_path):
    cluster = tmp_path / "one-dc"
    cluster.mkdir()
    for name in ("status.txt", "schema.cql"):
        shutil.copy(MULTI_DC / name, cluster / name)
    shutil.copytree(MULTI_DC / "us-east-1", cluster / "us-east-1")

    assert tools.check_cluster_collection(cluster_dir="one-dc")["estimate_coverage"] == "shared_sample"
    res = tools.evaluate_keyspaces_migration(region="us-east-1", directory="one-dc",
                                             dc_regions={"dc-eu": "eu-west-1"}, generate_pdf=False)
    assert res["errors"] == [], res["errors"]
    assert res["functional_readiness"]["datacenters_using_fallback_sample"] == {}
    assert res["cost"]["regions_by_datacenter"]["dc-eu"]["short"] == "eu-west-1"
    by_dc = _estimates_by_dc(res["artifacts"]["estimate_json"])
    # The one sample (reads included) is applied to dc-eu, which bench replicates to.
    for field in ("reads_per_second", "writes_per_second", "uncompressed_single_replica_gb"):
        assert by_dc["dc-eu"][field] == by_dc["us-east-1"][field] > 0
    assert by_dc["dc-eu"]["use_backup"] is True  # multi-DC defaults PITR on


def test_mixed_samples_fall_back_for_unsampled_datacenter(tools, tmp_path):
    cluster = tmp_path / "partial"
    shutil.copytree(MULTI_DC, cluster)
    status = (cluster / "status.txt").read_text() + (
        "Datacenter: dc-ap\n=================\nStatus=Up/Down\n|/ State=Normal/Leaving/Joining/Moving\n"
        "--  Address      Load        Tokens  Owns  Host ID                               Rack\n"
        "UN  10.2.0.1     1.5 MiB     256     ?     33333333-3333-3333-3333-333333333331  rack1\n"
    )
    (cluster / "status.txt").write_text(status)
    schema = (cluster / "schema.cql").read_text().replace("'dc-eu': '3'", "'dc-eu': '3', 'dc-ap': '3'")
    (cluster / "schema.cql").write_text(schema)

    collection = tools.check_cluster_collection(cluster_dir="partial")
    assert collection["estimate_coverage"] == "mixed"
    assert collection["pending_datacenters"] == ["dc-ap"]
    res = tools.evaluate_keyspaces_migration(region="us-east-1", directory="partial",
                                             dc_regions={"dc-eu": "eu-west-1", "dc-ap": "ap-south-1"},
                                             generate_pdf=False)
    assert res["errors"] == [], res["errors"]
    # dc-ap has no sample: it borrows us-east-1's (first sampled DC in status order), priced in ap-south-1.
    assert res["functional_readiness"]["datacenters_using_fallback_sample"] == {"dc-ap": {"bench": "us-east-1"}}
    assert res["cost"]["regions_by_datacenter"]["dc-ap"]["short"] == "ap-south-1"
    by_dc = _estimates_by_dc(res["artifacts"]["estimate_json"])
    assert by_dc["dc-ap"]["reads_per_second"] == by_dc["us-east-1"]["reads_per_second"]
    assert by_dc["dc-eu"]["reads_per_second"] < by_dc["us-east-1"]["reads_per_second"]  # dc-eu keeps its own


def test_per_dc_samples_keep_their_own_reads(tools):
    est = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory=str(MULTI_DC),
                                                         dc_regions={"dc-eu": "eu-west-1"})
    by_dc = _estimates_by_dc(est["output_path"])
    assert by_dc["dc-eu"]["reads_per_second"] < by_dc["us-east-1"]["reads_per_second"]
    assert by_dc["dc-eu"]["writes_per_second"] == by_dc["us-east-1"]["writes_per_second"]


def test_sampler_download(tools):
    res = tools.prepare_sampler_download()
    assert "error" not in res, res
    with zipfile.ZipFile(res["output_path"]) as zf:
        names = set(zf.namelist())
    assert names == {
        "keyspaces-samplers/row-size-sampler.sh",
        "keyspaces-samplers/prepared-statements-sampler.sh",
        "keyspaces-samplers/README.txt",
    }


def test_rules_file_separates_pricing_from_conversion(tools):
    res = tools.read_skill_reference()
    assert "error" not in res, res
    text = res["content"]
    pricing = text[text.index("## Part 2"):text.index("## Part 3")]
    conversion = text[text.index("## Part 3"):text.index("## Part 5")]
    # Pricing stays source-independent; conversion never sets prices.
    for term in ("tablestats", "nodetool", "÷ RF", "replication factor", "RF ", "RF)"):
        assert term not in pricing, term
    for term in ("price per", "Savings"):
        assert term not in conversion, term
    assert "error" in tools.read_skill_reference(name="../../../README.md")


def test_rules_file_worked_example_matches_code(tools, tmp_path):
    """Part 4 of keyspaces-pricing-rules.md: N=6, RF=3, 1 write/s, 3 reads/s, 30 GB uncompressed per node."""
    cluster = tmp_path / "example"
    (cluster / "us-east-1").mkdir(parents=True)
    nodes = "\n".join(
        f"UN  10.0.0.{i}  1 GiB  256  ?  0000000{i}-0000-0000-0000-000000000000  rack1" for i in range(1, 7)
    )
    (cluster / "status.txt").write_text(
        "Datacenter: us-east-1\n=====================\nStatus=Up/Down\n"
        "|/ State=Normal/Leaving/Joining/Moving\n--  Address  Load  Tokens  Owns  Host ID  Rack\n" + nodes + "\n"
    )
    (cluster / "schema.cql").write_text(
        "CREATE KEYSPACE shop WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east-1': '3'};\n"
        "CREATE TABLE shop.orders (id uuid PRIMARY KEY, total decimal);\n"
    )
    (cluster / "us-east-1" / "tablestats.txt").write_text(
        "Total number of tables: 1\n----------------\nKeyspace : shop\n\tRead Count: 7884000\n"
        "\tWrite Count: 2628000\n\t\tTable: orders\n\t\tSpace used (live): 16106127360\n"
        "\t\tSpace used (total): 16106127360\n\t\tSSTable Compression Ratio: 0.5\n"
        "\t\tLocal read count: 7884000\n\t\tLocal write count: 2628000\n"
    )
    (cluster / "us-east-1" / "info.txt").write_text(
        "ID                     : 00000001-0000-0000-0000-000000000000\n"
        "Uptime (seconds)       : 2628000\nData Center            : us-east-1\n"
    )
    est = tools.estimate_cost_from_cassandra_diagnostics(region="us-east-1", directory="example", pitr=False)
    assert "error" not in est, est
    ws = json.loads(Path(est["output_path"]).read_text())["report_data"]["estimateResults"]["us-east-1"]["shop"]
    assert (ws["writes_per_second"], ws["reads_per_second"], ws["uncompressed_single_replica_gb"]) == (2, 9, 60)
    assert round(est["result"]["on_demand"]["total"], 2) == 21.24
    assert round(est["result"]["provisioned"]["total"], 2) == 17.58
    # Part 5, source B: the same workload as PostgreSQL tables in a workload spec.
    spec = {"source": "postgres", "regions": ["us-east-1"], "pitr": False, "keyspaces": [{"name": "shop", "tables": [
        {"name": "orders", "reads_per_second": 6, "writes_per_second": 1.5, "avg_row_bytes": 1024, "rows": 47185920},
        {"name": "customers", "reads_per_second": 3, "writes_per_second": 0.5, "avg_row_bytes": 1024, "rows": 15728640},
    ]}]}
    from_spec = tools.estimate_from_workload_spec(spec=spec, output_name="part5-spec.json")
    assert "error" not in from_spec, from_spec
    assert from_spec["result"]["on_demand"]["total"] == est["result"]["on_demand"]["total"]
    assert from_spec["result"]["provisioned"]["total"] == est["result"]["provisioned"]["total"]
    # Same workload typed in directly prices identically (Part 2 is source-independent).
    manual = tools.estimate_keyspaces_cost(region="us-east-1", reads_per_second=9, writes_per_second=2,
                                           storage_gb=60, output_name="manual.json")
    assert manual["result"]["on_demand"]["total"] == est["result"]["on_demand"]["total"]
    assert manual["result"]["provisioned"]["total"] == est["result"]["provisioned"]["total"]


def test_skill_loads_as_agent_skill():
    from strands.vended_plugins.skills import Skill

    skills = Skill.from_directory(AGENT_DIR / "skills")
    assert [s.name for s in skills] == ["amazon-keyspaces-skill"]
    assert "Mode 3" in skills[0].instructions


def test_agent_factory_builds_orchestrator():
    import app
    from agent_builder_sdk.orchestrator_strands.base_orchestrator import AsyncBaseOrchestrator

    agent = app.agent_factory(None, "/tmp/keyspaces-test-orchestrator")
    assert isinstance(agent, AsyncBaseOrchestrator)
