"""Strands tools that expose the Amazon Keyspaces skill to the AWS Transform agent.

Every tool shells out to the same TypeScript CLIs the Claude Code / Kiro skill
uses (scripts/*.ts), pre-bundled by build.sh into vendor/calculator/*.js so the
container only needs a Node.js runtime. Pricing, parsing, and compatibility
logic therefore stays single-sourced in src/calculator/.

All inputs and outputs live under KEYSPACES_WORK_DIR. Customer files arrive as
AWS Transform artifacts: download them into the work dir with the agentic MCP
artifact tools (or save pasted text with save_input_file), then pass the local
paths to these tools. Generated PDFs / templates are written to the work dir and
can be uploaded back as CUSTOMER_OUTPUT artifacts.
"""

import base64
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Optional

from strands import tool

logger = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
# Local default: transform-agent/vendor/calculator. The Dockerfile sets KEYSPACES_VENDOR_DIR explicitly.
VENDOR_DIR = Path(os.environ.get("KEYSPACES_VENDOR_DIR", _HERE.parent / "vendor" / "calculator"))
WORK_DIR = Path(os.environ.get("KEYSPACES_WORK_DIR", "/tmp/keyspaces_work"))
NODE_BIN = os.environ.get("NODE_BIN", "node")
SCRIPT_TIMEOUT_SECONDS = int(os.environ.get("KEYSPACES_SCRIPT_TIMEOUT", "300"))
# Customer-side samplers (tools/*.sh), staged by build.sh.
SAMPLERS_DIR = Path(os.environ.get("KEYSPACES_SAMPLERS_DIR", VENDOR_DIR.parent / "samplers"))
# Skill supporting files (pricing-rules.md, ...), staged by build.sh next to SKILL.md.
SKILL_DIR = Path(
    os.environ.get("KEYSPACES_SKILLS_DIR", VENDOR_DIR.parent.parent / "skills")
) / "amazon-keyspaces-skill"
# Uncompressed size cap for uploaded diagnostic zips.
MAX_ZIP_BYTES = int(os.environ.get("KEYSPACES_MAX_ZIP_BYTES", str(512 * 1024 * 1024)))

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class ScriptError(RuntimeError):
    """A calculator CLI exited non-zero."""


def _work_dir() -> Path:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    return WORK_DIR


def _safe_name(name: str, default: str) -> str:
    cleaned = _SAFE_NAME.sub("_", os.path.basename(name or "")).strip("._")
    return cleaned or default


def _resolve(path: str) -> str:
    """Resolve a path relative to the work dir and require that it exists."""
    p = Path(path)
    if not p.is_absolute():
        p = _work_dir() / p
    if not p.exists():
        raise FileNotFoundError(f"Input not found: {p}")
    return str(p)


def _run(script: str, args: list[str], stdin: Optional[str] = None) -> str:
    return _run_with_stderr(script, args, stdin)[0]


def _run_with_stderr(script: str, args: list[str], stdin: Optional[str] = None) -> tuple[str, str]:
    js = VENDOR_DIR / f"{script}.js"
    if not js.exists():
        raise ScriptError(f"Calculator bundle missing: {js}. Run build.sh before starting the agent.")
    cmd = [NODE_BIN, str(js), *args]
    logger.info("Running %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        input=stdin,
        capture_output=True,
        text=True,
        timeout=SCRIPT_TIMEOUT_SECONDS,
        cwd=str(_work_dir()),
    )
    if proc.returncode != 0:
        raise ScriptError(f"{script} failed (exit {proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout, proc.stderr


def _run_json(script: str, args: list[str], output_name: str, stdin: Optional[str] = None) -> dict[str, Any]:
    data = json.loads(_run(script, args, stdin))
    out = _work_dir() / _safe_name(output_name, f"{script}.json")
    out.write_text(json.dumps(data, indent=2))
    # report_data only feeds the PDF renderer; keep it on disk, not in the LLM context.
    summary = {k: v for k, v in data.items() if k != "report_data"}
    return {"output_path": str(out), "result": summary}


def _error(e: Exception) -> dict[str, Any]:
    logger.exception("Keyspaces tool failed")
    return {"error": f"{type(e).__name__}: {e}"}


def _pitr_flag(pitr: Optional[bool]) -> list[str]:
    if pitr is None:
        return []
    return ["--pitr"] if pitr else ["--no-pitr"]


@tool
def save_input_file(file_name: str, content: str) -> dict[str, Any]:
    """Save text the user pasted in chat (CQL schema, nodetool output, prepared statements, SQL DDL)
    to the Keyspaces work directory so the other Keyspaces tools can read it.

    Args:
        file_name: File name to write, e.g. "schema.cql", "tablestats.txt", "info-node1.txt".
            Use a sub-path like "cluster-a/tablestats.txt" to group one cluster's files into a directory.
        content: Raw file content.

    Returns:
        {"path": absolute path written, "bytes": size}
    """
    try:
        parts = [_safe_name(p, "file") for p in Path(file_name).parts if p not in ("", ".", "..", "/")]
        target = _work_dir().joinpath(*parts) if parts else _work_dir() / "input.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        return {"path": str(target), "bytes": target.stat().st_size}
    except Exception as e:
        return _error(e)


@tool
def list_work_files(directory: Optional[str] = None) -> dict[str, Any]:
    """List files in the Keyspaces work directory (or a sub-directory of it), e.g. to confirm which
    downloaded artifacts or saved inputs are available before running an estimate.

    Args:
        directory: Optional sub-directory (relative to the work dir) or absolute path.

    Returns:
        {"directory": path, "files": [{"path", "bytes"}]}
    """
    try:
        root = Path(_resolve(directory)) if directory else _work_dir()
        files = [
            {"path": str(p), "bytes": p.stat().st_size}
            for p in sorted(root.rglob("*"))
            if p.is_file()
        ]
        return {"directory": str(root), "files": files}
    except Exception as e:
        return _error(e)


@tool
def read_cluster_topology(status_path: str) -> dict[str, Any]:
    """Read `nodetool status` (always collect it first) to learn the cluster's datacenters and node counts.
    For each DC, `region_from_name` is the AWS Region code when the DC is named after one (e.g. "us-east-1");
    otherwise it is null and you must ask the customer which AWS Region to price that DC in.

    Args:
        status_path: Path to the saved `nodetool status` output, e.g. "<cluster>/status.txt".

    Returns:
        {"datacenter_count", "datacenters": [{"name", "node_count", "region_from_name"}]}
    """
    try:
        data = json.loads(_run("cluster-bundle", ["--status", _resolve(status_path)]))
        if not data["datacenters"]:
            return {"error": "No datacenters found. The file does not look like `nodetool status` output."}
        return {
            "datacenter_count": data["datacenter_count"],
            "datacenters": [
                {k: dc[k] for k in ("name", "node_count", "region_from_name")} for dc in data["datacenters"]
            ],
        }
    except Exception as e:
        return _error(e)


@tool
def check_cluster_collection(cluster_dir: str) -> dict[str, Any]:
    """Report collection progress for a cluster directory: which datacenters from `nodetool status` already
    have a complete single-node bundle (tablestats + info) and which are still pending. Call it after every
    upload to tell the customer which datacenter to collect next.

    Expected layout (file names are free; files are recognized by content):
        <cluster>/status.txt, <cluster>/schema.cql, <cluster>/<datacenter>/{tablestats,info,rowsize,prepared}

    Args:
        cluster_dir: Cluster directory in the work dir, e.g. "acme-prod".

    Returns:
        {"datacenters": [{"name", "node_count", "region_from_name", "complete", "missing", "files"}],
         "collected_datacenters", "pending_datacenters", "unmatched_folders", "schema_file",
         "all_datacenters_collected", "ready_to_estimate"}
    """
    try:
        return json.loads(_run("cluster-bundle", ["--dir", _resolve(cluster_dir)]))
    except Exception as e:
        return _error(e)


@tool
def extract_diagnostics_zip(zip_path: str, cluster_name: str, datacenter: Optional[str] = None) -> dict[str, Any]:
    """Unpack a customer-uploaded diagnostics zip into the cluster directory, then report collection progress.
    The zip may hold the whole cluster (status.txt and schema.cql at the root, one folder per datacenter
    name) or a single datacenter folder; contents are merged into <cluster_name>/ so datacenters can arrive
    in separate uploads. A single wrapping top-level folder is unwrapped.

    Args:
        zip_path: Path to the downloaded zip artifact.
        cluster_name: Cluster directory name in the work dir, e.g. "acme-prod".
        datacenter: Set when the zip holds one datacenter's files without a datacenter folder; the files are
            placed under <cluster_name>/<datacenter>/.

    Returns:
        {"cluster_dir", "extracted_files", "collection": check_cluster_collection result}
    """
    try:
        cluster_root = _work_dir() / _safe_name(cluster_name, "cluster")
        target = cluster_root / _safe_name(datacenter, "datacenter") if datacenter else cluster_root
        target.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(_resolve(zip_path)) as zf:
            members = [
                m for m in zf.infolist()
                if not m.is_dir() and "__MACOSX" not in m.filename and not Path(m.filename).name.startswith(".")
            ]
            if sum(m.file_size for m in members) > MAX_ZIP_BYTES:
                return {"error": f"Zip expands beyond {MAX_ZIP_BYTES} bytes; upload one datacenter at a time."}
            parts_list = [[p for p in Path(m.filename).parts if p not in ("", ".", "..", "/")] for m in members]
            # Unwrap a single top-level folder that is not itself a datacenter bundle (e.g. "acme-prod/...").
            tops = {parts[0] for parts in parts_list if len(parts) > 1}
            if len(tops) == 1 and all(len(parts) > 1 for parts in parts_list):
                top = tops.pop()
                if datacenter or any(len(parts) > 2 for parts in parts_list) or top == cluster_name:
                    parts_list = [parts[1:] for parts in parts_list]
            extracted = []
            for member, parts in zip(members, parts_list):
                if not parts:
                    continue
                dest = target.joinpath(*(_safe_name(p, "file") for p in parts))
                if not dest.resolve().is_relative_to(target.resolve()):
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, open(dest, "wb") as out:
                    shutil.copyfileobj(src, out)
                extracted.append(str(dest))
        return {
            "cluster_dir": str(cluster_root),
            "extracted_files": extracted,
            "collection": check_cluster_collection(cluster_dir=str(cluster_root)),
        }
    except zipfile.BadZipFile:
        return {"error": "The file is not a valid zip archive."}
    except Exception as e:
        return _error(e)


_ARTIFACT_FILE_TYPES = {
    ".pdf": "PDF", ".json": "JSON", ".zip": "ZIP", ".html": "HTML", ".md": "MARKDOWN",
    ".csv": "CSV", ".xlsx": "XLSX", ".pptx": "PPTX",
}


@tool
def prepare_artifact_upload(path: str) -> dict[str, Any]:
    """Compute what create_artifact_upload_url needs for a file you want to deliver as a job artifact: the
    base64-encoded SHA-256 `content_digest` (never guess or hand-compute it) and the artifact `fileType`.
    Call this for every output (PDF report, CloudFormation template, JSON) right before
    create_artifact_upload_url, then upload_artifact with the same path, then complete_artifact_upload.

    Args:
        path: File in the Keyspaces work directory (as returned by the other tools).

    Returns:
        {"path", "content_digest", "file_type", "bytes", "file_name"}
    """
    try:
        target = Path(_resolve(path))
        digest = base64.b64encode(hashlib.sha256(target.read_bytes()).digest()).decode("ascii")
        return {
            "path": str(target),
            "content_digest": digest,
            # CloudFormation YAML and anything else without a dedicated type is uploaded as TXT.
            "file_type": _ARTIFACT_FILE_TYPES.get(target.suffix.lower(), "TXT"),
            "bytes": target.stat().st_size,
            "file_name": target.name,
        }
    except Exception as e:
        return _error(e)


@tool
def prepare_sampler_download() -> dict[str, Any]:
    """Package the two customer-side samplers (row-size-sampler.sh, prepared-statements-sampler.sh) with a
    short README into a zip in the work dir. Upload it as a CUSTOMER_OUTPUT artifact (fileType ZIP) so the
    customer can download it. nodetool and cqlsh captures are instructions only, not scripts.

    Returns:
        {"output_path": zip path, "files": [...]}
    """
    try:
        scripts = sorted(SAMPLERS_DIR.glob("*.sh"))
        if not scripts:
            return {"error": f"No samplers found in {SAMPLERS_DIR}. Run build.sh before starting the agent."}
        readme = (
            "Amazon Keyspaces samplers\n"
            "=========================\n\n"
            "Both scripts take cqlsh connection arguments and only run SELECT queries.\n"
            "Credentials are passed on your command line and are not written to the output.\n\n"
            "Row sizes (once per cluster, any datacenter):\n"
            "  ./row-size-sampler.sh <host> <port> [-u user] [-p pass] [--ssl] > rowsize.txt\n\n"
            "Prepared statements (once per datacenter; point <host> at the node you sampled in that datacenter,\n"
            "because system.prepared_statements is local to each node):\n"
            "  ./prepared-statements-sampler.sh <host> <port> [-u user] [-p pass] [--ssl] > prepared_statements.ndjson\n"
        )
        out = _work_dir() / "keyspaces-samplers.zip"
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            for script in scripts:
                info = zipfile.ZipInfo(f"keyspaces-samplers/{script.name}")
                info.external_attr = 0o755 << 16
                zf.writestr(info, script.read_bytes())
            zf.writestr("keyspaces-samplers/README.txt", readme)
        return {"output_path": str(out), "files": [s.name for s in scripts] + ["README.txt"]}
    except Exception as e:
        return _error(e)


@tool
def read_skill_reference(name: str = "keyspaces-pricing-rules.md") -> dict[str, Any]:
    """Read a reference file shipped with the amazon-keyspaces-skill. Use it to explain how an estimate was
    calculated, never to compute prices by hand. keyspaces-pricing-rules.md has four parts:
      1. the Keyspaces workload fields (per keyspace per Region) and shared constants
      2. Keyspaces pricing for any source (units, formulas, savings plans, non-Cassandra inputs, CloudFormation)
      3. Apache Cassandra diagnostics -> workload (N / RF scaling, row size and TTL, datacenters -> Regions, TCO)
      4. a worked example through conversion, workload and price

    Args:
        name: File name inside the skill directory (default "keyspaces-pricing-rules.md").

    Returns:
        {"name", "content"}
    """
    try:
        target = (SKILL_DIR / os.path.basename(name)).resolve()
        if not target.is_relative_to(SKILL_DIR.resolve()) or not target.is_file():
            available = sorted(p.name for p in SKILL_DIR.glob("*") if p.is_file())
            return {"error": f"No reference named {name!r}. Available: {available}"}
        return {"name": target.name, "content": target.read_text()}
    except Exception as e:
        return _error(e)


@tool
def estimate_keyspaces_cost(
    region: str,
    reads_per_second: float,
    writes_per_second: float,
    storage_gb: float,
    row_size_bytes: int = 1024,
    ttl_deletes_per_second: float = 0,
    dc_count: int = 1,
    reads_override: Optional[str] = None,
    pitr: Optional[bool] = None,
    rows_per_read: float = 1,
    explain: bool = False,
    pitr_by_region: Optional[dict[str, bool]] = None,
    output_name: str = "keyspaces-calc.json",
) -> dict[str, Any]:
    """Estimate Amazon Keyspaces monthly cost from manual workload inputs (skill Mode 1, calculate.ts).
    Returns on-demand vs provisioned totals, per-dimension costs, and savings-plan pricing when available.

    Args:
        region: AWS Region code, e.g. "us-east-1".
        reads_per_second: Read requests per second (per datacenter).
        writes_per_second: Write requests per second (per datacenter).
        storage_gb: Logical data stored, in GB (per datacenter).
        row_size_bytes: Average row size in bytes (default 1024).
        ttl_deletes_per_second: TTL-driven deletes per second (default 0).
        dc_count: Number of identical datacenters / Regions (default 1). IDs become dc-1..dc-N when > 1.
        reads_override: Optional per-DC reads/s, e.g. "dc-1=500,dc-2=100". Names must match dc-1..dc-N.
        pitr: Point-in-time recovery. None = skill default (on when dc_count > 1, off for a single DC).
        rows_per_read: Average rows returned per read (range / partition reads). Read units aggregate the
            bytes of all rows read (per 1 MB page) before dividing by 4 KB. Default 1.
        explain: Add `formulas`: every pricing formula written out with this estimate's numbers
            ({datacenter: {keyspace: [lines]}}). Show these lines verbatim when asked how it was calculated.
        pitr_by_region: PITR per Region, e.g. {"dc-2": False}; wins over `pitr` and the default. Reads per Region:
            use `reads_override`. Writes and storage are the same in every Region.
        output_name: File name for the JSON estimate in the work dir (pass it to generate_cost_report_pdf).

    Returns:
        {"output_path": saved JSON path, "result": estimate}
    """
    try:
        args = [
            region,
            str(reads_per_second),
            str(writes_per_second),
            str(row_size_bytes),
            str(storage_gb),
            str(ttl_deletes_per_second),
        ]
        if dc_count and dc_count > 1:
            args += ["--dc-count", str(dc_count)]
        if reads_override:
            args += ["--reads-override", reads_override]
        if rows_per_read and rows_per_read != 1:
            args += ["--rows-per-read", str(rows_per_read)]
        if explain:
            args.append("--explain")
        args += _pitr_flag(pitr)
        if pitr_by_region:
            args += ["--pitr-override", ",".join(f"{dc}={'on' if on else 'off'}" for dc, on in pitr_by_region.items())]
        return _run_json("calculate", args, output_name)
    except Exception as e:
        return _error(e)


@tool
def estimate_from_workload_spec(
    spec: dict[str, Any],
    explain: bool = False,
    output_name: str = "keyspaces-workload-estimate.json",
) -> dict[str, Any]:
    """Estimate Amazon Keyspaces cost from a source-independent workload spec (estimate-workload.ts): per-table
    metrics from any database (DynamoDB, MongoDB, PostgreSQL, ...), monitoring data, or a proposed data model
    such as one SQL-translation option. Supports several keyspaces and several Regions in one estimate.
    Conversion rules: Part 3 of keyspaces-pricing-rules.md.

    Args:
        spec: {"source": "postgres", "regions": ["us-east-1", {"name": "eu-west-1", "pitr": False}],
               "pitr": optional bool,
               "keyspaces": [{"name": "shop", "pitr": optional bool, "tables": [{
                   "name": "orders", "reads_per_second": 120, "writes_per_second": 40,
                   "reads_per_second_by_region": {"eu-west-1": 30},   # optional: reads differ per Region
                   "avg_row_bytes": 512,        # or "columns": ["UUID", "VARCHAR(100)", "TEXT"] (sizes summed)
                   "rows_per_read": 1, "ttl_deletes_per_second": 0,
                   "rows": 250000000            # storage = rows x avg_row_bytes, or instead:
                   "stored_bytes": ..., "compression_ratio": 0.5, "storage_copies": 3,
                   "request_copies": 1           # divide reads/writes when metrics count replica operations
               }]}]}
            Requests are logical client requests; storage is one uncompressed copy of the data.
        explain: Add `formulas`: the per-table conversion and every pricing formula with the spec's numbers.
        output_name: File name for the JSON estimate in the work dir (works with generate_cost_report_pdf and
            generate_keyspaces_cloudformation like any other estimate).

    Returns:
        {"output_path": saved JSON path, "result": estimate}
    """
    try:
        spec_path = _work_dir() / f"{Path(_safe_name(output_name, 'workload')).stem}-spec.json"
        spec_path.write_text(json.dumps(spec, indent=2))
        args = ["--spec", str(spec_path)]
        if explain:
            args.append("--explain")
        return _run_json("estimate-workload", args, output_name)
    except Exception as e:
        return _error(e)


@tool
def estimate_cost_from_cassandra_diagnostics(
    region: str,
    directory: Optional[str] = None,
    tablestats: Optional[str] = None,
    info: Optional[list[str]] = None,
    status: Optional[str] = None,
    rowsize: Optional[str] = None,
    schema_path: Optional[str] = None,
    prepared: Optional[str] = None,
    tco: Optional[str] = None,
    infer_reads: Optional[str] = None,
    reads_override: Optional[str] = None,
    pitr: Optional[bool] = None,
    dc_regions: Optional[dict[str, str]] = None,
    rows_per_read: float = 1,
    explain: bool = False,
    pitr_by_region: Optional[dict[str, bool]] = None,
    views: Optional[list[str]] = None,
    view_writes: str = "own",
    output_name: str = "keyspaces-calc.json",
) -> dict[str, Any]:
    """Estimate Amazon Keyspaces cost from Cassandra diagnostic captures (skill Mode 2, parse-cassandra.ts).
    Requires nodetool tablestats AND nodetool info. When a schema or prepared-statements file is present the
    result also contains a `compatibility` block. Prefer `directory` so files are auto-classified.

    Per-datacenter collection: pass the cluster directory (status.txt + schema.cql at the root, one
    sub-folder per datacenter with that DC's single-node tablestats/info). Each DC is priced from its own
    captures, scaled by its node count in nodetool status. A DC without its own folder uses, per keyspace,
    the first sampled DC's sample (reported in `fallback_datacenters`); with only one sample, that sample
    applies to every DC each keyspace replicates to.

    Args:
        region: Default AWS Region code, used for any DC not in dc_regions and not named after a Region.
        directory: Cluster directory (auto-detects files at the root and per-DC sub-folders).
        tablestats: Path to `nodetool tablestats` output (explicit paths override directory detection).
        info: Paths to `nodetool info` output, one per node.
        status: Path to `nodetool status` output (authoritative DC / node topology).
        rowsize: Path to row-size-sampler output.
        schema_path: Path to `DESCRIBE SCHEMA` CQL.
        prepared: Path to prepared-statements sampler NDJSON.
        tco: Path to cassandra_tco_helper.py JSON for a self-managed TCO comparison.
        infer_reads: Reads for DCs without their own info file: "zero" (default), "same", or "measured".
        reads_override: Per-DC reads/s override, e.g. "dc1=100,dc2=50" (status DC names).
        pitr: Point-in-time recovery. None = skill default (on for shared multi-DC bundles, else off).
        dc_regions: Region per status DC name, e.g. {"dc-eu": "eu-west-1"}. A DC already named after a
            Region code (e.g. "us-east-1") is priced there automatically.
        rows_per_read: Average rows returned per read, applied to every keyspace. tablestats counts read
            requests, not rows, so ask the customer when queries read ranges or whole partitions. Default 1.
        explain: Add `formulas`: the Cassandra conversion (sampled rate x N / RF) and every pricing formula,
            written out with this estimate's numbers. Show these lines verbatim when asked how it was calculated.
        pitr_by_region: PITR per datacenter (status names), e.g. {"dc-eu": False}; wins over `pitr` and the default.
        views: Materialized views to carry over as Keyspaces tables, e.g. ["shop.orders_by_customer"]. Default: all
            views; unlisted views aren't priced. Reads and storage come from the view's own tablestats.
        view_writes: "own" (default): view writes from the view's own tablestats counters (straight writes).
            "unlogged-batch": view writes = the base table's writes (view row written with the base row in an
            UNLOGGED BATCH). Each view's treatment is reported in `materialized_views`.
        output_name: File name for the JSON estimate in the work dir.

    Returns:
        {"output_path": saved JSON path, "result": estimate (incl. regions_by_datacenter and compatibility)}
    """
    try:
        if not directory and not tablestats:
            return {"error": "Provide `directory` or `tablestats`. Without tablestats and info use estimate_keyspaces_cost (Mode 1)."}
        args = ["--region", region]
        if directory:
            args += ["--dir", _resolve(directory)]
        for flag, value in (
            ("--tablestats", tablestats),
            ("--status", status),
            ("--rowsize", rowsize),
            ("--schema", schema_path),
            ("--prepared", prepared),
            ("--tco", tco),
        ):
            if value:
                args += [flag, _resolve(value)]
        for path in info or []:
            args += ["--info", _resolve(path)]
        if infer_reads:
            args += ["--infer-reads", infer_reads]
        if reads_override:
            args += ["--reads-override", reads_override]
        for dc, dc_region in (dc_regions or {}).items():
            args += ["--dc-region", f"{dc}={dc_region}"]
        if rows_per_read and rows_per_read != 1:
            args += ["--rows-per-read", str(rows_per_read)]
        if explain:
            args.append("--explain")
        args += _pitr_flag(pitr)
        if pitr_by_region:
            args += ["--pitr-override", ",".join(f"{dc}={'on' if on else 'off'}" for dc, on in pitr_by_region.items())]
        if views:
            args += ["--views", ",".join(views)]
        if view_writes and view_writes != "own":
            args += ["--view-writes", view_writes]
        return _run_json("parse-cassandra", args, output_name)
    except Exception as e:
        return _error(e)


@tool
def check_keyspaces_compatibility(
    schema_path: Optional[str] = None,
    prepared: Optional[str] = None,
    rowsize: Optional[str] = None,
    schema_cql: Optional[str] = None,
    output_name: str = "keyspaces-compat.json",
) -> dict[str, Any]:
    """Check a Cassandra schema and/or prepared statements for features Amazon Keyspaces does not support
    (skill Mode 3, check-compatibility.ts): secondary indexes, triggers, materialized views, UDFs, UDAs,
    LWT inside unlogged batches, aggregate calls, plus row-size / UDT quota findings. Compatibility is binary:
    every flagged feature is not supported.

    Args:
        schema_path: Path to CQL DDL (`DESCRIBE SCHEMA`).
        prepared: Path to prepared-statements NDJSON.
        rowsize: Optional row-size-sampler output, enables row-size quota checks.
        schema_cql: Inline CQL text, used when no schema_path is given.
        output_name: File name for the JSON report in the work dir.

    Returns:
        {"output_path": saved JSON path, "result": {"has_issues", "summary", "details"}}
    """
    try:
        if not schema_path and schema_cql:
            path = _work_dir() / "inline-schema.cql"
            path.write_text(schema_cql)
            schema_path = str(path)
        if not schema_path and not prepared:
            return {"error": "Provide `schema_path`, `schema_cql`, or `prepared`."}
        args: list[str] = []
        if schema_path:
            args += ["--schema", _resolve(schema_path)]
        if prepared:
            args += ["--prepared", _resolve(prepared)]
        if rowsize:
            args += ["--rowsize", _resolve(rowsize)]
        return _run_json("check-compatibility", args, output_name)
    except Exception as e:
        return _error(e)


@tool
def generate_keyspaces_cloudformation(
    schema_path: str,
    estimate_path: Optional[str] = None,
    dc_regions: Optional[dict[str, str]] = None,
    billing_mode: Optional[str] = None,
    pitr: bool = False,
    auto_scaling: bool = True,
    output_format: str = "yaml",
    include_system: bool = False,
    output_name: str = "keyspaces-cfn.yaml",
) -> dict[str, Any]:
    """Generate a deployable AWS CloudFormation template for the target Amazon Keyspaces data model
    (AWS::Cassandra::Keyspace / Type / Table) from CQL (generate-cfn.ts). The CQL can be the customer's
    `DESCRIBE SCHEMA` or a data model you produced (e.g. a SQL-translation option saved with save_input_file).

    Pass the matching estimate so the template follows the priced model: PITR where backups were priced,
    datacenter -> AWS Region for multi-Region keyspaces, and (when billing_mode="provisioned") RCU/WCU per
    table with auto scaling at the default target utilization. Tables are ON_DEMAND unless the customer
    asks for provisioned. Unsupported features are left out (warnings say what).

    Args:
        schema_path: Path to the CQL DDL for the model.
        estimate_path: `output_path` of the estimate for this model (estimate_keyspaces_cost or
            estimate_cost_from_cassandra_diagnostics). Strongly recommended.
        dc_regions: Datacenter -> Region for NetworkTopologyStrategy keyspaces, e.g. {"dc-eu": "eu-west-1"}.
            Diagnostics estimates already carry this; DCs named after a Region map automatically.
        billing_mode: "on_demand" (default) or "provisioned". Use "provisioned" only when the customer asks for it;
            capacity is then sized from the estimate.
        pitr: Force point-in-time recovery on every table.
        auto_scaling: Add auto scaling to provisioned tables.
        output_format: "yaml" or "json".
        include_system: Include system keyspaces.
        output_name: File name for the template in the work dir.

    Returns:
        {"output_path", "resource_count", "billing_mode", "warnings": [...], "preview": first lines}
    """
    try:
        out = _work_dir() / _safe_name(output_name, f"keyspaces-cfn.{output_format}")
        args = ["--schema", _resolve(schema_path), "--format", output_format, "--output", str(out)]
        if estimate_path:
            args += ["--estimate", _resolve(estimate_path)]
        for dc, region in (dc_regions or {}).items():
            args += ["--dc-region", f"{dc}={region}"]
        if billing_mode:
            args += ["--billing-mode", billing_mode]
        if pitr:
            args.append("--pitr")
        if not auto_scaling:
            args.append("--no-auto-scaling")
        if include_system:
            args.append("--include-system")
        _, stderr = _run_with_stderr("generate-cfn", args)
        text = out.read_text()
        return {
            "output_path": str(out),
            "resource_count": len(re.findall(r"AWS::Cassandra::(?:Keyspace|Table)", text)),
            "billing_mode": "provisioned" if "Mode: PROVISIONED" in text or '"Mode": "PROVISIONED"' in text else "on_demand",
            "warnings": [line[len("WARNING: "):] for line in stderr.splitlines() if line.startswith("WARNING: ")],
            "preview": "\n".join(text.splitlines()[:60]),
        }
    except Exception as e:
        return _error(e)


@tool
def generate_cost_report_pdf(
    estimate_paths: list[str],
    labels: Optional[list[str]] = None,
    output_name: str = "keyspaces-pricing-estimate.pdf",
) -> dict[str, Any]:
    """Render one or more saved estimate JSON files (from estimate_keyspaces_cost or
    estimate_cost_from_cassandra_diagnostics) into a single PDF report (generate-pdf.ts).
    Pass several estimates with labels to produce a side-by-side comparison (e.g. Options A/B/C).

    Args:
        estimate_paths: Paths to estimate JSON files (the `output_path` values returned by the estimate tools).
        labels: Optional display label per estimate, same order as estimate_paths.
        output_name: PDF file name in the work dir.

    Returns:
        {"output_path": PDF path, "bytes": size}
    """
    try:
        if not estimate_paths:
            return {"error": "estimate_paths must not be empty."}
        out = _work_dir() / _safe_name(output_name, "keyspaces-pricing-estimate.pdf")
        args: list[str] = []
        for i, path in enumerate(estimate_paths):
            args += ["--input", _resolve(path)]
            if labels and i < len(labels) and labels[i]:
                args += ["--label", labels[i]]
        args += ["--output", str(out)]
        _run("generate-pdf", args)
        return {"output_path": str(out), "bytes": out.stat().st_size}
    except Exception as e:
        return _error(e)


def _cost_summary(estimate: dict[str, Any]) -> dict[str, Any]:
    on_demand = (estimate.get("on_demand") or {}).get("total")
    provisioned = (estimate.get("provisioned") or {}).get("total")
    summary: dict[str, Any] = {
        "region": estimate.get("region"),
        "monthly_on_demand_usd": on_demand,
        "monthly_provisioned_usd": provisioned,
    }
    if estimate.get("savings_plan_available"):
        summary["monthly_on_demand_savings_plan_usd"] = (estimate.get("on_demand_savings_plan") or {}).get("total")
        summary["monthly_provisioned_savings_plan_usd"] = (estimate.get("provisioned_savings_plan") or {}).get("total")
    if on_demand is not None and provisioned is not None:
        summary["recommended_capacity_mode"] = "provisioned" if provisioned < on_demand else "on_demand"
    if estimate.get("regions_by_datacenter"):
        summary["regions_by_datacenter"] = estimate["regions_by_datacenter"]
    if estimate.get("fallback_datacenters"):
        summary["fallback_datacenters"] = estimate["fallback_datacenters"]
    return summary


@tool
def evaluate_keyspaces_migration(
    region: str,
    directory: Optional[str] = None,
    schema_path: Optional[str] = None,
    prepared: Optional[str] = None,
    rowsize: Optional[str] = None,
    reads_per_second: Optional[float] = None,
    writes_per_second: Optional[float] = None,
    storage_gb: Optional[float] = None,
    row_size_bytes: int = 1024,
    generate_pdf: bool = True,
    generate_cloudformation: bool = True,
    dc_regions: Optional[dict[str, str]] = None,
    report_prefix: str = "keyspaces-evaluation",
) -> dict[str, Any]:
    """One-shot Apache Cassandra -> Amazon Keyspaces migration evaluation covering cost, compatibility,
    and functional readiness. Runs the diagnostics-based estimate when `directory` has tablestats + info,
    otherwise the manual estimate from reads/writes/storage; checks compatibility when a schema or prepared
    statements are available; optionally renders the PDF report and CloudFormation template.

    Args:
        region: Default AWS Region code (DCs not in dc_regions and not named after a Region).
        directory: Cluster directory (status.txt + schema.cql at the root, one folder per datacenter).
            Compatibility then comes from the schema plus every datacenter's prepared statements.
        schema_path: CQL DDL path; only needed when it is not inside `directory`.
        prepared: Prepared-statements NDJSON path; only needed when it is not inside `directory`.
        rowsize: Row-size-sampler output path; only needed when it is not inside `directory`.
        reads_per_second: Manual reads/s (used when no diagnostics directory).
        writes_per_second: Manual writes/s (used when no diagnostics directory).
        storage_gb: Manual storage GB (used when no diagnostics directory).
        row_size_bytes: Manual average row size in bytes.
        generate_pdf: Render the cost PDF report.
        generate_cloudformation: Generate a Keyspaces CloudFormation template from the schema.
        dc_regions: Region per status DC name, e.g. {"dc-eu": "eu-west-1"}.
        report_prefix: Prefix for generated file names.

    Returns:
        {"cost": {...}, "compatibility": {...}, "functional_readiness": {...}, "artifacts": {...}, "errors": [...]}
    """
    errors: list[str] = []
    artifacts: dict[str, str] = {}
    estimate: Optional[dict[str, Any]] = None
    compat: Optional[dict[str, Any]] = None

    # Explicit schema / prepared inputs get their own compatibility run; otherwise the diagnostics
    # estimate already includes compatibility for everything inside `directory`.
    explicit_compat_inputs = bool(schema_path or prepared)
    if directory and not schema_path:
        collection = check_cluster_collection(cluster_dir=directory)
        if "error" in collection:
            errors.append(f"directory: {collection['error']}")
        else:
            schema_path = collection.get("schema_file")

    if directory:
        res = estimate_cost_from_cassandra_diagnostics(
            region=region, directory=directory, dc_regions=dc_regions, output_name=f"{report_prefix}-estimate.json"
        )
    elif reads_per_second is not None and writes_per_second is not None and storage_gb is not None:
        res = estimate_keyspaces_cost(
            region=region,
            reads_per_second=reads_per_second,
            writes_per_second=writes_per_second,
            storage_gb=storage_gb,
            row_size_bytes=row_size_bytes,
            output_name=f"{report_prefix}-estimate.json",
        )
    else:
        res = {"error": "No workload inputs: pass a diagnostics `directory` or reads/writes/storage."}
    if "error" in res:
        errors.append(f"cost: {res['error']}")
    else:
        estimate = res["result"]
        artifacts["estimate_json"] = res["output_path"]

    if explicit_compat_inputs or (schema_path and not directory):
        res = check_keyspaces_compatibility(
            schema_path=schema_path, prepared=prepared, rowsize=rowsize, output_name=f"{report_prefix}-compat.json"
        )
        if "error" in res:
            errors.append(f"compatibility: {res['error']}")
        else:
            compat = res["result"]
            artifacts["compatibility_json"] = res["output_path"]
    elif estimate and estimate.get("compatibility"):
        compat = estimate["compatibility"]

    if generate_pdf and "estimate_json" in artifacts:
        res = generate_cost_report_pdf(
            estimate_paths=[artifacts["estimate_json"]], output_name=f"{report_prefix}.pdf"
        )
        if "error" in res:
            errors.append(f"pdf: {res['error']}")
        else:
            artifacts["pdf_report"] = res["output_path"]

    cfn_resources = None
    cfn_billing_mode = None
    cfn_warnings: list[str] = []
    if generate_cloudformation and schema_path:
        res = generate_keyspaces_cloudformation(
            schema_path=schema_path,
            estimate_path=artifacts.get("estimate_json"),
            dc_regions=dc_regions,
            output_name=f"{report_prefix}-cfn.yaml",
        )
        if "error" in res:
            errors.append(f"cloudformation: {res['error']}")
        else:
            artifacts["cloudformation_template"] = res["output_path"]
            cfn_resources = res["resource_count"]
            cfn_billing_mode = res["billing_mode"]
            cfn_warnings = res["warnings"]

    blockers = (compat or {}).get("summary", {}).get("total_issues", 0) if compat else None
    query_patterns = ((compat or {}).get("details") or {}).get("query_patterns") or {}
    datacenters = (estimate or {}).get("datacenters") or []
    readiness = {
        "compatibility_checked": compat is not None,
        "unsupported_feature_count": blockers,
        "ready_without_changes": (blockers == 0) if compat is not None else None,
        "ttl_tables": sorted((query_patterns.get("ttl_tables") or {}).keys()),
        "datacenter_count": len(datacenters) or (estimate or {}).get("dc_count"),
        "multi_region_candidate": len(datacenters) > 1 or ((estimate or {}).get("dc_count") or 1) > 1,
        # DCs priced from another DC's sample: {dc: {keyspace: source_dc}}
        "datacenters_using_fallback_sample": (estimate or {}).get("fallback_datacenters") or {},
        "cloudformation_resources": cfn_resources,
        "cloudformation_billing_mode": cfn_billing_mode,
        "cloudformation_warnings": cfn_warnings,
    }

    return {
        "cost": _cost_summary(estimate) if estimate else None,
        "compatibility": compat,
        "functional_readiness": readiness,
        "artifacts": artifacts,
        "errors": errors,
    }


KEYSPACES_TOOLS = [
    save_input_file,
    list_work_files,
    read_cluster_topology,
    check_cluster_collection,
    extract_diagnostics_zip,
    prepare_sampler_download,
    prepare_artifact_upload,
    read_skill_reference,
    estimate_keyspaces_cost,
    estimate_from_workload_spec,
    estimate_cost_from_cassandra_diagnostics,
    check_keyspaces_compatibility,
    generate_keyspaces_cloudformation,
    generate_cost_report_pdf,
    evaluate_keyspaces_migration,
]
