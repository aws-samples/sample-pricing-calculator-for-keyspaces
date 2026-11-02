# Keyspaces Migration — AWS Transform custom agent

An [AWS Transform](https://docs.aws.amazon.com/transform/latest/userguide/developer-tools.html) orchestrator agent that evaluates Apache Cassandra workloads for migration to Amazon Keyspaces. It covers:

- **Cost**: monthly on-demand vs provisioned cost, savings plans, multi-datacenter / multi-Region, PITR, TTL, and an optional self-managed Cassandra TCO comparison.
- **Compatibility**: Cassandra features Amazon Keyspaces does not support (secondary indexes, triggers, materialized views, UDFs/UDAs, LWT in unlogged batches, aggregate queries, row-size/UDT quotas).
- **Functional readiness**: TTL tables, multi-Region topology, and the application changes the compatibility findings require.
- **CloudFormation for the target data model**: a deployable template built from the customer's schema or the proposed SQL-translation model, shaped by the estimate. It's on-demand by default, enables PITR where backups were priced, and puts real AWS Region codes in multi-Region keyspaces. Provisioned capacity with auto scaling is available on request.
- **Other sources**: workloads from other databases, monitoring data or SQL-model options, priced from a workload spec.

The agent reuses this repo's existing skill set. It doesn't reimplement any of it.

**Quickstart:**
1. Run `./build.sh` and the offline tests.
2. Deploy the IAM roles (`iam-roles.yaml`).
3. Run `deploy_agent_full_pipeline(job_orchestrator=True, chat_ui_label="Keyspaces Migration")`.
4. Grant account access.
5. Start a job in the AWS Transform web app.

The step-by-step commands are in [Build and test locally](#build-and-test-locally) and [Deploy and register](#deploy-and-register), and in the [root README's Transform quickstart](../README.md#transform-quickstart).

| Repo asset | How the agent uses it |
|---|---|
| `.claude/skills/amazon-keyspaces-skill/SKILL.md` | Loaded as a Strands Agent Skill (modes, capture commands, defaults, display rules, migration guidance) |
| `scripts/*.ts` + `src/calculator/` | Bundled by `build.sh` into `vendor/calculator/*.js` and called by the Python tools in `src/keyspaces_tools.py` |
| `tools/row-size-sampler.sh`, `tools/prepared-statements-sampler.sh` | Staged into the image and handed to customers as a downloadable zip |
| `test-fixtures/` | Offline test inputs (`tests/`), including the two-datacenter `multi-dc/` cluster |

## Architecture

```
AWS Transform webapp (chat, file upload, job plan)
        │
        ▼
Keyspaces Migration  ──────────── AsyncBaseOrchestrator (agent-builder-sdk-aws-transform)
  ├─ skill: amazon-keyspaces-skill          (domain instructions)
  ├─ agentic MCP: artifacts, job plan, HITL, chat   (built into the SDK runtime)
  └─ custom tools (src/keyspaces_tools.py) ─► node vendor/calculator/<script>.js
       save_input_file / list_work_files
       read_cluster_topology                       ← cluster-bundle.ts    (nodetool status → DCs, node counts, Regions)
       check_cluster_collection                    ← cluster-bundle.ts    (which DCs are collected / pending)
       extract_diagnostics_zip                     (whole-cluster or single-DC zip → <cluster>/<dc>/)
       prepare_sampler_download                    (row-size + prepared-statements samplers as a zip)
       estimate_keyspaces_cost                     ← calculate.ts         (Mode 1, round numbers)
       estimate_from_workload_spec                 ← estimate-workload.ts (Mode 4 options, other databases)
       estimate_cost_from_cassandra_diagnostics    ← parse-cassandra.ts   (Mode 2)
       check_keyspaces_compatibility               ← check-compatibility.ts (Mode 3)
       generate_keyspaces_cloudformation           ← generate-cfn.ts --estimate  (template from the priced model)
       generate_cost_report_pdf                    ← generate-pdf.ts
       evaluate_keyspaces_migration                (cost + compatibility + readiness in one pass)
       read_skill_reference                        (keyspaces-pricing-rules.md, for explaining estimates)
```

Customer files arrive as `CUSTOMER_INPUT` artifacts. The agent downloads them into `/tmp/keyspaces_work/<cluster>/`, runs the tools, and uploads the PDF report, CloudFormation template, and JSON results as `CUSTOMER_OUTPUT` artifacts.

## Diagnostics collection (one node per datacenter)

The agent never connects to the cluster. It guides the customer through the captures:

1. **`nodetool status` first**, from any node. This gives the datacenters and node counts.
2. **Region per datacenter.** A datacenter named after an AWS Region (e.g. `us-east-1`) is priced in that Region. For any other datacenter, the agent asks the customer.
3. **`DESCRIBE SCHEMA`** once, from any node.
4. **Sampler download.** The agent uploads a zip with `row-size-sampler.sh` and `prepared-statements-sampler.sh`. These are the only scripts; nodetool and cqlsh captures are instructions with example commands.
5. **One datacenter at a time**, on one node in that datacenter: `nodetool tablestats`, `nodetool info`, and the prepared-statements sampler pointed at that node. The row-size sampler runs once per cluster.
6. **Upload.** The customer sends files one by one, a zip per datacenter, or one zip with a folder per datacenter name. After each upload the agent reports which datacenters are still pending.

```
<cluster>/
├── status.txt
├── schema.cql
├── us-east-1/   tablestats.txt  info.txt  [prepared_statements.ndjson]  [rowsize.txt]
└── dc-eu/       tablestats.txt  info.txt  [prepared_statements.ndjson]
```

`parse-cassandra.ts` prices each datacenter from its own sample, scaled by that datacenter's node count in `nodetool status`, in its own Region (`--dc-region dc=region`). Prepared statements from all datacenters are merged for the compatibility check.

If only **one** datacenter is sampled, its tablestats + info apply to every datacenter its keyspaces replicate to (per `schema.cql`). That includes reads, and each datacenter is priced in its own Region. If **several** are sampled, each one uses its own sample. A datacenter without one falls back, per keyspace, to the first sampled datacenter in `nodetool status` order; these are listed in `fallback_datacenters`. Multi-datacenter estimates default PITR on.

## Layout

```
transform-agent/
├── build.sh                 # bundle calculator JS + stage the skill (run before every image build)
├── Dockerfile               # canonical AWS Transform orchestrator template + Node.js
├── requirements.txt
├── iam-roles.yaml           # AgentCoreExecutionRole + AWSTransformAgentInvokeRole (us-east-1)
├── agent-card.json          # PublishAgentVersion configuration template (placeholders <...>)
├── .mcp.json                # AWS Transform Agent Toolkit + AWS Transform MCP servers
├── calculator/cluster-bundle.ts   # topology + per-DC collection progress (bundled by build.sh)
├── src/
│   ├── app.py               # AgentRuntimeServer entry point (delayed_timeout=3600)
│   ├── keyspaces_tools.py   # Strands @tool wrappers over the calculator CLIs
│   └── prompts/orchestrator_prompt.md
└── tests/test_keyspaces_tools.py
```

`vendor/` and `skills/` are build outputs (git-ignored).

## Prerequisites

- Python 3.11+, Node.js (for `build.sh`; the image installs its own), AWS CLI, Finch or Docker
- AWS account allowlisted by AWS Transform for agent registration (ask your Solutions Architect)
- The two IAM roles in `iam-roles.yaml`

## Build and test locally

```bash
cd transform-agent
./build.sh

python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade 'agent-builder-sdk-aws-transform>=1.0.0' 'agent-builder-agentic-mcp-aws-transform>=1.0.0' \
  'agent-builder-types-aws-transform>=1.0.0' 'agent-builder-mcp-client-aws-transform>=1.0.0' pytest
AWS_REGION=us-east-1 python -m pytest -q tests

finch build -t keyspaces-transform-agent:dev .     # or: docker build ...
finch run --rm -p 127.0.0.1:8080:8080 keyspaces-transform-agent:dev
curl http://127.0.0.1:8080/ping
```

The tests are offline. They run every tool against `test-fixtures/`, check that the skill parses as a Strands Agent Skill, and check that the orchestrator constructs. Model inference and the AWS Transform job APIs only run once the agent is deployed.

## Deploy and register

1. Create the IAM roles once:
   ```bash
   aws cloudformation deploy --template-file iam-roles.yaml \
     --stack-name aws-transform-agent-iam-roles --capabilities CAPABILITY_NAMED_IAM --region us-east-1
   ```
2. Register the botocore models (once per machine). See the toolkit onboarding in the [AWS Transform Agent Toolkit power](https://github.com/kirodotdev/powers/tree/main/aws-transform-agent-toolkit).
3. Run `./build.sh`, then deploy with the AWS Transform Agent Toolkit MCP server (configured in `.mcp.json`). Ask your assistant to call:
   ```python
   deploy_agent_full_pipeline(
       agent_path="transform-agent",
       agent_name="keyspaces-migration",   # lowercase: also the ECR repo name
       agent_version="1.0.0",
       owner_contact_info="<team email>",
       job_orchestrator=True,
       chat_ui_label="Keyspaces Migration",
   )
   ```
   This builds the ARM64 image, pushes it to ECR, creates the Bedrock AgentCore runtime, and registers the agent with AWS Transform.
4. To publish richer metadata (skills, `chatFileUpload`, code repo), fill the `<...>` placeholders in `agent-card.json` and publish a new version with the toolkit's `publish_agent_version` tool or `aws atxagentregistryexternal publish-agent-version --configuration file://agent-card.json ...`.
5. Debug with the toolkit's `validate_agent_setup`, `list_log_streams`, and `fetch_logs`.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `KEYSPACES_AGENT_MODEL_ID` | `us.anthropic.claude-sonnet-4-5-20250929-v1:0` | Bedrock cross-Region inference profile ID |
| `KEYSPACES_WORK_DIR` | `/tmp/keyspaces_work` | Tool inputs/outputs |
| `KEYSPACES_VENDOR_DIR` | `vendor/calculator` | Bundled calculator CLIs |
| `KEYSPACES_SKILLS_DIR` | `skills` | Agent Skills directory |
| `KEYSPACES_SCRIPT_TIMEOUT` | `300` | Per-CLI timeout (seconds) |

## Keeping the agent in sync with the skill

The agent has no separate copy of the pricing or compatibility logic. After changing `scripts/`, `src/calculator/`, or `SKILL.md`, rerun `./build.sh` and the tests, then rebuild and publish a new agent version.
