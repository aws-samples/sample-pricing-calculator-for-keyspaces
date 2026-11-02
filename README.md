# Pricing Calculator for Amazon Keyspaces (for Apache Cassandra) 

This application helps you quickly estimate your monthly costs when using **Amazon Keyspaces (for Apache Cassandra)**. By providing basic workload parameters—such as read/write throughput, data size, and TTL delete rates—you can view an approximate cost breakdown for **Provisioned** and **On-Demand** capacity modes. You can also determine Keyspaces compatibility when you provide your Cassandra schema.


Try the calculator: [Pricing calculator for Amazon Keyspaces](https://aws-samples.github.io/sample-pricing-calculator-for-keyspaces/)

## AI agent skill (Claude & Kiro)

This repository includes the **Amazon Keyspaces** skill for agents that load project skills:

| Product | Skill path |
|--------|------------|
| **Claude** (e.g. Claude Code) | [`.claude/skills/amazon-keyspaces-skill/SKILL.md`](.claude/skills/amazon-keyspaces-skill/SKILL.md) |
| **Kiro** | [`.kiro/skills/amazon-keyspaces-skill/SKILL.md`](.kiro/skills/amazon-keyspaces-skill/SKILL.md) |

Clone or copy the repo so the skill is on disk.

**Supported modes** (pick the one that matches how you work)

1. **Manual inputs** — You do **not** need a running cluster. You type the basics (throughput, storage, region, backups, and so on), and the agent returns a **cost estimate**. Best when you already know your workload in round numbers.
2. **Live Cassandra / diagnostics** — You **do** have (or can collect) output from your Cassandra cluster—things like **nodetool** and **cqlsh** captures, schema exports, or optional prepared-statement samples. The agent turns that into a **cost estimate** that is grounded in your real topology and data shape, and can **flag Keyspaces compatibility issues** when schema or queries are included.
3. **Compatibility only** — You want a **yes/no-style readout** on whether your CQL schema and typical prepared statements match what Keyspaces supports—**without** asking for a price. Use this for “will my app work?” reviews.
4. **SQL → Keyspaces** — You start from **relational SQL** (`CREATE TABLE`–style models). The agent proposes **three different Cassandra-style designs**, prices **each** option, and helps you compare trade-offs—not a single automatic translation.

For modes **1, 2, and 4**, a **written PDF report** is optional; the agent usually shows results in the conversation first (see the skill’s *PDF reporting* section).

For modes **2, 3, and 4**, the agent can also generate an **AWS CloudFormation template** for the target Keyspaces data model, built from your schema or the proposed SQL model. Templates are on-demand by default. With an estimate, they get PITR and per-datacenter Regions, and provisioned capacity with auto scaling when you ask for provisioned (see the skill's *CloudFormation template* section).

**What else the skill does**
- **Other databases:** DynamoDB, MongoDB, PostgreSQL and similar sources, or monitoring data, are described as a *workload spec* (tables with row counts, column types, reads and writes). `scripts/estimate-workload.ts` converts and prices it, across many keyspaces and Regions.
- **Formulas written out:** add `--explain` to any estimate to see every formula with your numbers, from the Cassandra conversion (sampled rate × N ÷ RF) to each cost line and the savings plans.
- **Guided diagnostics:** the skill asks for `nodetool status` first, then one node per datacenter. Regions can be set per datacenter, and reads and PITR can differ per Region. Datacenters that map to the same Region are priced once.
- **Materialized views:** carried over as tables, priced from their own counters or as `UNLOGGED BATCH` writes with the base table (`--views`, `--view-writes`).
- **Rules reference:** [`keyspaces-pricing-rules.md`](.claude/skills/amazon-keyspaces-skill/keyspaces-pricing-rules.md) documents every pricing and conversion rule the code implements, the AWS Free Tier, data transfer, and common wrong assumptions. Estimates always come from the scripts; the file is for questions and development.

**Can't run an AI assistant?** The scripts work on their own (Node.js 18+): run `./package-skill.sh`, unzip, run `npm install`, then for example `npm run --silent parse-cassandra -- --dir <cluster> --explain > estimate.json`. The skill's Mode 2 section lists the capture commands.

**Example prompts** (wording can vary; these are the simplest patterns):

- Estimate from a live cluster (agent will use **mode 2** and need host, port if not default **9042**, and help gathering diagnostics as in the skill):

  > Provide an estimate for Amazon Keyspaces based on my Cassandra cluster at **10.10.1.1** (port **9042**).

- Estimate from manual throughput and storage (agent will use **mode 1** and may ask for region, row size, TTL, PITR if omitted):

  > Provide an estimate for Amazon Keyspaces with **500** writes per second, **500** reads per second, and **10 TB** of storage.

- Compatibility only (agent will use **mode 3**):

  > Check Amazon Keyspaces compatibility for this table:
  >
  > ```sql
  > CREATE TABLE ...
  > ```

## AWS Transform custom agent

[`transform-agent/`](transform-agent/README.md) packages this skill set as an [AWS Transform](https://docs.aws.amazon.com/transform/latest/userguide/developer-tools.html) custom orchestrator agent, **Keyspaces Migration**. Customers chat with it in the AWS Transform web app. It evaluates an Apache Cassandra (or other) workload for Amazon Keyspaces, covering cost, compatibility and functional readiness, and delivers a PDF report and a CloudFormation template as job artifacts. It runs the same skill and calculator scripts as Claude Code and Kiro, so the numbers match.

How a job works:
1. The agent asks for `nodetool status` first.
2. It confirms a Region per datacenter.
3. It hands out the row-size and prepared-statement samplers as a download.
4. It collects diagnostics one datacenter at a time, as single files or a zip.
5. It reports which datacenters are still pending, then estimates.

### Transform quickstart

**Prerequisites:** Python 3.11+, Node.js 18+, AWS CLI v2, Finch or Docker, and an AWS account allowlisted by AWS Transform for agent registration (ask your AWS account team).

```bash
cd transform-agent

# 1. Build: bundle the calculator scripts, samplers and skill into the agent
./build.sh

# 2. Test locally (offline, no AWS calls)
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade 'agent-builder-sdk-aws-transform>=1.0.0' 'agent-builder-agentic-mcp-aws-transform>=1.0.0' \
  'agent-builder-types-aws-transform>=1.0.0' 'agent-builder-mcp-client-aws-transform>=1.0.0' pytest cfn-lint
AWS_REGION=us-east-1 python -m pytest -q tests

# 3. Run the container locally (loopback only)
finch build -t keyspaces-transform-agent:dev .
finch run --rm -p 127.0.0.1:8080:8080 keyspaces-transform-agent:dev
curl http://127.0.0.1:8080/ping

# 4. One-time IAM roles for AgentCore and AWS Transform (us-east-1)
aws cloudformation deploy --template-file iam-roles.yaml \
  --stack-name aws-transform-agent-iam-roles --capabilities CAPABILITY_NAMED_IAM --region us-east-1
```

5. **Deploy and register** with the AWS Transform Agent Toolkit MCP server (configured in `transform-agent/.mcp.json`). Ask your assistant to run:
   ```python
   deploy_agent_full_pipeline(agent_path="transform-agent", agent_name="keyspaces-migration",
                              agent_version="1.0.0", owner_contact_info="<team email>",
                              job_orchestrator=True, chat_ui_label="Keyspaces Migration")
   ```
6. **Grant access** to each account that should see the agent, including your own, with `update_publisher_access_control`.
7. **Use it:** in the AWS Transform web app, create a job with **Keyspaces Migration** and start with *"Evaluate my Cassandra cluster for Amazon Keyspaces."*

See [`transform-agent/README.md`](transform-agent/README.md) for the architecture, tools, configuration and troubleshooting.

## Table of Contents
1. [Features](#features)  
2. [AI agent skill (Claude & Kiro)](#ai-agent-skill-claude--kiro)  
3. [AWS Transform custom agent](#aws-transform-custom-agent) ([quickstart](#transform-quickstart))  
4. [Prerequisites](#prerequisites)  
5. [Configuration](#configuration)  
6. [Contributing](#contributing)  
7. [Security](#security)  
8. [License](#license)

---

## Features

- **Dynamic Cost Estimation:** Quickly calculate approximate monthly charges based on read/write throughput, average row size, storage needs, TTL deletes, and more.
- **Multi-Region Support:** Optionally replicate to up to five AWS Regions and see how your choices affect total cost.
- **Provisioned vs. On-Demand:** Compare both capacity modes to determine the pricing strategy that best fits your use case.
- **Point-in-Time Recovery (PITR) Toggle:** Easily include or exclude PITR in your cost estimates.
- **Compatibility:** Report output can include Keyspaces compatibility when you provide your current Cassandra schema (web app and agent skill).

---

## Prerequisites
- **Node.js (optional)** – if you are running a local web app or CLI tool that uses JavaScript.
- **Python 3 (optional)** – for [`cassandra_benchmark.py`](cassandra_benchmark.py). Connecting to Amazon Keyspaces with SigV4 and TLS can use the repository’s public CA files **`AmazonRootCA1.pem`** (Amazon Root CA 1) and **`sf-class2-root.crt`** (Starfield Class 2) as `--ssl-certfile` (see the script’s examples).
- **AWS Account** – to reference or verify costs with the official [AWS Keyspaces pricing page](https://aws.amazon.com/keyspaces/pricing/).  
- **Web Browser** – if you are running this as a static web app.

*(Depending on how you deploy or run the calculator, your prerequisites may vary.)*

---

## Configuration

Within the app, you can configure the following parameters:

- **Primary AWS Region**  
  Choose the AWS Region where your Keyspaces data primarily resides (e.g., `us-east-1`, `us-east-2`, etc.).
- **Replicate to AWS Regions** *(0–5 optional regions)*  
  Select additional regions for multi-region replication.
- **Average Read Requests**  
  Estimated average read requests per second (per region).
- **Average Write Requests**  
  Estimated average write requests per second (all regions).
- **Average Row Size (Bytes)**  
  Size of each row in bytes (e.g., 1024 for 1 KB).
- **Storage (GB)**  
  Total amount of data stored, in gigabytes.
- **Point in Time Recovery**  
  Enable or disable PITR to see how it affects cost.
- **TTL Deletes per Second**  
  If your application uses TTL (Time to Live) on rows, estimate how many deletions occur each second.

---



## Contributing

1. Fork this repository.
2. Create a new branch with your feature/fix:
   ```bash
   git checkout -b feature/new-cool-feature
   ```
3. Make your changes and commit:
   ```bash
   git commit -m "Add a new cool feature"
   ```
4. Push to your fork and submit a pull request.

---

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

### Disclaimer
This calculator is an **estimation tool** only. Actual AWS costs will vary based on factors like data distribution, network usage, and additional services. Always consult the official [Amazon Keyspaces pricing](https://aws.amazon.com/keyspaces/pricing/) for the most up-to-date and accurate information.
```
