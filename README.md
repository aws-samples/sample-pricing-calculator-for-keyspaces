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

## Table of Contents
1. [Features](#features)  
2. [AI agent skill (Claude & Kiro)](#ai-agent-skill-claude--kiro)  
3. [Prerequisites](#prerequisites)  
4. [Configuration](#configuration)  
5. [Contributing](#contributing)  
6. [Security](#security)  
7. [License](#license)

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
