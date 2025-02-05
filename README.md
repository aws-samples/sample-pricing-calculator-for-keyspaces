# Amazon Keyspaces (for Apache Cassandra) Pricing Calculator

This application helps you quickly estimate your monthly costs when using **Amazon Keyspaces (for Apache Cassandra)**. By providing basic workload parameters—such as read/write throughput, data size, and TTL delete rates—you can view an approximate cost breakdown for **Provisioned** and **On-Demand** capacity modes.

## Table of Contents
1. [Features](#features)  
2. [Prerequisites](#prerequisites)  
3. [Installation](#installation)  
4. [Configuration](#configuration)  
5. [Usage](#usage)  
6. [Example Inputs & Outputs](#example-inputs--outputs)  
7. [Contributing](#contributing)  
8. [License](#license)

---

## Features
- **Dynamic Cost Estimation:** Quickly calculate approximate monthly charges based on read/write throughput, average row size, storage needs, TTL deletes, and more.
- **Multi-Region Support:** Optionally replicate to up to five AWS Regions and see how your choices affect total cost.
- **Provisioned vs. On-Demand:** Compare both capacity modes to determine the pricing strategy that best fits your use case.
- **Point-in-Time Recovery (PITR) Toggle:** Easily include or exclude PITR in your cost estimates.

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