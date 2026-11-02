#!/usr/bin/env npx ts-node
/**
 * generate-cfn.ts
 *
 * Generates an AWS CloudFormation template from a Cassandra CQL schema file.
 * Produces AWS::Cassandra::Keyspace, AWS::Cassandra::Type, and
 * AWS::Cassandra::Table resources with proper dependency ordering.
 *
 * Usage:
 *   npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json \
 *     scripts/generate-cfn.ts --schema schema.cql [options]
 *
 *   cat schema.cql | npx ts-node --require tsconfig-paths/register --project tsconfig.scripts.json \
 *     scripts/generate-cfn.ts
 *
 * Options:
 *   --schema <path>       Path to CQL schema file (or pipe via stdin)
 *   --format json|yaml    Output format (default: yaml)
 *   --billing-mode on_demand|provisioned  (default: on_demand)
 *   --pitr                Enable PointInTimeRecovery on all tables
 *   --include-system      Include system keyspaces in output
 *   --output <path>       Write to file instead of stdout
 *   --estimate <path>     calculate.ts / parse-cassandra.ts / estimate-workload.ts JSON: shape the template
 *                         from the priced model (provisioned RCU/WCU + auto scaling, PITR, DC Regions).
 *                         Billing mode stays ON_DEMAND unless --billing-mode provisioned.
 *                         Explicit --pitr / --dc-region still win.
 *   --dc-region <dc>=<region>  Map a NetworkTopologyStrategy datacenter to an AWS Region for
 *                         MULTI_REGION keyspaces. Repeatable or CSV. DCs named after a Region map to it.
 *   --no-auto-scaling     PROVISIONED tables without AutoScalingSpecifications
 */

import fs from 'fs';
import {
  generateCloudFormationTemplate,
  deriveTemplateOptionsFromEstimate,
  templateToJson,
  templateToYaml,
  type GenerateOptions,
} from 'src/calculator/CloudFormationGenerator';

interface Args {
  schema: string | null;
  format: 'json' | 'yaml';
  billingMode: 'ON_DEMAND' | 'PROVISIONED';
  pitr: boolean;
  includeSystem: boolean;
  skipIncompatible: boolean;
  forceSingleRegion: boolean;
  includeMaterializedViews: boolean;
  output: string | null;
  estimate: string | null;
  dcRegions: Record<string, string>;
  autoScaling: boolean;
}

function parseArgs(argv: string[]): Args {
  const args: Args = {
    schema: null,
    format: 'yaml',
    billingMode: 'ON_DEMAND',
    pitr: false,
    includeSystem: false,
    skipIncompatible: true,
    forceSingleRegion: false,
    includeMaterializedViews: true,
    output: null,
    estimate: null,
    dcRegions: {},
    autoScaling: true,
  };
  let i = 0;
  while (i < argv.length) {
    switch (argv[i]) {
      case '--schema':
        args.schema = argv[++i];
        break;
      case '--format':
        const fmt = (argv[++i] || '').toLowerCase();
        if (fmt === 'json' || fmt === 'yaml') args.format = fmt;
        break;
      case '--billing-mode': {
        const mode = (argv[++i] || '').toLowerCase();
        if (mode === 'provisioned') args.billingMode = 'PROVISIONED';
        else args.billingMode = 'ON_DEMAND';
        break;
      }
      case '--estimate':
        args.estimate = argv[++i];
        break;
      case '--dc-region':
        for (const pair of (argv[++i] || '').split(',')) {
          const eq = pair.indexOf('=');
          if (eq > 0) args.dcRegions[pair.slice(0, eq).trim()] = pair.slice(eq + 1).trim();
        }
        break;
      case '--no-auto-scaling':
        args.autoScaling = false;
        break;
      case '--pitr':
        args.pitr = true;
        break;
      case '--include-system':
        args.includeSystem = true;
        break;
      case '--no-skip-incompatible':
        args.skipIncompatible = false;
        break;
      case '--single-region':
        args.forceSingleRegion = true;
        break;
      case '--no-materialized-views':
        args.includeMaterializedViews = false;
        break;
      case '--output':
        args.output = argv[++i];
        break;
    }
    i++;
  }
  return args;
}

function readStdinSync(): string {
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return '';
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));

  let cql: string | null = null;
  if (args.schema) {
    try {
      cql = fs.readFileSync(args.schema, 'utf8');
    } catch (err: unknown) {
      console.error(`Failed to read schema file: ${(err as Error).message}`);
      process.exit(1);
    }
  } else if (!process.stdin.isTTY) {
    cql = readStdinSync();
  }

  if (!cql) {
    console.error('Usage: generate-cfn.ts --schema <path> [--estimate <estimate.json>] [--dc-region dc=region] [--format json|yaml] [--billing-mode on_demand|provisioned] [--pitr] [--no-auto-scaling] [--output <path>]');
    console.error('       cat schema.cql | generate-cfn.ts [options]');
    process.exit(1);
  }

  let fromEstimate: GenerateOptions = {};
  if (args.estimate) {
    try {
      fromEstimate = deriveTemplateOptionsFromEstimate(JSON.parse(fs.readFileSync(args.estimate, 'utf8')));
    } catch (err: unknown) {
      console.error(`Failed to read estimate file: ${(err as Error).message}`);
      process.exit(1);
    }
  }

  const options: GenerateOptions = {
    // ON_DEMAND unless --billing-mode provisioned; the estimate only sizes provisioned capacity.
    billingMode: args.billingMode,
    pointInTimeRecovery: args.pitr || !!fromEstimate.pointInTimeRecovery,
    pitrKeyspaces: fromEstimate.pitrKeyspaces,
    keyspaceCapacity: fromEstimate.keyspaceCapacity,
    datacenterRegions: { ...(fromEstimate.datacenterRegions ?? {}), ...args.dcRegions },
    autoScalingTargetPercent: args.autoScaling ? undefined : 0,
    includeSystemKeyspaces: args.includeSystem,
    skipIncompatible: args.skipIncompatible,
    forceSingleRegion: args.forceSingleRegion,
    includeMaterializedViews: args.includeMaterializedViews,
    materializedViews: fromEstimate.materializedViews,
  };

  const template = generateCloudFormationTemplate(cql, options);

  if (template.Metadata?.Warnings?.length) {
    for (const w of template.Metadata.Warnings) {
      console.error(`WARNING: ${w}`);
    }
  }

  const output = args.format === 'json' ? templateToJson(template) : templateToYaml(template);

  if (args.output) {
    fs.writeFileSync(args.output, output, 'utf8');
    console.error(`Template written to ${args.output}`);
  } else {
    console.log(output);
  }
}

main();
