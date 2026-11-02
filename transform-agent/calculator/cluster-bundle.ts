/**
 * cluster-bundle.ts
 *
 * Report cluster topology and per-datacenter collection progress for the AWS Transform agent.
 * Uses the same ParsingHelpers detectors as parse-cassandra.ts, so "complete" here means
 * parse-cassandra.ts will accept the bundle.
 *
 * Usage:
 *   cluster-bundle.js --status <status.txt>   topology only
 *   cluster-bundle.js --dir <cluster-root>    topology + per-DC bundle progress
 *
 * Expected layout under <cluster-root> (file names are free; files are classified by content):
 *   status.txt                 nodetool status (required, collected first)
 *   schema.cql                 DESCRIBE SCHEMA (once per cluster)
 *   <datacenter>/              one folder per DC, captured on ONE node in that DC:
 *     tablestats.txt, info.txt (required), rowsize.txt, prepared_statements.ndjson (optional)
 */

import fs from 'fs';
import path from 'path';

import {
  parseNodetoolStatus,
  parseNodetoolInfo,
  detectFileType,
  type CassandraFileType,
} from 'src/calculator/ParsingHelpers';

const regionsMap: Record<string, string> = require('src/calculator/data/regions.json');

type FileIndex = Partial<Record<CassandraFileType, string>>;

function classifyDir(dir: string): FileIndex {
  const index: FileIndex = {};
  for (const entry of fs.readdirSync(dir).sort()) {
    const p = path.join(dir, entry);
    if (!fs.statSync(p).isFile()) continue;
    let type: CassandraFileType = 'unknown';
    try {
      type = detectFileType(fs.readFileSync(p, 'utf8'));
    } catch {
      /* unreadable */
    }
    if (type !== 'unknown' && !index[type]) index[type] = p;
  }
  return index;
}

function parseArgs(argv: string[]): { status: string | null; dir: string | null } {
  const out = { status: null as string | null, dir: null as string | null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--status') out.status = argv[++i];
    else if (argv[i] === '--dir') out.dir = argv[++i];
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.status && !args.dir) {
    console.error('Usage: cluster-bundle.js --status <file> | --dir <cluster-root>');
    process.exit(1);
  }

  const rootFiles: FileIndex = args.dir ? classifyDir(args.dir) : {};
  const statusFile = args.status ?? rootFiles.status ?? null;
  const status = statusFile ? parseNodetoolStatus(fs.readFileSync(statusFile, 'utf8')) : new Map<string, number>();

  // Per-DC folders: any sub-directory, matched to a status DC by folder name or by nodetool info's Data Center.
  const bundles: Array<{ dir: string; folder: string; files: FileIndex; infoDc: string | null }> = [];
  if (args.dir) {
    for (const entry of fs.readdirSync(args.dir).sort()) {
      const p = path.join(args.dir, entry);
      if (!fs.statSync(p).isDirectory()) continue;
      const files = classifyDir(p);
      const infoDc = files.info ? parseNodetoolInfo(fs.readFileSync(files.info, 'utf8')).dc || null : null;
      bundles.push({ dir: p, folder: entry, files, infoDc });
    }
  }

  const matched = new Set<string>();
  const datacenters = [...status.entries()].map(([name, nodeCount]) => {
    const lower = name.toLowerCase();
    const bundle =
      bundles.find((b) => b.folder.toLowerCase() === lower) ??
      bundles.find((b) => (b.infoDc ?? '').toLowerCase() === lower) ??
      null;
    if (bundle) matched.add(bundle.dir);
    const missing: string[] = [];
    if (!bundle) missing.push('bundle folder');
    else {
      if (!bundle.files.tablestats) missing.push('tablestats');
      if (!bundle.files.info) missing.push('info');
    }
    return {
      name,
      node_count: nodeCount,
      region_from_name: regionsMap[name] ? name : null,
      bundle_dir: bundle?.dir ?? null,
      info_datacenter: bundle?.infoDc ?? null,
      files: bundle
        ? {
            tablestats: bundle.files.tablestats ?? null,
            info: bundle.files.info ?? null,
            rowsize: bundle.files.rowsize ?? null,
            prepared: bundle.files.prepared ?? null,
          }
        : null,
      complete: missing.length === 0,
      missing,
    };
  });

  // A flat capture (tablestats + info at the cluster root, no DC folders) is one shared sample.
  const rootSample = !!(rootFiles.tablestats && rootFiles.info);
  const completeCount = datacenters.filter((d) => d.complete).length;
  const coverage =
    completeCount === 0
      ? rootSample ? 'shared_sample' : 'none'
      : completeCount === datacenters.length
        ? 'per_datacenter'
        : completeCount === 1 && !rootSample ? 'shared_sample' : 'mixed';

  const schemaFile = rootFiles.schema ?? bundles.map((b) => b.files.schema).find(Boolean) ?? null;
  const result = {
    cluster_dir: args.dir,
    status_file: statusFile,
    schema_file: schemaFile,
    datacenter_count: status.size,
    datacenters,
    unmatched_folders: bundles.filter((b) => !matched.has(b.dir)).map((b) => b.folder),
    root_files: rootFiles,
    collected_datacenters: datacenters.filter((d) => d.complete).map((d) => d.name),
    pending_datacenters: datacenters.filter((d) => !d.complete).map((d) => d.name),
    // How parse-cassandra.ts will use what is on disk:
    //   per_datacenter - every DC has its own sample
    //   shared_sample  - exactly one sample; it applies to every DC each keyspace replicates to
    //   mixed          - several samples; sampled DCs use their own, the rest fall back per keyspace
    //                    to the first sampled DC (status order)
    estimate_coverage: coverage,
    all_datacenters_collected: status.size > 0 && datacenters.every((d) => d.complete),
    ready_to_estimate: status.size > 0 && coverage !== 'none',
  };
  console.log(JSON.stringify(result, null, 2));
}

main();
