#!/usr/bin/env node
/**
 * generate-pdf.ts — Node.js entry point for PDF generation.
 *
 * Reads JSON from stdin (output of calculate.ts / parse-cassandra.ts) and
 * writes keyspaces-pricing-estimate.pdf to the current working directory.
 *
 * Delegates all report logic to src/components/CreatePDFReport.ts — no
 * duplication of PDF-building code here.
 *
 * Usage:
 *   npx ts-node ... parse-cassandra.ts ... | \
 *   npx ts-node --project tsconfig.scripts.json generate-pdf.ts
 */

import fs from 'fs';
import path from 'path';
import CreatePDFReport from '../../../../src/components/CreatePDFReport';

// ---------------------------------------------------------------------------
// Load logo for Node.js (webpack asset imports don't work outside a bundler)
// ---------------------------------------------------------------------------

const projectRoot = path.resolve(__dirname, '../../../../');
const logoPath = path.join(projectRoot, 'src/data/logo-intuit.png');
const logoBase64: string | null = fs.existsSync(logoPath)
    ? 'data:image/png;base64,' + fs.readFileSync(logoPath).toString('base64')
    : null;

// ---------------------------------------------------------------------------
// Subclass that writes the finished PDF to the filesystem instead of
// triggering a browser download.
// ---------------------------------------------------------------------------

class NodePDFReport extends CreatePDFReport {
    private readonly filePath: string;

    constructor(filePath: string) {
        super(logoBase64);
        this.filePath = filePath;
    }

    protected _output(): void {
        const buffer = new Uint8Array(this.doc.output('arraybuffer') as ArrayBuffer);
        fs.writeFileSync(this.filePath, buffer);
        console.error(`PDF saved: ${this.filePath}`);
    }
}

// ---------------------------------------------------------------------------
// Read JSON from stdin and generate the report
// ---------------------------------------------------------------------------

let raw = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk: string) => { raw += chunk; });
process.stdin.on('end', () => {
    let data: Record<string, unknown>;
    try {
        data = JSON.parse(raw);
    } catch (e: unknown) {
        console.error('Failed to parse JSON from stdin:', (e as Error).message);
        process.exit(1);
    }

    const reportData = data.report_data as Record<string, unknown> | undefined;
    if (!reportData) {
        console.error('No report_data found in input. Run calculate.ts or parse-cassandra.ts first.');
        process.exit(1);
    }

    const { datacenters, regions, estimateResults, pricing } = reportData as {
        datacenters: Parameters<CreatePDFReport['createReport']>[0];
        regions: Parameters<CreatePDFReport['createReport']>[1];
        estimateResults: Parameters<CreatePDFReport['createReport']>[2];
        pricing: Parameters<CreatePDFReport['createReport']>[3];
    };

    const outputPath = path.join(process.cwd(), 'keyspaces-pricing-estimate.pdf');
    const report = new NodePDFReport(outputPath);
    report.createReport(datacenters, regions, estimateResults, pricing, null);
});
