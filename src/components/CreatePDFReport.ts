import jsPDF from 'jspdf';
import 'jspdf-autotable';
import { UserOptions, Table } from 'jspdf-autotable';
import intuitLogo from '../data/logo-intuit.png';

declare module 'jspdf' {
    interface jsPDF {
        autoTable(options: UserOptions): void;
        lastAutoTable: Table;
    }
}

const formatCurrency = (amount: number): string => {
    if (amount < 0.01) {
        return `$${Math.ceil(amount * 100) / 100}`;
    }
    if (amount < 1) {
        return `$${amount.toFixed(2)}`;
    }
    return `$${Math.ceil(amount).toLocaleString()}`;
};

// --- Input types ---

interface Datacenter {
    name: string;
    nodeCount: number;
}

interface KeyspaceEstimate {
    writes_per_second: number;
    reads_per_second: number;
    avg_read_row_size_bytes: number;
    avg_write_row_size_bytes: number;
    total_live_space_gb: number;
    uncompressed_single_replica_gb: number;
    ttls_per_second: number;
    replication_factor: number;
}

type EstimateResults = Record<string, Record<string, KeyspaceEstimate>>;
type Regions = Record<string, string>;

interface KeyspaceCost {
    name: string;
    storage: number;
    backup: number;
    reads_provisioned: number;
    writes_provisioned: number;
    reads_on_demand: number;
    writes_on_demand: number;
    ttlDeletes: number;
    provisioned_total: number;
    on_demand_total: number;
}

interface DatacenterCost {
    region: string;
    keyspaceCosts: Record<string, KeyspaceCost>;
}

interface Pricing {
    total_monthly_provisioned_cost: number;
    total_monthly_on_demand_cost: number;
    total_monthly_provisioned_cost_savings: number;
    total_monthly_on_demand_cost_savings: number;
    total_datacenter_cost: Record<string, DatacenterCost>;
}

interface TcoSingleNode {
    instance?: { monthly_cost: number };
    storage?: { monthly_cost: number };
    backup?: { monthly_cost: number };
    network_out?: { monthly_cost: number };
    network_in?: { monthly_cost: number };
    license?: { monthly_cost: number };
}

interface TcoEntry {
    single_node: TcoSingleNode;
    operations?: { operator_hours?: { monthly_cost: number } };
}

type TcoData = Record<string, TcoEntry> | null;

// --- Render option types ---

interface RenderTextOptions {
    contentFontSize?: number;
    lineHeight?: number;
    maxWidth?: number;
    pageBreakThreshold?: number;
    fontStyle?: string;
    startX?: number;
    startY?: number;
}

interface RenderTitleOptions {
    titleFontSize?: number;
    pageBreakThreshold?: number;
    startX?: number;
    startY?: number;
    maxWidth?: number;
    lineHeight?: number;
}

interface RenderImageOptions {
    imageWidth?: number;
    imageHeight?: number;
    startX?: number;
    startY?: number;
}

interface SectionOptions extends RenderTextOptions {
    titleFontSize?: number;
    imageUrl?: string;
    imageWidth?: number;
    imageHeight?: number;
    imageMargin?: number;
    addPageAfter?: boolean;
}

class CreatePDFReport {
    private doc!: jsPDF;
    private yPosition: number = 20;
    private xPosition: number = 20;

    createReport(
        datacenters: Datacenter[],
        regions: Regions,
        estimateResults: EstimateResults,
        pricing: Pricing,
        tcoData: TcoData
    ): void {
        this.doc = new jsPDF();
        this.yPosition = 20;
        this.xPosition = 20;

        this.addTitle();
        this.addExecutiveSummary(datacenters, regions, estimateResults, pricing, tcoData);
        this.addIntroduction();
        this.customerQuote();
        this.addCostSummary(pricing);
        this.addResultsTables(datacenters, regions, estimateResults);
        this.addPricingTables(pricing);
        this.addAssumptions();
        this.addCassandraTCOSection(datacenters, tcoData);

        this.doc.save('keyspaces-pricing-estimate.pdf');
    }

    private addTitle(): void {
        this.doc.setFontSize(20);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text('Amazon Keyspaces (for Apache Cassandra)', this.xPosition, this.yPosition);
        this.yPosition += 10;
        this.doc.text('Pricing estimate report', this.xPosition, this.yPosition);
        this.yPosition += 20;
    }

    private addDate(): void {
        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'normal');
        this.doc.text(`Generated on: ${new Date().toLocaleDateString()}`, this.xPosition, this.yPosition);
        this.yPosition += 15;
    }

    private addExecutiveSummary(
        datacenters: Datacenter[],
        regions: Regions,
        estimateResults: EstimateResults,
        pricing: Pricing,
        tcoData: TcoData
    ): void {
        if (!pricing) return;

        const totalKeyspaces = datacenters.reduce((total, dc) => {
            const results = estimateResults[dc.name];
            return total + (results ? Object.keys(results).length : 0);
        }, 0);

        const totalStorageGB = datacenters.reduce((total, dc) => {
            const results = estimateResults[dc.name];
            if (!results) return total;
            return total + Object.values(results).reduce((dcTotal, data) =>
                dcTotal + data.uncompressed_single_replica_gb, 0);
        }, 0);

        const totalWritesPerSecond = datacenters.reduce((total, dc) => {
            const results = estimateResults[dc.name];
            if (!results) return total;
            return total + Object.values(results).reduce((dcTotal, data) =>
                dcTotal + data.writes_per_second, 0);
        }, 0);

        const totalReadsPerSecond = datacenters.reduce((total, dc) => {
            const results = estimateResults[dc.name];
            if (!results) return total;
            return total + Object.values(results).reduce((dcTotal, data) =>
                dcTotal + data.reads_per_second, 0);
        }, 0);

        let totalCassandraTCO = 0;
        let instanceCost = 0;
        let storageCost = 0;
        let backupCost = 0;
        let networkCost = 0;
        let operationsCost = 0;
        let totalNodeCost = 0;
        let licenseCost = 0;
        if (tcoData) {
            datacenters.forEach(dc => {
                const tco = tcoData[dc.name];
                if (!tco) return;

                instanceCost += (tco.single_node?.instance?.monthly_cost || 0) * dc.nodeCount;
                storageCost += (tco.single_node?.storage?.monthly_cost || 0) * dc.nodeCount;
                backupCost += (tco.single_node?.backup?.monthly_cost || 0) * dc.nodeCount;
                const networkOutCost = tco.single_node?.network_out?.monthly_cost || 0;
                const networkInCost = tco.single_node?.network_in?.monthly_cost || 0;
                networkCost += (networkOutCost + networkInCost) * dc.nodeCount;
                licenseCost += (tco.single_node?.license?.monthly_cost || 0) * dc.nodeCount;

                const perNodeCost = instanceCost + storageCost + backupCost + networkCost;
                totalNodeCost += perNodeCost;
                operationsCost += tco.operations?.operator_hours?.monthly_cost || 0;
            });
        }
        totalCassandraTCO = totalNodeCost + operationsCost;

        const summaryContent = `This report provides a comprehensive pricing estimate for migrating your Apache Cassandra workload to Amazon Keyspaces (for Apache Cassandra).`;

        this.addSection("Executive Summary", summaryContent, {
            addPageAfter: false
        });

        this.yPosition += 5;

        const keyDetails =
            `        • Total Datacenters: ${datacenters.length}
        • Total Keyspaces: ${totalKeyspaces}
        • Total Live Storage: ${Math.round(totalStorageGB)} GB
        • Total Write Operations: ${Math.round(totalWritesPerSecond)} per second
        • Total Read Operations: ${Math.round(totalReadsPerSecond)} per second`;

        this.addSubSection("Cassandra cluster:", keyDetails, {
            addPageAfter: false
        });

        this.yPosition += 5;

        let infrastructureContent =
            `        • Instance Cost: ${formatCurrency(instanceCost || 0)}
        • Storage Cost: ${formatCurrency(storageCost || 0)}
        • Backup Cost: ${formatCurrency(backupCost || 0)}
        • Network Cost: ${formatCurrency(networkCost || 0)}
        • License Cost: ${formatCurrency(licenseCost || 0)}
        • Operations Cost: ${formatCurrency(operationsCost || 0)}
        -----------------------------------------------------------
        • Total MonthlyCost: ${formatCurrency(totalCassandraTCO)}
        • Total Annual Cost: ${formatCurrency(totalCassandraTCO * 12)}`;

        if (totalCassandraTCO === 0) {
            infrastructureContent = `TCO data was not provided. Check file and upload section to add the total cost of ownership details.`;
        }

        this.addSubSection("Self-managed Cassandra cost estimate:", infrastructureContent, {
            addPageAfter: false
        });

        this.yPosition += 5;

        const keyspacesPricingContent =
            `        • Monthly Provisioned Capacity: ${formatCurrency(pricing.total_monthly_provisioned_cost)} / Savings Plan: ${formatCurrency(pricing.total_monthly_provisioned_cost_savings)}
        • Annual Provisioned Cost: ${formatCurrency(pricing.total_monthly_provisioned_cost * 12)} / Savings Plan: ${formatCurrency(pricing.total_monthly_provisioned_cost_savings * 12)}
        -----------------------------------------------------------
        • Monthly On-Demand Capacity: ${formatCurrency(pricing.total_monthly_on_demand_cost)} / Savings Plan: ${formatCurrency(pricing.total_monthly_on_demand_cost_savings)}
        • Annual On-Demand Cost: ${formatCurrency(pricing.total_monthly_on_demand_cost * 12)} / Savings Plan: ${formatCurrency(pricing.total_monthly_on_demand_cost_savings * 12)}
       


        This Keyspaces estimate is based on your current Cassandra cluster configuration and usage patterns. The provisioned pricing model offers predictable costs with 70% target utilization, while on-demand pricing provides flexibility for variable workloads.
        `;

        this.addSubSection("Keyspaces pricing estimate:", keyspacesPricingContent, {
            addPageAfter: true
        });
    }

    private addIntroduction(): void {
        const content =
`Amazon Keyspaces (for Apache Cassandra) is a serverless, fully managed database service that enables you to run Cassandra workloads at scale on AWS without refactoring your applications.

Many customers face challenges operating and scaling self-managed Cassandra clusters — including the complexity of managing infrastructure, tuning performance, handling repairs and upgrades, and meeting demanding availability and compliance requirements.

These challenges can be addressed with a solution that provides serverless infrastructure, elastic scalability, built-in security, and automated operations — all without the need to manage nodes, clusters, or software maintenance tasks.

Amazon Keyspaces uniquely delivers these capabilities through its purpose-built, serverless architecture, seamless integration with AWS security and observability tools, and pay-as-you-go pricing model — differentiating it from self-managed Cassandra, managed services on virtual machines, and other NoSQL offerings.

With 99.999% availability SLA, the ability to double capacity in under 30 minutes, and consistent single-digit millisecond read/write performance, Keyspaces helps customers achieve operational excellence at scale. Leading organizations such as Monzo Bank, Intuit, GE Digital, and Adobe rely on Keyspaces to power critical, high-scale applications.`;

        this.addSection("Introduction", content, {
            addPageAfter: false
        });

        this.yPosition += 10;
    }

    private customerQuote(): void {
        const content = `"In our prior state, if we had to scale out our cluster for more capacity, we would need a lead time of a few weeks. Now, using Amazon Keyspaces, we can accomplish this in 1 day."
        
        - Manoj Mohan, Software Engineer Leader, Intuit`;

        this.addSection("Intuit Zero downtime migration to Amazon Keyspaces", content, {
            imageUrl: intuitLogo,
            imageWidth: 66,
            imageHeight: 25,
            imageMargin: 15,
            addPageAfter: false
        });
    }

    private addCostSummary(pricing: Pricing): void {
        if (!pricing) return;

        const cost_summary = 'The following section outlines the estimation process for Amazon Keyspaces. It begins by detailing the inputs used to generate the estimate, followed by the output of the Keyspaces cost estimate.';
        this.addSection("Estimate summary", cost_summary, {
            addPageAfter: false
        });

        this.yPosition += 10;
    }

    private addResultsTables(datacenters: Datacenter[], regions: Regions, estimateResults: EstimateResults): void {
        datacenters.forEach((datacenter) => {
            const results = estimateResults[datacenter.name];
            if (!results) return;

            if (this.yPosition > 250) {
                this.doc.addPage();
                this.yPosition = 20;
            }

            const dc_summary = 'The following table provides input gathered from the user interface about your existing workload.';
            this.addSection(`Input details - DC:${datacenter.name} to AWS Region:${regions[datacenter.name] || 'Unknown Region'} `, dc_summary, {
                addPageAfter: false
            });

            const tableData = Object.entries(results).map(([keyspace, data]) => [
                keyspace,
                Math.round(data.writes_per_second).toString(),
                Math.round(data.reads_per_second).toString(),
                Math.round((data.avg_read_row_size_bytes + data.avg_write_row_size_bytes) / 2).toString(),
                Math.round(data.total_live_space_gb).toString(),
                Math.round(data.uncompressed_single_replica_gb).toString(),
                Math.round(data.ttls_per_second).toString(),
                data.replication_factor.toString()
            ]);

            this.doc.autoTable({
                startY: this.yPosition,
                head: [['Keyspace', 'Writes per/sec', 'Read per/sec', 'Avg Row Size (bytes)', 'Live Space (GB)', 'Uncompressed (GB)', 'TTL per/sec', 'Replication factor']],
                body: tableData,
                theme: 'grid',
                headStyles: { fillColor: [66, 139, 202] },
                styles: { fontSize: 8 },
                columnStyles: {
                    0: { cellWidth: 30 },
                    1: { cellWidth: 20 },
                    2: { cellWidth: 20 },
                    3: { cellWidth: 20 },
                    4: { cellWidth: 20 },
                    5: { cellWidth: 20 },
                    6: { cellWidth: 20 },
                    7: { cellWidth: 20 }
                }
            });

            this.yPosition = (this.doc.lastAutoTable.finalY ?? this.yPosition) + 15;
        });
    }

    private addPricingTables(pricing: Pricing): void {
        if (!pricing) return;

        Object.entries(pricing.total_datacenter_cost).forEach(([datacenter, data]) => {
            if (this.yPosition > 250) {
                this.doc.addPage();
                this.yPosition = 20;
            }

            const dc_summary = 'The following table provides Keyspaces estimate based on the inputs provided.';
            this.addSection(`Keyspaces estimate - DC:${datacenter} to AWS Region:${data.region}`, dc_summary, {
                addPageAfter: false
            });

            const pricingTableData = Object.entries(data.keyspaceCosts).map(([, costs]) => [
                costs.name,
                formatCurrency(costs.storage),
                formatCurrency(costs.backup),
                formatCurrency(costs.reads_provisioned),
                formatCurrency(costs.writes_provisioned),
                formatCurrency(costs.reads_on_demand),
                formatCurrency(costs.writes_on_demand),
                formatCurrency(costs.ttlDeletes),
                formatCurrency(costs.provisioned_total),
                formatCurrency(costs.on_demand_total)
            ]);

            this.doc.autoTable({
                startY: this.yPosition,
                head: [['Keyspace', 'Storage', 'Backup', 'Prov Reads', 'Prov Writes', 'OnDemand Reads', 'OnDemand Writes', 'TTL Deletes', 'Provisioned Total', 'OnDemand Total']],
                body: pricingTableData,
                theme: 'grid',
                headStyles: { fillColor: [66, 139, 202] },
                styles: { fontSize: 7 },
                columnStyles: {
                    0: { cellWidth: 25 },
                    1: { cellWidth: 18 },
                    2: { cellWidth: 18 },
                    3: { cellWidth: 18 },
                    4: { cellWidth: 18 },
                    5: { cellWidth: 18 },
                    6: { cellWidth: 18 },
                    7: { cellWidth: 18 },
                    8: { cellWidth: 18 },
                    9: { cellWidth: 18 }
                }
            });

            this.yPosition = (this.doc.lastAutoTable.finalY ?? this.yPosition) + 15;
        });
    }

    private addCassandraTCOSection(datacenters: Datacenter[], tcoData: TcoData): void {
        if (!tcoData || !datacenters || datacenters.length === 0) return;

        if (this.yPosition > 200) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        const sectionTitle = 'Cassandra TCO (Total Cost of Ownership)';
        const sectionDescription = 'The following table shows the current Cassandra infrastructure costs per datacenter. Costs are calculated per node and multiplied by the total number of nodes in each datacenter.';

        this.addSection(sectionTitle, sectionDescription, {
            addPageAfter: false
        });

        const tcoTableData: string[][] = [];
        let totalTCO = 0;

        datacenters.forEach(dc => {
            const tco = tcoData[dc.name];
            if (!tco) return;

            const instanceCost = tco.single_node?.instance?.monthly_cost || 0;
            const storageCost = tco.single_node?.storage?.monthly_cost || 0;
            const backupCost = tco.single_node?.backup?.monthly_cost || 0;
            const networkOutCost = tco.single_node?.network_out?.monthly_cost || 0;
            const networkInCost = tco.single_node?.network_in?.monthly_cost || 0;
            const networkCost = networkOutCost + networkInCost;
            const licenseCost = tco.single_node?.license?.monthly_cost || 0;
            const perNodeCost = instanceCost + storageCost + backupCost + networkCost + licenseCost;
            const totalNodeCost = perNodeCost * dc.nodeCount;
            const operationsCost = tco.operations?.operator_hours?.monthly_cost || 0;
            const datacenterTCO = totalNodeCost + operationsCost;
            totalTCO += datacenterTCO;

            tcoTableData.push([
                dc.name,
                dc.nodeCount.toString(),
                formatCurrency(instanceCost),
                formatCurrency(storageCost),
                formatCurrency(backupCost),
                formatCurrency(networkCost),
                formatCurrency(licenseCost),
                formatCurrency(perNodeCost),
                formatCurrency(totalNodeCost),
                formatCurrency(operationsCost),
                formatCurrency(datacenterTCO)
            ]);
        });

        if (tcoTableData.length === 0) return;

        this.doc.autoTable({
            startY: this.yPosition,
            head: [['Datacenter', 'Nodes', 'Instance/Node', 'Storage/Node', 'Backup/Node', 'Network/Node', 'License/Node', 'Per Node Total', 'Node Total (All Nodes)', 'Operations', 'Datacenter TCO']],
            body: tcoTableData,
            theme: 'grid',
            headStyles: { fillColor: [66, 139, 202] },
            styles: { fontSize: 7 },
            columnStyles: {
                0: { cellWidth: 25 },
                1: { cellWidth: 15 },
                2: { cellWidth: 18 },
                3: { cellWidth: 18 },
                4: { cellWidth: 18 },
                5: { cellWidth: 18 },
                6: { cellWidth: 18 },
                7: { cellWidth: 20 },
                8: { cellWidth: 18 },
                9: { cellWidth: 20 }
            }
        });

        this.yPosition = (this.doc.lastAutoTable.finalY ?? this.yPosition) + 15;

        if (this.yPosition > 250) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text(`Self managed Cassandra Total Ownership Cost (All Datacenters): ${formatCurrency(totalTCO)}/month`, this.xPosition, this.yPosition);
        this.yPosition += 15;
    }

    private addAssumptions(): void {
        if (this.yPosition > 250) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        this.doc.setFontSize(14);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text('Assumptions', this.xPosition, this.yPosition);
        this.yPosition += 10;

        this.doc.setFontSize(10);
        this.doc.setFont('helvetica', 'normal');
        this.doc.text('• Provisioned estimate includes 70% target utilization for auto-scaling', 20, this.yPosition);
        this.yPosition += 8;
        this.doc.text('• Costs are calculated based on usage patterns from your Cassandra cluster data', 20, this.yPosition);
        this.yPosition += 8;
        this.doc.text('• Pricing uses Amazon Keyspaces rates for the selected regions', 20, this.yPosition);
        this.yPosition += 16;
    }

    private _renderTextContent(content: string, options: RenderTextOptions = {}): number {
        const {
            contentFontSize = 12,
            lineHeight = 7,
            maxWidth = 180,
            pageBreakThreshold = 280,
            fontStyle = 'normal',
            startX = this.xPosition,
            startY = this.yPosition
        } = options;

        let currentY = startY;

        this.doc.setFontSize(contentFontSize);
        this.doc.setFont('helvetica', fontStyle);

        const lines: string[] = this.doc.splitTextToSize(content, maxWidth);

        lines.forEach(line => {
            if (currentY > pageBreakThreshold) {
                this.doc.addPage();
                currentY = 20;
            }
            this.doc.text(line, startX, currentY);
            currentY += lineHeight;
        });

        return currentY;
    }

    private _renderTitle(title: string, options: RenderTitleOptions = {}): number {
        const {
            titleFontSize = 16,
            pageBreakThreshold = 280,
            startX = this.xPosition,
            startY = this.yPosition,
            maxWidth = 180,
            lineHeight = 8
        } = options;

        let currentY = startY;

        if (currentY > pageBreakThreshold - 50) {
            this.doc.addPage();
            currentY = 20;
        }

        this.doc.setFontSize(titleFontSize);
        this.doc.setFont('helvetica', 'bold');

        const lines: string[] = this.doc.splitTextToSize(title, maxWidth);

        lines.forEach(line => {
            this.doc.text(line, startX, currentY);
            currentY += lineHeight;
        });

        return currentY;
    }

    private _renderImage(imageUrl: string, options: RenderImageOptions = {}): number {
        const {
            imageWidth = 60,
            imageHeight = 40,
            startX = this.xPosition,
            startY = this.yPosition
        } = options;

        try {
            let imageFormat = 'JPEG';
            if (imageUrl.toLowerCase().includes('.png')) {
                imageFormat = 'PNG';
            } else if (imageUrl.toLowerCase().includes('.gif')) {
                imageFormat = 'GIF';
            } else if (imageUrl.toLowerCase().includes('.webp')) {
                imageFormat = 'WEBP';
            }

            this.doc.addImage(imageUrl, imageFormat, startX, startY, imageWidth, imageHeight);
        } catch (error) {
            console.warn('Failed to add image:', error);
            this.doc.rect(startX, startY, imageWidth, imageHeight);
            this.doc.text('Image', startX + imageWidth / 2 - 10, startY + imageHeight / 2);
        }

        return startY + imageHeight;
    }

    addTextContent(content: string, options: SectionOptions & { title?: string } = {}): void {
        const {
            title,
            addPageAfter = false,
            titleFontSize,
            imageUrl: _imageUrl,
            imageWidth: _imageWidth,
            imageHeight: _imageHeight,
            imageMargin: _imageMargin,
            ...renderOptions
        } = options;

        let currentY = this.yPosition;

        if (title) {
            currentY = this._renderTitle(title, {
                titleFontSize: titleFontSize || 16,
                pageBreakThreshold: renderOptions.pageBreakThreshold || 280,
                startY: currentY
            });
        }

        currentY = this._renderTextContent(content, {
            ...renderOptions,
            startY: currentY
        });

        this.yPosition = currentY;

        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    private addSubsectionTextContent(content: string, options: SectionOptions & { title?: string } = {}): void {
        const {
            title,
            addPageAfter = false,
            titleFontSize: _titleFontSize,
            imageUrl: _imageUrl,
            imageWidth: _imageWidth,
            imageHeight: _imageHeight,
            imageMargin: _imageMargin,
            ...renderOptions
        } = options;

        let currentY = this.yPosition;

        if (title) {
            currentY = this._renderTitle(title, {
                titleFontSize: 12,
                pageBreakThreshold: renderOptions.pageBreakThreshold || 280,
                startY: currentY
            });
        }

        currentY = this._renderTextContent(content, {
            ...renderOptions,
            startY: currentY
        });

        this.yPosition = currentY;

        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    addSection(title: string, content: string, options: SectionOptions = {}): void {
        const {
            imageUrl,
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            addPageAfter = false,
            ...textOptions
        } = options;

        if (imageUrl) {
            this.addSectionWithImage(content, imageUrl, {
                imageWidth,
                imageHeight,
                imageMargin,
                addPageAfter,
                ...textOptions
            });
        } else {
            this.addTextContent(content, {
                title,
                addPageAfter,
                ...textOptions
            });
        }
    }

    private addSubSection(title: string, content: string, options: SectionOptions = {}): void {
        const {
            imageUrl,
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            addPageAfter = false,
            ...textOptions
        } = options;

        if (imageUrl) {
            this.addSectionWithImage(content, imageUrl, {
                imageWidth,
                imageHeight,
                imageMargin,
                addPageAfter,
                ...textOptions
            });
        } else {
            this.addSubsectionTextContent(content, {
                title,
                addPageAfter,
                ...textOptions
            });
        }
    }

    private addSectionWithImage(content: string, imageUrl: string, options: SectionOptions = {}): void {
        const {
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            maxWidth = 180,
            pageBreakThreshold = 280,
            addPageAfter = false,
            ...textOptions
        } = options;

        const textWidth = maxWidth - imageWidth - imageMargin;
        const imageX = this.xPosition;
        const textX = imageX + imageWidth + imageMargin;

        if (this.yPosition + Math.max(imageHeight, 50) > pageBreakThreshold) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        this._renderImage(imageUrl, {
            imageWidth,
            imageHeight,
            startX: imageX,
            startY: this.yPosition
        });

        const finalY = this._renderTextContent(content, {
            ...textOptions,
            maxWidth: textWidth,
            startX: textX,
            startY: this.yPosition
        });

        this.yPosition = Math.max(this.yPosition + imageHeight, finalY) + 10;

        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    addParagraph(content: string, options: SectionOptions = {}): void {
        this.addTextContent(content, {
            contentFontSize: 12,
            ...options
        });
    }
}

export default CreatePDFReport;
