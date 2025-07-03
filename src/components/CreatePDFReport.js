import jsPDF from 'jspdf';
import 'jspdf-autotable';

// Function to format currency with commas and proper rounding
const formatCurrency = (amount) => {
    if (amount < 0.01) {
        return `$${Math.ceil(amount * 100) / 100}`;
    }
    if (amount < 1) {
        return `$${amount.toFixed(2)}`;
    }
    return `$${Math.ceil(amount).toLocaleString()}`;
};

class CreatePDFReport {
    constructor() {
        this.doc = null;
        this.yPosition = 20;
        this.xPosition = 20;
    }

    createReport(datacenters, regions, estimateResults, pricing) {
        this.doc = new jsPDF();
        this.yPosition = 20;
        this.xPosition = 20;

        this.addTitle();
        this.addDate();
        this.addIntroduction();
        this.addCostSummary(pricing);
        this.addResultsTables(datacenters, regions, estimateResults);
        this.addPricingTables(pricing);
        this.addAssumptions();

        // Save the PDF
        this.doc.save('keyspaces-pricing-estimate.pdf');
    }

    addTitle() {
        this.doc.setFontSize(20);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text('Amazon Keyspaces (for Apache Cassandra) pricing estimate report', this.xPosition, this.yPosition);
        this.yPosition += 20;
    }

    addDate() {
        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'normal');
        this.doc.text(`Generated on: ${new Date().toLocaleDateString()}`, this.xPosition, this.yPosition);
        this.yPosition += 15;
    }

    addIntroduction() {
        const content = 
`Amazon Keyspaces (for Apache Cassandra) is a serverless, fully managed database service designed to run Cassandra workloads at scale on AWS. It provides CQL API compatibility, allowing applications built for Apache Cassandra to run without code changes. This simplifies modernization efforts while ensuring consistency with existing Cassandra tools and drivers.

As a serverless service, Keyspaces automatically manages capacity and infrastructure. It offers granular controls and observability features, enabling customers to monitor and optimize cost, performance, and table-level isolation according to application requirements. This architecture improves operational agility by eliminating the need to manage nodes or clusters, and allows developers to deploy and operate independently without impacting other applications.

Keyspaces is built for elastic scalability. It automatically scales to meet workload demands—scaling up to provide higher availability and throughput during peak periods, and scaling down to reduce costs during lower utilization. The service also supports multi-Region replication with a 99.999% availability SLA, helping organizations achieve low recovery time objectives (RTO) for business continuity.

Security is integrated by design. Keyspaces supports AWS Identity and Access Management (IAM) for fine-grained access control, encrypts data at rest and in transit by default, and provides point-in-time recovery (PITR) and continuous backups to support data protection and compliance needs.

Unlike self-managed Cassandra, Amazon Keyspaces eliminates operational overhead, including cluster upgrades, compaction and repair management, JVM tuning, and configuration of common settings. This allows teams to focus on data modeling and application development, while Amazon handles the undifferentiated heavy lifting.`;

        this.addSection("Introduction to Amazon Keyspaces", content, {
            addPageAfter: true
        });
    }

    addCostSummary(pricing) {
        if (!pricing) return;

        const cost_summary = 'The following section outlines the estimation process for Amazon Keyspaces. It begins by detailing the inputs used to generate the estimate, followed by the resulting Keyspaces cost estimate.'
        this.addSection("Estimate summary", cost_summary, {
            addPageAfter: false
        });
        
        this.yPosition += 10;
        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'normal');
        this.doc.text(`Total monthly estimate (Provisioned): ${formatCurrency(pricing.total_monthly_provisioned_cost)}, Total yearly estimate (Provisioned):  ${formatCurrency(pricing.total_monthly_provisioned_cost * 12)}`, this.xPosition, this.yPosition);
        this.yPosition += 8;
        this.doc.text(`Total monthly estimate (On-Demand): ${formatCurrency(pricing.total_monthly_on_demand_cost)}, Total yearly estimate (On-Demand):  ${formatCurrency(pricing.total_monthly_on_demand_cost * 12)}`, this.xPosition, this.yPosition);
        this.yPosition += 15;
    }

    addResultsTables(datacenters, regions, estimateResults) {
        datacenters.forEach((datacenter) => {
            const results = estimateResults[datacenter.name];
            if (!results) return;
            
            // Check if we need a new page
            if (this.yPosition > 250) {
                this.doc.addPage();
                this.yPosition = 20;
            }
            

            const dc_summary = 'The following table provides input gathered from the user interface about your existing workload.'
            this.addSection(`${datacenter.name} (${regions[datacenter.name] || 'Unknown Region'}) - Cassandra sizing details`, dc_summary, {
                addPageAfter: false
            });

           
            // Prepare table data
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
            
            // Add table
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
            
            this.yPosition = this.doc.lastAutoTable.finalY + 15;
        });
    }

    addPricingTables(pricing) {
        if (!pricing) return;

        Object.entries(pricing.total_datacenter_cost).forEach(([datacenter, data]) => {
            // Check if we need a new page
            if (this.yPosition > 250) {
                this.doc.addPage();
                this.yPosition = 20;
            }
            
            // Datacenter pricing header
            
            const dc_summary = 'The following table provides Keyspaces estimate based on the inputs provided.'
            this.addSection(`${datacenter} (${data.region}) - Keyspaces estimation`, dc_summary, {
                addPageAfter: false
            });

            // Prepare pricing table data
            const pricingTableData = Object.entries(data.keyspaceCosts).map(([keyspace, costs]) => [
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
            
            // Add pricing table
            this.doc.autoTable({
                startY: this.yPosition,
                startX: this.xPosition-10,
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
            
            this.yPosition = this.doc.lastAutoTable.finalY + 15;
        });
    }

    addAssumptions() {
        // Assumptions section
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
    }

    /**
     * Generic function to add text content to the PDF document
     * @param {string} content - The text content to add
     * @param {Object} options - Configuration options
     * @param {string} options.title - Optional title for the section
     * @param {number} options.titleFontSize - Font size for title (default: 16)
     * @param {number} options.contentFontSize - Font size for content (default: 12)
     * @param {number} options.lineHeight - Height between lines (default: 7)
     * @param {number} options.maxWidth - Maximum width for text wrapping (default: 180)
     * @param {number} options.pageBreakThreshold - Y position threshold for page break (default: 280)
     * @param {boolean} options.addPageAfter - Whether to add a new page after content (default: false)
     * @param {string} options.fontStyle - Font style: 'normal', 'bold', 'italic' (default: 'normal')
     */
    addTextContent(content, options = {}) {
        const {
            title,
            titleFontSize = 16,
            contentFontSize = 12,
            lineHeight = 7,
            maxWidth = 180,
            pageBreakThreshold = 280,
            addPageAfter = false,
            fontStyle = 'normal'
        } = options;

        // Add title if provided
        if (title) {
            // Check if we need a new page
            if (this.yPosition > pageBreakThreshold - 50) {
                this.doc.addPage();
                this.yPosition = 20;
            }

            this.doc.setFontSize(titleFontSize);
            this.doc.setFont('helvetica', 'bold');
            this.doc.text(title, this.xPosition, this.yPosition);
            this.yPosition += 10;
        }

        // Set font for content
        this.doc.setFontSize(contentFontSize);
        this.doc.setFont('helvetica', fontStyle);

        // Split content into lines that fit within maxWidth
        const lines = this.doc.splitTextToSize(content, maxWidth);

        // Write each line
        lines.forEach(line => {
            // Check for page break
            if (this.yPosition > pageBreakThreshold) {
                this.doc.addPage();
                this.yPosition = 20;
            }
            
            this.doc.text(line, this.xPosition, this.yPosition);
            this.yPosition += lineHeight;
        });

        // Add page after content if requested
        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    /**
     * Add a section with title and content
     * @param {string} title - Section title
     * @param {string} content - Section content
     * @param {Object} options - Additional options for addTextContent
     */
    addSection(title, content, options = {}) {
        this.addTextContent(content, {
            title,
            titleFontSize: 16,
            contentFontSize: 12,
            ...options
        });
    }

    /**
     * Add a simple paragraph without title
     * @param {string} content - Paragraph content
     * @param {Object} options - Additional options for addTextContent
     */
    addParagraph(content, options = {}) {
        this.addTextContent(content, {
            contentFontSize: 12,
            ...options
        });
    }
}

export default CreatePDFReport; 