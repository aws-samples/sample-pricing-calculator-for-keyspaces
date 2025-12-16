import jsPDF from 'jspdf';
import 'jspdf-autotable';
import intuitLogo from '../data/logo-intuit.png';

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

    createReport(datacenters, regions, estimateResults, pricing, tcoData) {
        console.log('tcoData', tcoData);
        this.doc = new jsPDF();
        this.yPosition = 20;
        this.xPosition = 20;

        this.addTitle();
        //this.addDate();
        this.addExecutiveSummary(datacenters, regions, estimateResults, pricing, tcoData);
        this.addIntroduction();
        this.customerQuote();
        //this.doc.addPage();
        this.addCostSummary(pricing);
        this.addResultsTables(datacenters, regions, estimateResults);
       
        this.addPricingTables(pricing);
       
        this.addAssumptions();

        this.addCassandraTCOSection(datacenters, tcoData);

        // Save the PDF
        this.doc.save('keyspaces-pricing-estimate.pdf');
    }

    addTitle() {
        this.doc.setFontSize(20);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text('Amazon Keyspaces (for Apache Cassandra)', this.xPosition, this.yPosition);
        this.yPosition += 10;
        this.doc.text('Pricing estimate report', this.xPosition, this.yPosition);
        this.yPosition += 20;
    }

    addDate() {
        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'normal');
        this.doc.text(`Generated on: ${new Date().toLocaleDateString()}`, this.xPosition, this.yPosition);
        this.yPosition += 15;
    }

    addExecutiveSummary(datacenters, regions, estimateResults, pricing, tcoData) {
        if (!pricing) return;

        // Calculate summary statistics
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

        // Calculate total Cassandra TCO if available
        let totalCassandraTCO = 0;
        let instanceCost = 0;
        let storageCost = 0;
        let backupCost = 0;
        let networkCost = 0;
        let operationsCost = 0;
        let perNodeCost = 0;
        let totalNodeCost = 0;
        if (tcoData) {
            datacenters.forEach(dc => {
                const tco = tcoData[dc.name];
                if (!tco) return;

                 instanceCost +=  (tco.single_node?.instance?.monthly_cost || 0) * dc.nodeCount;
                 storageCost += (tco.single_node?.storage?.monthly_cost || 0) * dc.nodeCount;
                 backupCost += (tco.single_node?.backup?.monthly_cost || 0) * dc.nodeCount;
                 const networkOutCost = (tco.single_node?.network_out?.monthly_cost || 0);
                 const networkInCost = (tco.single_node?.network_in?.monthly_cost || 0);
                 networkCost += (networkOutCost + networkInCost) * dc.nodeCount;

                 perNodeCost = instanceCost + storageCost + backupCost + networkCost;
                 totalNodeCost += perNodeCost
                 operationsCost += tco.operations?.operator_hours?.monthly_cost ||  0;

                
            });
        }
        totalCassandraTCO = totalNodeCost + operationsCost;


        const summaryContent = `This report provides a comprehensive pricing estimate for migrating your Apache Cassandra workload to Amazon Keyspaces (for Apache Cassandra).`

          this.addSection("Executive Summary", summaryContent, {
            addPageAfter: false
        });

        this.yPosition += 5;

        const keyDtails = 
        `        • Total Datacenters: ${datacenters.length}
        • Total Keyspaces: ${totalKeyspaces}
        • Total Live Storage: ${Math.round(totalStorageGB)} GB
        • Total Write Operations: ${Math.round(totalWritesPerSecond)} per second
        • Total Read Operations: ${Math.round(totalReadsPerSecond)} per second`

        this.addSubSection("Cassandra cluster:", keyDtails, {
            addPageAfter: false
        });

        this.yPosition += 5;

        
        var infrastructureContent = 
        `        • Instance Cost: ${formatCurrency(instanceCost || 0)}
        • Storage Cost: ${formatCurrency(storageCost || 0)}
        • Backup Cost: ${formatCurrency(backupCost || 0)}
        • Network Cost: ${formatCurrency(networkCost || 0)}
        • Operations Cost: ${formatCurrency(operationsCost || 0)}
        -----------------------------------------------------------
        • Total MonthlyCost: ${formatCurrency(totalCassandraTCO)}
        • Total Annual Cost: ${formatCurrency(totalCassandraTCO * 12)}`;

        if (totalCassandraTCO === 0) {
            infrastructureContent = `TCO data was not provided. Check file and upload section to add the total cost of ownership details.` 
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

    addIntroduction() {
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

    customerQuote(){
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

    addCostSummary(pricing) {
        if (!pricing) return;

        const cost_summary = 'The following section outlines the estimation process for Amazon Keyspaces. It begins by detailing the inputs used to generate the estimate, followed by the output of the Keyspaces cost estimate.'
        this.addSection("Estimate summary", cost_summary, {
            addPageAfter: false
        });
        
        this.yPosition += 10;
        //this.doc.setFontSize(12);
        //this.doc.setFont('helvetica', 'normal');
        //this.doc.text(`Total monthly estimate (Provisioned): ${formatCurrency(pricing.total_monthly_provisioned_cost)}, Total yearly estimate (Provisioned):  ${formatCurrency(pricing.total_monthly_provisioned_cost * 12)}`, this.xPosition, this.yPosition);
        //this.yPosition += 8;
        //this.doc.text(`Total monthly estimate (On-Demand): ${formatCurrency(pricing.total_monthly_on_demand_cost)}, Total yearly estimate (On-Demand):  ${formatCurrency(pricing.total_monthly_on_demand_cost * 12)}`, this.xPosition, this.yPosition);
        //this.yPosition += 15;
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
            this.addSection(`Input details - DC:${datacenter.name} to AWS Region:${regions[datacenter.name] || 'Unknown Region'} `, dc_summary, {
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
            this.addSection(`Keyspaces estimate - DC:${datacenter} to AWS Region:${data.region}`, dc_summary, {
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

    addCassandraTCOSection(datacenters, tcoData) {
        if (!tcoData || !datacenters || datacenters.length === 0) return;

        // Check if we need a new page
        if (this.yPosition > 200) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        const sectionTitle = 'Cassandra TCO (Total Cost of Ownership)';
        const sectionDescription = 'The following table shows the current Cassandra infrastructure costs per datacenter. Costs are calculated per node and multiplied by the total number of nodes in each datacenter.';
        
        this.addSection(sectionTitle, sectionDescription, {
            addPageAfter: false
        });

        // Prepare TCO table data
        const tcoTableData = [];
        let totalTCO = 0;

        datacenters.forEach(dc => {
            const tco = tcoData[dc.name];
            if (!tco) return;

            // Calculate per-node costs
            const instanceCost =  tco.single_node?.instance?.monthly_cost || 0;
            const storageCost = tco.single_node?.storage?.monthly_cost || 0;
            const backupCost =  tco.single_node?.backup?.monthly_cost || 0;
            const networkOutCost = tco.single_node?.network_out?.monthly_cost || 0;
            const networkInCost = tco.single_node?.network_in?.monthly_cost || 0;
            const networkCost = networkOutCost + networkInCost;

            // Total per-node cost
            const perNodeCost = instanceCost + storageCost + backupCost + networkCost;

            // Total node cost for datacenter (per node * number of nodes)
            const totalNodeCost = perNodeCost * dc.nodeCount;

            // Operations cost (already total, not per node)
            const operationsCost = tco.operations?.operator_hours?.monthly_cost || 0;

            // Total TCO for this datacenter
            const datacenterTCO = totalNodeCost + operationsCost;
            totalTCO += datacenterTCO;

            tcoTableData.push([
                dc.name,
                dc.nodeCount.toString(),
                formatCurrency(instanceCost),
                formatCurrency(storageCost),
                formatCurrency(backupCost),
                formatCurrency(networkCost),
                formatCurrency(perNodeCost),
                formatCurrency(totalNodeCost),
                formatCurrency(operationsCost),
                formatCurrency(datacenterTCO)
            ]);
        });

        if (tcoTableData.length === 0) return;

        // Add TCO table
        this.doc.autoTable({
            startY: this.yPosition,
            startX: this.xPosition - 10,
            head: [['Datacenter', 'Nodes', 'Instance/Node', 'Storage/Node', 'Backup/Node', 'Network/Node', 'Per Node Total', 'Node Total (All Nodes)', 'Operations', 'Datacenter TCO']],
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

        this.yPosition = this.doc.lastAutoTable.finalY + 15;

        // Add total TCO summary
        if (this.yPosition > 250) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        this.doc.setFontSize(12);
        this.doc.setFont('helvetica', 'bold');
        this.doc.text(`Self managed Cassandra Total Ownership Cost (All Datacenters): ${formatCurrency(totalTCO)}/month`, this.xPosition, this.yPosition);
        this.yPosition += 15;
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
        this.yPosition += 16;
    }

    /**
     * Core method to render text content with common logic
     * @param {string} content - The text content to add
     * @param {Object} options - Configuration options
     * @param {number} options.contentFontSize - Font size for content (default: 12)
     * @param {number} options.lineHeight - Height between lines (default: 7)
     * @param {number} options.maxWidth - Maximum width for text wrapping (default: 180)
     * @param {number} options.pageBreakThreshold - Y position threshold for page break (default: 280)
     * @param {string} options.fontStyle - Font style: 'normal', 'bold', 'italic' (default: 'normal')
     * @param {number} options.startX - Starting X position (default: this.xPosition)
     * @param {number} options.startY - Starting Y position (default: this.yPosition)
     * @returns {number} The final Y position after rendering
     */
    _renderTextContent(content, options = {}) {
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

        // Set font for content
        this.doc.setFontSize(contentFontSize);
        this.doc.setFont('helvetica', fontStyle);

        // Split content into lines that fit within maxWidth
        const lines = this.doc.splitTextToSize(content, maxWidth);

        // Write each line
        lines.forEach(line => {
            // Check for page break
            if (currentY > pageBreakThreshold) {
                this.doc.addPage();
                currentY = 20;
            }
            
            this.doc.text(line, startX, currentY);
            currentY += lineHeight;
        });

        return currentY;
    }

    /**
     * Core method to render a title
     * @param {string} title - The title text
     * @param {Object} options - Configuration options
     * @param {number} options.titleFontSize - Font size for title (default: 16)
     * @param {number} options.pageBreakThreshold - Y position threshold for page break (default: 280)
     * @param {number} options.startX - Starting X position (default: this.xPosition)
     * @param {number} options.startY - Starting Y position (default: this.yPosition)
     * @param {number} options.maxWidth - Maximum width for title wrapping (default: 180)
     * @param {number} options.lineHeight - Height between title lines (default: 8)
     * @returns {number} The final Y position after rendering
     */
    _renderTitle(title, options = {}) {
        const {
            titleFontSize = 16,
            pageBreakThreshold = 280,
            startX = this.xPosition,
            startY = this.yPosition,
            maxWidth = 180,
            lineHeight = 8
        } = options;

        let currentY = startY;

        // Check if we need a new page
        if (currentY > pageBreakThreshold - 50) {
            this.doc.addPage();
            currentY = 20;
        }

        this.doc.setFontSize(titleFontSize);
        this.doc.setFont('helvetica', 'bold');
        
        // Split title into lines that fit within maxWidth
        const lines = this.doc.splitTextToSize(title, maxWidth);
        
        // Write each line
        lines.forEach(line => {
            this.doc.text(line, startX, currentY);
            currentY += lineHeight;
        });

        return currentY;
    }

    /**
     * Core method to add an image
     * @param {string} imageUrl - URL or base64 string of the image
     * @param {Object} options - Configuration options
     * @param {number} options.imageWidth - Width of the image in mm (default: 60)
     * @param {number} options.imageHeight - Height of the image in mm (default: 40)
     * @param {number} options.startX - Starting X position (default: this.xPosition)
     * @param {number} options.startY - Starting Y position (default: this.yPosition)
     * @returns {number} The final Y position after rendering
     */
    _renderImage(imageUrl, options = {}) {
        const {
            imageWidth = 60,
            imageHeight = 40,
            startX = this.xPosition,
            startY = this.yPosition
        } = options;

        try {
            // Determine image format from URL or use default
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
            // If image fails, just add a placeholder rectangle
            this.doc.rect(startX, startY, imageWidth, imageHeight);
            this.doc.text('Image', startX + imageWidth/2 - 10, startY + imageHeight/2);
        }

        return startY + imageHeight;
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
            addPageAfter = false,
            ...renderOptions
        } = options;

        let currentY = this.yPosition;

        // Add title if provided
        if (title) {
            currentY = this._renderTitle(title, {
                titleFontSize: renderOptions.titleFontSize || 16,
                pageBreakThreshold: renderOptions.pageBreakThreshold || 280,
                startY: currentY
            });
        }

        // Render content
        currentY = this._renderTextContent(content, {
            ...renderOptions,
            startY: currentY
        });

        // Update global position
        this.yPosition = currentY;

        // Add page after content if requested
        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }
    addSubsectionTextContent(content, options = {}) {
        const {
            title,
            addPageAfter = false,
            ...renderOptions
        } = options;

        let currentY = this.yPosition;

        // Add title if provided
        if (title) {
            currentY = this._renderTitle(title, {
                titleFontSize: renderOptions.titleFontSize || 12,
                pageBreakThreshold: renderOptions.pageBreakThreshold || 280,
                startY: currentY
            });
        }

        // Render content
        currentY = this._renderTextContent(content, {
            ...renderOptions,
            startY: currentY
        });

        // Update global position
        this.yPosition = currentY;

        // Add page after content if requested
        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    /**
     * Add a section with title and content, optionally with an image
     * @param {string} title - Section title
     * @param {string} content - Section content
     * @param {Object} options - Additional options
     * @param {string} options.imageUrl - Optional URL or base64 string of the image
     * @param {number} options.imageWidth - Width of the image in mm (default: 60)
     * @param {number} options.imageHeight - Height of the image in mm (default: 40)
     * @param {number} options.imageMargin - Margin between image and text in mm (default: 10)
     * @param {boolean} options.addPageAfter - Whether to add a new page after content (default: false)
     */
    addSection(title, content, options = {}) {
        const {
            imageUrl,
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            addPageAfter = false,
            ...textOptions
        } = options;

        // If image is provided, create side-by-side layout
        if (imageUrl) {
            this.addSectionWithImage(content, imageUrl, {
                imageWidth,
                imageHeight,
                imageMargin,
                addPageAfter,
                ...textOptions
            });
        } else {
            // Use the simplified text-only method
            this.addTextContent(content, {
                title,
                addPageAfter,
                ...textOptions
            });
        }
    }
    addSubSection(title, content, options = {}) {
        const {
            imageUrl,
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            addPageAfter = false,
            ...textOptions
        } = options;

        // If image is provided, create side-by-side layout
        if (imageUrl) {
            this.addSectionWithImage(content, imageUrl, {
                imageWidth,
                imageHeight,
                imageMargin,
                addPageAfter,
                ...textOptions
            });
        } else {
            // Use the simplified text-only method
            this.addSubsectionTextContent(content, {
                title,
                addPageAfter,
                ...textOptions
            });
        }
    }

    /**
     * Add a section with image and text side by side
     * @param {string} content - Section content
     * @param {string} imageUrl - URL or base64 string of the image
     * @param {Object} options - Configuration options
     */
    addSectionWithImage(content, imageUrl, options = {}) {
        const {
            imageWidth = 60,
            imageHeight = 40,
            imageMargin = 10,
            maxWidth = 180,
            pageBreakThreshold = 280,
            addPageAfter = false,
            ...textOptions
        } = options;

        // Calculate layout dimensions
        const textWidth = maxWidth - imageWidth - imageMargin;
        const imageX = this.xPosition;
        const textX = imageX + imageWidth + imageMargin;

        // Check if we need a new page for the entire section
        if (this.yPosition + Math.max(imageHeight, 50) > pageBreakThreshold) {
            this.doc.addPage();
            this.yPosition = 20;
        }

        // Add the image
        this._renderImage(imageUrl, {
            imageWidth,
            imageHeight,
            startX: imageX,
            startY: this.yPosition
        });

        // Add the text content with adjusted width
        const finalY = this._renderTextContent(content, {
            ...textOptions,
            maxWidth: textWidth,
            startX: textX,
            startY: this.yPosition
        });

        // Update y position to the bottom of the section
        this.yPosition = Math.max(this.yPosition + imageHeight, finalY) + 10;

        // Add page after content if requested
        if (addPageAfter) {
            this.doc.addPage();
            this.yPosition = 20;
        }
    }

    /**
     * Add a simple paragraph without title
     * @param {string} content - Paragraph content
     * @param {Object} options - Additional options
     */
    addParagraph(content, options = {}) {
        this.addTextContent(content, {
            contentFontSize: 12,
            ...options
        });
    }
}

export default CreatePDFReport; 