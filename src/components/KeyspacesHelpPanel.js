import React from 'react';
import { HelpPanel, Box, Link } from '@cloudscape-design/components';

function KeyspacesHelpPanel() {
    return (
        <HelpPanel 
            header={<h2>Amazon Keyspaces</h2>}
        >
            <Box>
                <Box variant="p" padding={{ bottom: 's' }}>
                    Amazon Keyspaces (for Apache Cassandra) is a scalable, highly available, and managed Apache Cassandra–compatible database service. With Amazon Keyspaces, you can run your Cassandra workloads of any scale on AWS using the same Cassandra application code and developer tools that you use today.
                </Box>
                <h3>Keyspaces on-demand capacity</h3>
                <Box padding={{ bottom: 's' }}>
                    <Link external href="https://docs.aws.amazon.com/keyspaces/latest/devguide/ReadWriteCapacityMode.html#OnDemand">
                        Learn more
                    </Link>
                </Box>
                <h3>Keyspaces provisioned capacity</h3>
                <Box>
                    <Link external href="https://docs.aws.amazon.com/keyspaces/latest/devguide/ReadWriteCapacityMode.html#Provisioned">
                        Learn more
                    </Link>
                </Box>
            </Box>
        </HelpPanel>
    );
}

export default KeyspacesHelpPanel; 