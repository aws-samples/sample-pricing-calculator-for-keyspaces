import React from 'react';
import { SideNavigation, Box } from '@cloudscape-design/components';

const navItems = [
    { 
        type: 'section',
        text: 'Help & Support',
        items: [
            {
                type: 'link',
                text: (
                    <Box color="text-body-secondary" padding={{ top: 's', bottom: 's' }}>
                        Need help? Our experts can review your estimates and provide guidance.
                        <br /><br />
                        Export the results and email them to{' '}
                        <span style={{ color: '#0972d3', textDecoration: 'underline' }}>
                            amazon-keyspaces-estimates@amazon.com
                        </span>
                    </Box>
                ),
                href: "mailto:amazon-keyspaces-estimates@amazon.com"
            },
            {
                type: 'divider'
            },
            { 
                type: 'link', 
                text: 'Documentation', 
                href: 'https://aws.amazon.com/keyspaces/',
                external: true 
            }
        ]
    }
];

function Navigation({ activeHref, onFollow }) {
    return (
        <SideNavigation
            header={{
                href: '#/',
                text: 'Amazon Keyspaces (for Apache Cassandra) pricing calculator'
            }}
            items={navItems}
            activeHref={activeHref}
            onFollow={onFollow}
        />
    );
}

export default Navigation;