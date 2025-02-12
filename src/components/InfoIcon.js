import React from 'react';
import { Icon } from '@cloudscape-design/components';
import Popover from "@cloudscape-design/components/popover";

const InfoIcon = ({ content }) => {
    return (
        <Popover
            dismissButton={false}
            position="top"
            size="large"
            content={content}
        
        >
            <Icon
                name="status-info"
                size="small"
                variant="subtle"
            />
        </Popover>
    );
};

export default InfoIcon;