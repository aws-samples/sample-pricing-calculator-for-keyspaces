import React from 'react';
import { FormField, Input, Checkbox, SpaceBetween } from '@cloudscape-design/components';
import InfoIcon from './InfoIcon';
import { formatLabel, getFieldDescription, getFieldInfoContent } from '../utils/formatters';

const InputField = ({regionKey, fieldKey, value, handleInputChange}) => {
    const validateInput = (key, value) => {
        switch (fieldKey) {
            case 'averageTtlDeletesPerSecond':
                if (!Number.isInteger(+value)) {
                    return 'Please enter an integer';
                }
                if (+value < 0) {
                    return 'Value must be 0 or greater';
                }
                return '';
            default:
                return Number.isInteger(+value) ? '' : 'Please enter an integer';
        }
    };

    const error = fieldKey !== 'pointInTimeRecoveryForBackups' ? validateInput(fieldKey, value) : '';

    return (
       
        <FormField 
            
            label={formatLabel(fieldKey)}
             description={<span>
                    {getFieldDescription(fieldKey)} 
                    <InfoIcon content={getFieldInfoContent(fieldKey)} />
                </span>}
            errorText={error}
            >
            {fieldKey === 'pointInTimeRecoveryForBackups' ? (
               
                <Checkbox
                    checked={value}
                    onChange={(e) => handleInputChange({ detail: { name: fieldKey, type: 'checkbox', checked: e.detail.checked } }, regionKey)}
                    
                >
                    Enable Point-in-Time Recovery
                </Checkbox>
            ) :
            fieldKey === 'multipointInTimeRecoveryForBackups' ? (
                <Checkbox
                    checked={value}
                    onChange={(e) => handleInputChange({ detail: { name: fieldKey, type: 'checkbox', checked: e.detail.checked } }, regionKey)}
                    disabled={true}
                  >    Enable Point-in-Time Recovery
                </Checkbox>
            ) :
            fieldKey === 'multiaverageWriteRequestsPerSecond' || fieldKey === 'multistorageSizeInGb'
            || fieldKey === 'multiaverageTtlDeletesPerSecond' ? (
                <Input
                    type="number"
                    value={value}
                    invalid={!!error}
                    disabled={true}
                />
            ) : (
                <Input  
                    type="number"
                    value={value}
                    onChange={(e) => handleInputChange({ detail: { ...e.detail, name: fieldKey } }, regionKey)}
                    invalid={!!error}
                    inputMode="numeric"
                    pattern="^[0-9]*$"
                    
                />
            )}
        </FormField>
    
    );
}

export default InputField;
