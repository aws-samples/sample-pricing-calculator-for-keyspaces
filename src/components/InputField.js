import React from 'react';
import { FormField, Input, Checkbox } from '@cloudscape-design/components';
import InfoIcon from './InfoIcon';
import { formatLabel, getFieldDescription, getFieldInfoContent } from '../utils/formatters';

const InputField = ({regionKey, fieldKey, value, handleInputChange}) => {
    const validateInput = (name, value) => {
        switch (name) {
            case 'ttlDeletesPerSecond':
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

    const error = fieldKey !== 'pointIntimeRecovery' ? validateInput(fieldKey, value) : '';

    return (
        <FormField
            label={formatLabel(fieldKey)}
            description={<span>
                {getFieldDescription(fieldKey)}
                <InfoIcon content={getFieldInfoContent(fieldKey)} />
            </span>}
            errorText={error}
        >
            {fieldKey === 'pointIntimeRecovery' ? (
                <Checkbox
                    checked={value}
                    onChange={(e) => handleInputChange({ detail: { name: fieldKey, type: 'checkbox', checked: e.detail.checked } }, regionKey)}
                >
                    Enable Point-in-Time Recovery
                </Checkbox>
            ) : (
                <Input
                    type="number"
                    value={value}
                    onChange={(e) => handleInputChange({ detail: { ...e.detail, name: fieldKey } }, regionKey)}
                    invalid={!!error}
                />
            )}
        </FormField>
    );
}

export default InputField;
