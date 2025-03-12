import {
    Box,
    FormControl,
    FormControlLabel,
    Input,
    InputLabel, MenuItem,
    Select,
    Switch,
    Typography
} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
import {
    RebalanceServerConfigurationOptionLabel
} from "./RebalanceServerConfigurationOptionLabel/RebalanceServerConfigurationOptionLabel";

type RebalanceServerConfigurationOptionSelectProps = {
    option: RebalanceServerOption;
    handleConfigChange: (config: { [key: string]: string | number | boolean }) => void;
}
export const RebalanceServerConfigurationOptionSelect = (
    { option, handleConfigChange }: RebalanceServerConfigurationOptionSelectProps
) => {
    const [value, setValue] = useState<string>(option.defaultValue as string);
    return (
        <Box display='flex' flexDirection='column'>
            <FormControl fullWidth={true}>
                <RebalanceServerConfigurationOptionLabel option={option} />
                <Select
                    id={`rebalance-server-number-select-${option.name}`}
                    value={value}
                    onChange={(e) => {
                        handleConfigChange({
                            [option.name]: e.target.value as string
                        })
                        setValue(e.target.value as string);
                    }}
                >
                    {option.allowedValues.map((allowedValue) => (<MenuItem key={allowedValue} value={allowedValue}>{allowedValue}</MenuItem>))}
                </Select>
            </FormControl>
            <Typography variant='caption'>{option.description}</Typography>
        </Box>
    );
}