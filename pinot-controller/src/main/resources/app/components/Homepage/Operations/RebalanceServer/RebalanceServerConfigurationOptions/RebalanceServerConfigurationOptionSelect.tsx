import {
    Box, FormControl, MenuItem, Select, TextField, Typography
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
                <TextField
                    variant='outlined'
                    fullWidth
                    style={{ width: '100%' }}
                    size='small'
                    select
                    id={`rebalance-server-select-input-${option.name}`}
                    value={value}
                    onChange={(e) => {
                        handleConfigChange(
                            {
                                [option.name]: e.target.value
                            });
                        setValue(e.target.value);
                    }}>
                    {option.allowedValues.map((allowedValue) => (
                        <MenuItem key={allowedValue} value={allowedValue}>{allowedValue}</MenuItem>
                    ))}
                </TextField>
            </FormControl>
            <Typography variant='caption'>{option.description}</Typography>
        </Box>
    );
}