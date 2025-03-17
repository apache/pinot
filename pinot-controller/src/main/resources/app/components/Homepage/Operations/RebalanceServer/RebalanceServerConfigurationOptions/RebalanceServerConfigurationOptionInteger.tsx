import {Box, FormControl, TextField, Typography} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
import {
    RebalanceServerConfigurationOptionLabel
} from "./RebalanceServerConfigurationOptionLabel/RebalanceServerConfigurationOptionLabel";

type RebalanceServerConfigurationOptionIntegerProps = {
    option: RebalanceServerOption;
    handleConfigChange: (config: { [key: string]: string | number | boolean }) => void;
}
export const RebalanceServerConfigurationOptionInteger = (
    { option, handleConfigChange }: RebalanceServerConfigurationOptionIntegerProps
) => {
    const [value, setValue] = useState<number>(option.defaultValue as number);
    return (
        <Box display='flex' flexDirection='column'>
            <FormControl fullWidth>
                <RebalanceServerConfigurationOptionLabel option={option} />
                <TextField
                    variant='outlined'
                    fullWidth
                    style={{ width: '100%' }}
                    size='small'
                    id={`rebalance-server-number-input-${option.name}`}
                    type='number'
                    value={value}
                    onChange={(e) => {
                        handleConfigChange(
                            {
                                [option.name]: parseInt(e.target.value)
                            });
                        setValue(parseInt(e.target.value));
                    }}/>
                <Typography variant='caption'>{option.description}</Typography>
            </FormControl>
        </Box>
    );
}