import {Box, FormControl, FormControlLabel, Input, InputLabel, Switch, Typography} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
import {
    RebalanceServerConfigurationOptionLabel
} from "./RebalanceServerConfigurationOptionLabel/RebalanceServerConfigurationOptionLabel";

type RebalanceServerConfigurationOptionIntegerProps = {
    option: RebalanceServerOption;
}
export const RebalanceServerConfigurationOptionInteger = ({option}: RebalanceServerConfigurationOptionIntegerProps) => {
    const [value, setValue] = useState<number>(option.defaultValue as number);
    return (
        <Box display='flex' flexDirection='column'>
            <FormControl fullWidth={true}>
                <RebalanceServerConfigurationOptionLabel option={option} />
                <Input id={`rebalance-server-number-input-${option.name}`} type="number" value={value} onChange={(e) => setValue(parseInt(e.target.value))}/>
            </FormControl>
            <Typography variant='caption'>{option.description}</Typography>
        </Box>
    );
}