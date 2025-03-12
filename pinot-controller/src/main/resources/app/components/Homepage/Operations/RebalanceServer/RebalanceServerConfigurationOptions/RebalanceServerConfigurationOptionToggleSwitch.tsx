import {Box, FormControlLabel, Switch, Typography} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
import {WarningOutlined} from "@material-ui/icons";
import {
    RebalanceServerConfigurationOptionLabel
} from "./RebalanceServerConfigurationOptionLabel/RebalanceServerConfigurationOptionLabel";

type RebalanceServerConfigurationOptionSwitchProps = {
    option: RebalanceServerOption;
}
export const RebalanceServerConfigurationOptionSwitch = ({option}: RebalanceServerConfigurationOptionSwitchProps) => {
    const [isChecked, setIsChecked] = useState<boolean>(option.defaultValue as boolean);
    return (
        <Box display='flex' flexDirection='column'>
            <FormControlLabel
                control={
                    <Switch
                        checked={isChecked}
                        onChange={() => setIsChecked(isChecked => !isChecked)}
                        name={option.name}
                        color="primary"
                    />
                }
                label={<RebalanceServerConfigurationOptionLabel option={option} />}
            />
            <Typography variant='caption'>{option.description}</Typography>
        </Box>
    );
}