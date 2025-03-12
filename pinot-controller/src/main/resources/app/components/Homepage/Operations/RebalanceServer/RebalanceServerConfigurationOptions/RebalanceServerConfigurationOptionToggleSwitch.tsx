import {Box, FormControlLabel, Switch, Typography} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
import {WarningOutlined} from "@material-ui/icons";
import {
    RebalanceServerConfigurationOptionLabel
} from "./RebalanceServerConfigurationOptionLabel/RebalanceServerConfigurationOptionLabel";

type RebalanceServerConfigurationOptionSwitchProps = {
    option: RebalanceServerOption;
    handleConfigChange: (config: { [key: string]: string | number | boolean }) => void;
}
export const RebalanceServerConfigurationOptionSwitch = (
    { option, handleConfigChange }: RebalanceServerConfigurationOptionSwitchProps) => {
    const [isChecked, setIsChecked] = useState<boolean>(option.defaultValue as boolean);
    return (
        <Box display='flex' flexDirection='column'>
            <FormControlLabel
                control={
                    <Switch
                        checked={isChecked}
                        onChange={() => {
                            handleConfigChange({
                                [option.name]: !isChecked
                            })
                            setIsChecked(isChecked => !isChecked)
                        }}
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