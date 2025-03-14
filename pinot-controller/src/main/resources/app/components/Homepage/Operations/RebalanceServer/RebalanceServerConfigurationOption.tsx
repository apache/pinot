import {RebalanceServerOption} from "./RebalanceServerOptions";
import {
    RebalanceServerConfigurationOptionSwitch
} from "./RebalanceServerConfigurationOptions/RebalanceServerConfigurationOptionToggleSwitch";
import React from 'react';
import {
    RebalanceServerConfigurationOptionInteger
} from "./RebalanceServerConfigurationOptions/RebalanceServerConfigurationOptionInteger";
import {
    RebalanceServerConfigurationOptionSelect
} from "./RebalanceServerConfigurationOptions/RebalanceServerConfigurationOptionSelect";

export const RebalanceServerConfigurationOption = (
    { option, handleConfigChange }: { option: RebalanceServerOption, handleConfigChange: (config: { [key: string]: string | number | boolean }) => void }) => {
    switch (option.type) {
        case "BOOL":
            return <RebalanceServerConfigurationOptionSwitch option={option} handleConfigChange={handleConfigChange} />;
        case "INTEGER":
            return <RebalanceServerConfigurationOptionInteger option={option} handleConfigChange={handleConfigChange} />;
        case "SELECT":
            return <RebalanceServerConfigurationOptionSelect option={option} handleConfigChange={handleConfigChange} />;
        default:
            return null;
    }
}