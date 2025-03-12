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

export const RebalanceServerConfigurationOption = ({option}: {option: RebalanceServerOption}) => {
    switch (option.type) {
        case "BOOL":
            return <RebalanceServerConfigurationOptionSwitch option={option} />;
        case "INTEGER":
            return <RebalanceServerConfigurationOptionInteger option={option} />;
        case "SELECT":
            return <RebalanceServerConfigurationOptionSelect option={option} />;
        default:
            return null;
    }
}