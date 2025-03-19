/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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