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
import {Box, FormControlLabel, Switch, Typography} from "@material-ui/core";
import React, {useState} from "react";
import {RebalanceServerOption} from "../RebalanceServerOptions";
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