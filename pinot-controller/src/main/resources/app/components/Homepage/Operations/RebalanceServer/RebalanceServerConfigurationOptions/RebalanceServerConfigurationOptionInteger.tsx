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