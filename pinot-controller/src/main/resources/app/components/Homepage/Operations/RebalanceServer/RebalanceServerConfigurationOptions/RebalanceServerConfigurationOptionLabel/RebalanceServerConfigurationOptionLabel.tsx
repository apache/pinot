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
import {Box, Tooltip, Typography} from "@material-ui/core";
import {ReportProblemOutlined} from "@material-ui/icons";
import React from "react";
import {RebalanceServerOption} from "../../RebalanceServerOptions";

type RebalanceServerConfigurationOptionLabelProps = {
    option: RebalanceServerOption;
}
export const RebalanceServerConfigurationOptionLabel = ({option}: RebalanceServerConfigurationOptionLabelProps) => (
    <Box display='flex' flexDirection='row' alignItems='center'>
        <Typography variant='body2' style={{marginRight: 10, fontWeight: "600"}}>
            {option.label}
        </Typography>
        {option.markWithWarningIcon && (
            <Tooltip title={option.toolTip} arrow placement="right">
                <ReportProblemOutlined color='error' fontSize='small' />
            </Tooltip>
        )}
    </Box>
);