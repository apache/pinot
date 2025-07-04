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
import {Box, Typography} from "@material-ui/core";
import React from "react";
import Link from "@material-ui/core/Link";

type LinkTextProps = {
    text: string;
    link: string;
}
const LinkText = ({ text, link }: LinkTextProps) => {
    return (
        <Link href={link} target="_blank" rel="noopener">
            <Typography style={{ fontWeight: 'bold' }} variant="inherit">{text}</Typography>
        </Link>
    );
}
export const RebalanceServerDialogHeader = () => {
    return (
        <Box>
            <Typography style={{ fontWeight: "bold" }} variant="h6">Rebalance Server</Typography>
            <Typography variant="subtitle2">
                Click <LinkText text='here' link='https://docs.pinot.apache.org/operators/operating-pinot/rebalance/rebalance-servers' /> for more details
            </Typography>
        </Box>
    );
}