/*
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
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import React, {Component} from "react";

export default class  TabPanel extends Component{

    constructor(props) {
        super(props);
        const { children, value, index, ...other } = props;
    }

    render() {
        return (
            <Typography
                component="div"
                role="tabpanel"
                hidden={this.value !== this.index}
                id={`simple-tabpanel-${this.index}`}
                aria-labelledby={`simple-tab-${this.index}`}
                {...this.other}
            >
                {this.value === this.index && <Box p={3}>{this.children}</Box>}
            </Typography>
        );
    }
}