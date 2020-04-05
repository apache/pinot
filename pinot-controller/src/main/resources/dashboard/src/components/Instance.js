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
import React, {Component} from 'react';
import Table from '@material-ui/core/Table';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import withStyles from "@material-ui/core/styles/withStyles";
import TableBody from "@material-ui/core/TableBody";
import CardContent from "@material-ui/core/CardContent";
import Card from "@material-ui/core/Card";
import TypoGraphy from "@material-ui/core/Typography";
import MaterialTable from "material-table";
import Utils from "./Utils";

const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Instance extends Component {

    classes = useStyles();
    instances = [];

    constructor(props) {
        super(props);
        this.state = {instances:[]};
    }

    componentDidMount() {
        
        this.setState({instances:this.props.instances.filter(ins => ins.name.startsWith(this.props.instanceName))});
        
    }

    render() {
        return (
            <div style={{width:"90%", margin: "0 auto"}}>
                 <MaterialTable
                    title={this.props.instanceName+' Details'}
                    columns={[
                        { title: 'Instance Name', field: 'name' },
                        { title: 'Enabled', field: 'enabled' },
                        { title: 'Hostname', field: 'hostName'},
                        { title: 'Port', field: 'port'},
                    ]}
                    data={this.props.instances.filter(ins => ins.name.startsWith(this.props.instanceName))}
                    options={{
                        headerStyle: Utils.getTableHeaderStyles(),
                        search: true
                    }}
                />
                
                
            </div>
        );
    }

}

export default withStyles(useStyles) (Instance);
