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
import TableBody from "@material-ui/core/TableBody";
import CardContent from "@material-ui/core/CardContent";
import Card from "@material-ui/core/Card";
import App from "../App";
import TypoGraphy from "@material-ui/core/Typography";


const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});
class Tenants extends Component {
    classes = useStyles();
    instances = [];

    constructor(props) {
        super(props);
        this.state = {instances:[]};
    }


    populateDisplayData(data) {
        this.instances['tents_name']=data.tenantName
        this.instances['numServers']=data.ServerInstances.length
        this.instances['numBrokers']=data.BrokerInstances.length


        this.setState({instances: this.instances})
    }

    populateTblDisplayData(data) {
        this.instances['numTables']=data.tables.length
        this.setState({instances: this.instances})
    }

    populateInstance(instance) {

        fetch(App.serverAddress + '/tenants/' + instance +'/metadata')
            .then(res => res.json())
            .then((data) => {
                this.populateDisplayData(data);
            })
            .catch(console.log)
    }

    populateInstanceTbl(instance) {
        fetch(App.serverAddress + '/tenants/' + instance +'/tables')
            .then(res => res.json())
            .then((data) => {
                this.populateTblDisplayData(data);
            })
            .catch(console.log)
    }



    render() {


        return (


            <div style={{width:"90%", margin: "0 auto"}}>
                <Card style={{background:"#f5f5f5"}}>
                    <CardContent >
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        <TableCell>
                                            <TypoGraphy color="inherit" variant="h5" align= "center">
                                                Tenants Summary Table
                                            </TypoGraphy>
                                        </TableCell>
                                    </TableRow>
                                </TableHead>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>


                <Card style={{background:"#f5f5f5"}}>
                    <CardContent >
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>

                                        <TableCell> TENANT NAME</TableCell>
                                        <TableCell align="right"> Number of Servers</TableCell>
                                        <TableCell align="right"> Number of Brokers</TableCell>
                                        <TableCell align="right"> Number of Tables</TableCell>



                                    </TableRow>
                                </TableHead>
                                <TableBody>

                                    {/*{this.state.instances.map(instance => (*/}
                                    <TableRow>
                                        <TableCell component="th" scope="row">
                                            {this.state.instances.tents_name}
                                        </TableCell>
                                        <TableCell align="right">{this.state.instances.numServers}</TableCell>
                                        <TableCell align="right">{this.state.instances.numBrokers}</TableCell>
                                        <TableCell align="right">{this.state.instances.numTables}</TableCell>

                                    </TableRow>
                                    {/*))}*/}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
            </div>
        );
    }
    componentDidMount() {
        fetch(App.serverAddress+'/tenants ')
            .then(res => res.json())
            .then((data) => {
                data.SERVER_TENANTS.forEach((ins) => {
                    this.populateInstance(ins);
                    this.populateInstanceTbl(ins);


                });

            })


            .catch(console.log)



    };


}

export default Tenants;