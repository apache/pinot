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
import TreeView from '@material-ui/lab/TreeView';
import App from "../App";
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import Utils from "./Utils";
import PinotTable from "./tables/PinotTable";
import Link from '@material-ui/core/Link';
import MaterialTable from "material-table";



const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Tables extends Component {

    classes = useStyles();
    tables = [];
    currentTable =  '';
    tableDisplay = '';


    constructor(props) {
        super(props);
        this.state = {instances:[], treeData:{}, currentTable:''};
    }

    componentDidMount() {
        this.loadInstances();
    }

    loadInstances() {
        fetch(App.serverAddress + '/tables')
            .then(res => res.json())
            .then((data) => {
                if(data && data.tables) {
                    this.currentTable = data.tables[0];
                    this.setState({currentTable: this.currentTable});
                }
                data.tables.forEach((ins) => {
                    this.populateTable(ins);
                });
            })
            .catch(console.log)
    }

    populateDisplayData(data, table) {
        this.tables.push({name: table, reportedSizeInBytes: data.reportedSizeInBytes, estimatedSizeInBytes: data.estimatedSizeInBytes});
        this.setState({instances: this.tables})
    }

    populateTable(table) {
        fetch(App.serverAddress + '/tables/' + table + '/size')
            .then(res => res.json())
            .then((data) => {
                this.populateDisplayData(data, table);
                this.populatePropTree(table);
            })
            .catch(console.log)
    }

    populatePropTree(table) {
        fetch(App.serverAddress + '/tables/' + table)
            .then(res => res.json())
            .then((data) => {
                console.log(JSON.stringify(data));
                this.treeData = Utils.populateNode(data,1, 'properties');
                // console.log(this.treeData['children'])
                this.setState({instances: this.tables, treeData: this.treeData})
                
            })
            .catch(console.log)
    }

    displayTable(table) {
        this.currentTable  = table;
        console.log('hi')
        return () =>  {
            this.tableDisplay = <PinotTable table = {this.currentTable}></PinotTable>;
            this.setState({currentTable: this.currentTable });
            

        }
    }

    render() {
        return (
            <div>
          
                <MaterialTable
                    title="Table Details"
                    columns={[
                        { title: 'Table Name', field: 'name' },
                        { title: 'Reported Size', field: 'reportedSizeInBytes' },
                        { title: 'Estimated Size', field: 'estimatedSizeInBytes'},
                        { title: 'Table Properties', field: 'properties'},
                    ]}
                    data={this.tables}
                    localization={{
                        header: {
                            actions: 'Table Properties'
                        },
                      }}
                    // actions={[
                    //     {
                    //       icon: 'check',
                    //       tooltip: 'Save User',
                    //       onClick: (event, rowData) => {return (
                            
                                
                        
                    //     <PinotTable table = {rowData.name}></PinotTable>
                        
                    //       )}
                    //     }
                    //   ]}
                    detailPanel={rowData => {
                        return (
                            <div>
                                
                        
                        <PinotTable table = {rowData.name}></PinotTable>
                        </div>
                        )
                      }}
                    
                    options={{
                        headerStyle: Utils.getTableHeaderStyles(),
                        search: true
                    }}
                />
                
               
            </div>
        );
    }

}


export default withStyles(useStyles) (Tables);
