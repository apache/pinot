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
import App from "../App";
import MaterialTable from "material-table";
import Utils from "./Utils";


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


    populateDisplayData(data1,data2) {
  
    
        const tents_name = data1['tenantName'];
        const numServers = data1['ServerInstances'].length;
        const numBrokers = data1['BrokerInstances'].length;
        const numTables = data2['tables'].length
        this.instances.push({tents_name: tents_name, numServers: numServers, numBrokers: numBrokers,numTables: numTables});



        this.setState({instances: this.instances})
    }


    populateInstance(instance) {
        const meta = fetch(App.serverAddress + '/tenants/' + instance +'/metadata');
        const tbl = fetch(App.serverAddress + '/tenants/' + instance +'/tables');
        Promise.all([meta,tbl])
        .then(([res1, res2]) => Promise.all([res1.json(), res2.json()]))
        .then(([data1, data2]) => {
            this.populateDisplayData(data1,data2);
        })
            
            .catch(console.log)
    }



    render() {


        return (


            <div style={{width:"90%", margin: "0 auto"}}>
                <MaterialTable
                    title="Tenants Details"
                    columns={[
                        { title: 'Tenant name', field: 'tents_name' },
                        { title: 'Number of Servers', field: 'numServers' },
                        { title: 'Number of Brokers	', field: 'numBrokers'},
                        { title: 'Number of Tables	', field: 'numTables'},
                    ]}
                    data={this.instances}
                    options={{
                        headerStyle: Utils.getTableHeaderStyles(),
                        search: true
                    }}
                />
            </div>
        );
    }
    componentDidMount() {
        fetch(App.serverAddress+'/tenants ')
            .then(res => res.json())
            .then((data) => {
                data.SERVER_TENANTS.forEach((ins) => {
                    this.populateInstance(ins);
                    


                });

            })


            .catch(console.log)



    };


}

export default Tenants;