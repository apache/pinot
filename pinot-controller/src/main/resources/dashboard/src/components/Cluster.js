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
import withStyles from "@material-ui/core/styles/withStyles";
import App from "../App";
import MaterialTable from "material-table";
import Utils from "./Utils";

const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Cluster extends Component {

    classes = useStyles();
    instances = [];

    constructor(props) {
        super(props);
        this.state = {instances:[]};
    }
    populateDisplayData(data) {
        const join = data['allowParticipantAutoJoin'];
        const override = data['pinot.broker.enable.query.limit.override'];
        const pql = data['enable.case.insensitive.pql'];
        this.instances.push({join: join, override: override, pql: pql});
        this.setState({instances: this.instances})
    }

    populateTblDisplayData(data) {
        this.instances['numTables']=data.tables.length;
        this.setState({instances: this.instances})
    }

    populateInstance(instance) {
        alert(instance['enable.case.insensitive.pql'])
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
                <MaterialTable
                    title="Cluster Details"
                    columns={[
                        { title: 'Participant-AutoJoin', field: 'join' },
                        { title: 'Broker Querylimit-override', field: 'override' },
                        { title: 'Case-insensitive pql', field: 'pql'},
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
        fetch(App.serverAddress+'/cluster/configs ')
            .then(res => res.json())
            .then((data) => {
                this.populateDisplayData(data);

            })
            .catch(console.log)
    };
}


export default withStyles(useStyles) (Cluster);
