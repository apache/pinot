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
import React from 'react'
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import NavBar from "./components/NavBar";
import Cluster from "./components/Cluster";
import Servers from "./components/Instance";
import Tables from "./components/Tables";
import Tenants from "./components/Tenants";

class App extends React.Component {

    static serverAddress = "http://localhost:9000";

    constructor(props) {
        super(props);
        this.processState = this.processState.bind(this);
    }

    state = {
        currentState : 'cluster'
    };

    instances = [];

    processState(value) {
        this.setState({currentState:value})
    }

    componentDidMount() {
        console.log('mounting');
        this.loadInstances();
    }

    loadInstances() {
        fetch(App.serverAddress + '/instances')
            .then(res => res.json())
            .then((data) => {
                data.instances.forEach((ins) => {
                    this.populateInstance(ins);
                });

            })
            .catch(console.log)
    }

    populateDisplayData(data) {
        console.log(this.props.instanceName);
            this.instances.push({
                name: data.instanceName,
                enabled: '' + data.enabled,
                port: data.port,
                hostName: data.hostName
            });
            this.setState({instances: this.instances})
    }

    populateInstance(instance) {
        fetch(App.serverAddress + '/instances/' + instance)
            .then(res => res.json())
            .then((data) => {
                this.populateDisplayData(data);
            })
            .catch(console.log)
    }



    render() {
        let displayTile;
        if(this.state.currentState === 'cluster') {
            displayTile = <Cluster></Cluster>
        }
        if(this.state.currentState === 'post1') {
            displayTile = <Tenants></Tenants>
        }
        if(this.state.currentState === 'servers') {
            const inss = this.instances.filter(ins => ins.name.startsWith('Server'))
            displayTile = <Servers instanceName = {'Server'} instances={inss}></Servers>
        }
        if(this.state.currentState === 'brokers') {
            const inss = this.instances.filter(ins => ins.name.startsWith('Broker'))
            displayTile = <Servers instanceName = {'Broker'} instances={inss}></Servers>
        }
        if(this.state.currentState === 'controllers') {
            const inss = this.instances.filter(ins => ins.name.startsWith('Controller'))
            displayTile = <Servers instanceName = {'Controller'} instances={inss}></Servers>
        }
        if(this.state.currentState === 'tables') {
            displayTile = <Tables></Tables>
        }

        return (
            <div>
                <AppBar color="primary" position="static">
                    <Toolbar>
                        <NavBar mutateState={this.processState}/>
                    </Toolbar>
                </AppBar>
                {displayTile}
            </div>
        )
    }
}
export default App