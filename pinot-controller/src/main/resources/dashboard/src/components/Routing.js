import React from 'react'
import Cluster from "./Cluster";
import Tables from "./Tables";
import {Route, BrowserRouter as Router, Redirect} from 'react-router-dom'
import App from "../App";
import Tenants from "./Tenants";
import Instance from "./Instance";

export default class Routing extends React.Component{

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
        return <Router>
            <App></App>
            <div>
                <Route exact path="/" render={() => (
                    <Redirect to="/cluster"/>
                )}/>
                <Route path="/cluster" component={Cluster} />
                <Route path="/tables" component={Tables} />
                <Route path="/tenants" component={Tenants} />
                <Route path="/servers" render={(props) => <Instance {...props} instanceName = {'Server'} instances={this.instances} />}/>
                <Route path="/brokers" render={(props) => <Instance {...props} instanceName = {'Broker'} instances={this.instances} />} />
                <Route path="/controllers" render={(props) => <Instance {...props} instanceName = {'Controller'} instances={this.instances} />} />
            </div>
        </Router>
    }

}