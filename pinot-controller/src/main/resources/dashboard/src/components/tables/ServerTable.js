import React, {Component} from "react";
import App from "../../App";
import MaterialTable from "material-table";
import Utils from "../Utils";

export default class ServerTable extends Component {

    constructor(props) {
        super(props);
        this.state  = {servers:[]};

    }


    serverNames = {};
    servers   = [];

    componentDidMount() {
        this.loadServerDetails();
    }

    loadServerDetails() {
        fetch(App.serverAddress + '/segments/' + this.props.table + '/servers')
            .then(res => res.json())
            .then((data) => {
                data.forEach((ins) => {
                    if(ins.tableName && ins.tableName.endsWith('OFFLINE')) {
                        this.populateSegmentDetails(ins.serverToSegmentsMap, this.updateOfflineServer);
                    } else {
                        this.populateSegmentDetails(ins.serverToSegmentsMap , this.updateRealTimeServer);
                    }
                });
            })
            .catch(console.log)
    }

    updateOfflineServer(serverDetails, comp) {
        console.log(comp.offlineServerNames);
        const serverName = serverDetails.name;
        if(comp.serverNames[serverName]) {
            const server = comp.servers.filter(server => server.name === serverName)[0];
            server.offlineSegmentCount++;
            if(serverDetails.docs !== -1) {
                server.offLineDocs = parseInt(server.offLineDocs) + parseInt(serverDetails.docs);
            }
        } else {
            comp.serverNames[serverName] = serverName;
            comp.servers.push({name: serverName, offlineSegmentCount: 1, offLineDocs: serverDetails.docs, realTimeSegmentCount: 0, realtimeDocs: 0})
        }
        comp.setState({servers:comp.servers})
    }


    updateRealTimeServer(serverDetails, comp) {
        const serverName = serverDetails.name;
        if(comp.serverNames[serverName]) {
            const server = comp.servers.filter(server => server.name === serverName)[0];
            server.realTimeSegmentCount++;
            if(serverDetails.docs !== -1) {
                server.realtimeDocs = parseInt(server.realtimeDocs) + parseInt(serverDetails.docs);
            }
        } else {
            comp.serverNames[serverName] = serverName;
            comp.servers.push({name: serverName, realTimeSegmentCount: 1, realtimeDocs: serverDetails.docs, offlineSegmentCount: 0, offLineDocs: 0})
        }
        comp.setState({servers:comp.servers})
    }


    populateSegmentDetails(serverToSegmentsMap, updateFunc) {
        for (var key in serverToSegmentsMap) {
            if (serverToSegmentsMap.hasOwnProperty(key)) {
                var val = serverToSegmentsMap[key];
                const serverName = key;
                const segments = val;
                for(let i=0;i<segments.length;i++) {
                    const segmentNa = segments[i];
                    fetch(App.serverAddress + '/segments/' + this.props.table + '/'  + segmentNa + '/metadata')
                        .then(res => res.json())
                        .then((data) => {
                            updateFunc({name: serverName, segmentName: segmentNa, docs: data['segment.total.docs']}, this);
                        })
                        .catch(console.log)
                }
            }
        }
    }

    render() {
        return (
        <div>
        <MaterialTable
        title="Server Details"
        columns={[
            { title: 'Name', field: 'name' },
            { title: 'Real Time Segments', field: 'realTimeSegmentCount' },
            { title: 'Real Time Docs	', field: 'realtimeDocs'},
            { title: 'Offline Segments', field: 'offlineSegmentCount'},
            { title: 'Offline docs', field: 'offLineDocs'},

        ]}
        data={this.servers}
        options={{
            headerStyle: Utils.getTableHeaderStyles(),
            search: true
        }}
    />
    
    </div>
        )
    
    }
}