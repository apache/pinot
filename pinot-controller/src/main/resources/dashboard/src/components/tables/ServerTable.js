import React, {Component} from "react";
import TypoGraphy from "@material-ui/core/Typography";
import CardContent from "@material-ui/core/CardContent";
import TableContainer from "@material-ui/core/TableContainer";
import Paper from "@material-ui/core/Paper";
import Table from "@material-ui/core/Table";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import TableCell from "@material-ui/core/TableCell";
import TableBody from "@material-ui/core/TableBody";
import Card from "@material-ui/core/Card";
import App from "../../App";

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
        return <Card style={{background:"#f5f5f5"}}>
            <CardContent >
                <TypoGraphy color="inherit" variant="body1" align= "center">
                    Server Details
                </TypoGraphy>
                <TableContainer component={Paper} >
                    <Table  aria-label="simple table">
                        <TableHead>
                            <TableRow>
                                <TableCell> Name</TableCell>
                                <TableCell align="right">Real Time Segments</TableCell>
                                <TableCell align="right">Real Time Docs</TableCell>
                                <TableCell align="right">Offline Segments</TableCell>
                                <TableCell align="right">Offline docs</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {this.state.servers.map(server => (
                                <TableRow key={server['name']}>
                                    <TableCell component="th" scope="row">
                                        {server.name}
                                    </TableCell>
                                    <TableCell align="right">{server.realTimeSegmentCount}</TableCell>
                                    <TableCell align="right">{server.realtimeDocs}</TableCell>
                                    <TableCell align="right">{server.offlineSegmentCount}</TableCell>
                                    <TableCell align="right">{server.offLineDocs}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </TableContainer>
            </CardContent>
        </Card>
    }
}