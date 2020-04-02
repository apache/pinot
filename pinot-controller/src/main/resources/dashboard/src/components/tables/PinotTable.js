import React, {Component} from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import TableContainer from "@material-ui/core/TableContainer";
import Paper from "@material-ui/core/Paper";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import TableCell from "@material-ui/core/TableCell";
import TypoGraphy from "@material-ui/core/Typography";
import TableBody from "@material-ui/core/TableBody";
import RealTimeOffline from "../RealTimeOffline";
import App from "../../App";
import Table from "@material-ui/core/Table";
import ServerTable from "./ServerTable";

export default class PinotTable extends Component {

    constructor(props) {
        super(props);
        this.state = {offlineSegments:[], realTimeSegments:[]};
    }

    componentDidMount() {
        if(this.props.table) {
            this.loadSegments();
        }
    }

    offlineSegments = [];
    realTimeSegments = [];

    loadSegments() {
        fetch(App.serverAddress + '/segments/' + this.props.table)
            .then(res => res.json())
            .then((data) => {
                data.forEach((ins) => {
                    this.populateOfflineSegment(ins);
                    this.populateRealtimeSegment(ins);
                });
            })
            .catch(console.log)
    }

    populateOfflineSegment(seg) {
        const segmentNames = seg.OFFLINE;
        if(!segmentNames) return;
        segmentNames.forEach((name) => {
            fetch(App.serverAddress + '/segments/' + this.props.table + '/' + name + '/metadata')
                .then(res => res.json())
                .then((data) => {
                    this.offlineSegments.push(data);
                    this.setState({offlineSegments:this.offlineSegments, realTimeSegments:this.realTimeSegments})
                })
                .catch(console.log)
        });
    }

    populateRealtimeSegment(seg) {
        const segmentNames = seg.REALTIME;
        if(!segmentNames)  return;
        segmentNames.forEach((name) => {
            fetch(App.serverAddress + '/segments/' + this.props.table + '/' + name + '/metadata')
                .then(res => res.json())
                .then((data) => {
                    this.realTimeSegments.push(data);
                    this.setState({offlineSegments:this.offlineSegments, realTimeSegments:this.realTimeSegments})
                })
                .catch(console.log)
        });
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
                                            <TypoGraphy color="inherit" variant="body1" align= "center">
                                                Table {this.props.table}
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
                        <TypoGraphy color="inherit" variant="body1" align= "center">
                            OFFLINE Segment Details
                        </TypoGraphy>
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        <TableCell> Name</TableCell>
                                        <TableCell align="right">State</TableCell>
                                        <TableCell align="right"># Documents</TableCell>
                                        <TableCell align="center">Size</TableCell>
                                        <TableCell align="center">Created Time</TableCell>
                                        <TableCell align="center">Min Data Time</TableCell>
                                        <TableCell align="center">Max Data Time</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.state.offlineSegments.map(segment => (
                                        <TableRow key={segment['segment.name']}>
                                            <TableCell component="th" scope="row">
                                                {segment['segment.name']}
                                            </TableCell>
                                            <TableCell align="right">{segment['segment.type']}</TableCell>
                                            <TableCell align="right">{segment['segment.total.docs']}</TableCell>
                                            <TableCell align="right">{segment['segment.crc']}</TableCell>
                                            <TableCell align="right">{segment['segment.creation.time']}</TableCell>
                                            <TableCell align="right">??</TableCell>
                                            <TableCell align="right">??</TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                    <CardContent >
                        <TypoGraphy color="inherit" variant="body1" align= "center">
                            REALTIME Segment Details
                        </TypoGraphy>
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        <TableCell> Name</TableCell>
                                        <TableCell align="right">State</TableCell>
                                        <TableCell align="right"># Documents</TableCell>
                                        <TableCell align="center">Size</TableCell>
                                        <TableCell align="center">Created Time</TableCell>
                                        <TableCell align="center">Min Data Time</TableCell>
                                        <TableCell align="center">Max Data Time</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.state.realTimeSegments.map(segment => (
                                        <TableRow key={segment['segment.name']}>
                                            <TableCell component="th" scope="row">
                                                {segment['segment.name']}
                                            </TableCell>
                                            <TableCell align="right">{segment['segment.type']}</TableCell>
                                            <TableCell align="right">{segment['segment.total.docs']}</TableCell>
                                            <TableCell align="right">{segment['segment.crc']}</TableCell>
                                            <TableCell align="right">{segment['segment.creation.time']}</TableCell>
                                            <TableCell align="right">??</TableCell>
                                            <TableCell align="right">??</TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
                <ServerTable table = {this.props.table}></ServerTable>
                <Card style={{background:"#f5f5f5"}}>
                    <CardContent>
                        <RealTimeOffline table = {this.props.table}></RealTimeOffline>
                    </CardContent>
                </Card>
            </div>
        );
    }
}