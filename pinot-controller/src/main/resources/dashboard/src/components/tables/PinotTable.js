import React, {Component} from "react";
import App from "../../App";
import ServerTable from "./ServerTable";
import MaterialTable from "material-table";
import Utils from "../Utils";

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
                    this.setState({offlineSegments:this.offlineSegments, realTimeSegments:this.realTimeSegments});
                    this.populateSegmentState(this.props.table, this.offlineSegments, 'OFFLINE');
                })
                .catch(console.log)
        });
    }

    populateSegmentState(table, segmentList,segmentType) {
        fetch(App.serverAddress + '/tables/' + table + '/externalview')
            .then(res => res.json())
            .then((data) => {
                if(data[segmentType]) {
                    const segmentDetails = data[segmentType];
                    for (const key in segmentDetails) {
                        if (segmentDetails.hasOwnProperty(key)) {
                            const segValue = segmentDetails[key];
                            if(segmentList.length >0 ) {
                                const segStats = segmentList.filter(segm => segm['segment.name'] === key)[0];
                                for(const key1 in segValue) {
                                    segStats.server = key1;
                                    segStats.state = segValue[key1];
                                }
                            }
                        }
                    }
                }
                this.offlineSegments = this.offlineSegments.sort((seg1,seg2) => {
                    if(seg1.name < seg2.name) {return -1} else return 1;
                });
                this.realTimeSegments = this.realTimeSegments.sort((seg1,seg2) => {
                    if(seg1.name < seg2.name) {return -1} else return 1;
                });
                this.setState({offlineSegments:this.offlineSegments, realTimeSegments:this.realTimeSegments})
            })
            .catch(console.log)
    }

    populateRealtimeSegment(seg) {
        const segmentNames = seg.REALTIME;
        if(!segmentNames)  return;
        segmentNames.forEach((name) => {
            fetch(App.serverAddress + '/segments/' + this.props.table + '/' + name + '/metadata')
                .then(res => res.json())
                .then((data) => {
                    this.realTimeSegments.push(data);
                    this.setState({offlineSegments:this.offlineSegments, realTimeSegments:this.realTimeSegments});
                    this.populateSegmentState(this.props.table, this.realTimeSegments, 'REALTIME');
                })
                .catch(console.log)
        });
    }

    render() {
        return (
            <div style={{width:"90%", margin: "0 auto"}}>
                
                <MaterialTable
                    title="OFFLINE Segment Details"
                    columns={[
                        { title: 'Name', field: 'segment.name' },
                        { title: 'Server', field: 'server' },
                        { title: 'State	', field: 'state'},
                        { title: '# Documents', field: 'segment.total.docs'},
                        { title: 'Size', field: 'segment.crc'},
                        { title: 'Created Time', field: 'segment.creation.time'},
                        { title: 'Min Data Time', label: '??'},
                        { title: 'Max Data Time	', field: '??'},
                    ]}
                    data={this.offlineSegments}
                    options={{
                        headerStyle: Utils.getTableHeaderStyles(),
                        search: true
                    }}
                />
               <MaterialTable
                    title="REALTIME Segment Details"
                    columns={[
                        { title: 'Name', field: 'segment.name' },
                        { title: 'Server', field: 'server' },
                        { title: 'State	', field: 'state'},
                        { title: '# Documents', field: 'segment.total.docs'},
                        { title: 'Size', field: 'segment.crc'},
                        { title: 'Created Time', field: 'segment.creation.time'},
                        { title: 'Min Data Time', label: '??'},
                        { title: 'Max Data Time	', field: '??'},
                    ]}
                    data={this.realTimeSegments}
                    options={{
                        headerStyle: Utils.getTableHeaderStyles(),
                        search: true
                    }}
                />
                <ServerTable table = {this.props.table}></ServerTable>
                
               
            </div>
        );
    }
}