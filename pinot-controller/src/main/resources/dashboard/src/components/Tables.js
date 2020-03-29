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
import TreeItem from "@material-ui/lab/TreeItem";
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import RealTimeOffline from "./RealTimeOffline";
import Utils from "./Utils";


const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Tables extends Component {

    classes = useStyles();
    tables = [];

    constructor(props) {
        super(props);
        this.state = {instances:[], treeData:{}};
    }

    componentDidMount() {
        this.loadInstances();
    }

    loadInstances() {
        fetch(App.serverAddress + '/tables')
            .then(res => res.json())
            .then((data) => {
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
                this.setState({instances: this.tables, treeData: this.treeData})
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
                                                Tables
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
                                        <TableCell>Table Name</TableCell>
                                        <TableCell align="right">Reported Size</TableCell>
                                        <TableCell align="right">Estimated Size</TableCell>
                                        <TableCell align="center">Table Properties</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.state.instances.map(instance => (
                                        <TableRow key={instance.name}>
                                            <TableCell component="th" scope="row">
                                                {instance.name}
                                            </TableCell>
                                            <TableCell align="right">{instance.reportedSizeInBytes}</TableCell>
                                            <TableCell align="right">{instance.estimatedSizeInBytes}</TableCell>
                                            <TableCell align="left" style={{border:"10px"}}>
                                                <TreeView
                                                    defaultCollapseIcon={<ExpandMoreIcon />}
                                                    defaultExpandIcon={<ChevronRightIcon />}
                                                    defaultExpanded={['2']}>
                                                    {Utils.renderTree(this.state.treeData)}
                                                </TreeView>
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
                <Card style={{background:"#f5f5f5"}}>
                    <CardContent >
                        <RealTimeOffline></RealTimeOffline>
                    </CardContent>
                </Card>
            </div>
        );
    }

}


export default withStyles(useStyles) (Tables);
