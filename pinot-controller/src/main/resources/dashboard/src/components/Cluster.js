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

const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Cluster extends Component {

    classes = useStyles();
    instances = new Array();

    constructor(props) {
        super(props);
        this.state = {instances:[]};



    }



    populateDisplayData(data) {
        this.instances['tents_name']=data.tenantName
        this.instances['numServers']=data.ServerInstances.length
        this.instances['numBrokers']=data.BrokerInstances.length


        this.setState({instances: this.instances})
    }

    populateTblDisplayData(data) {
        this.instances['numTables']=data.tables.length
        this.setState({instances: this.instances})
    }

    populateInstance(instance) {

        fetch('http://localhost:9000' + '/tenants/' + instance +'/metadata')
            .then(res => res.json())
            .then((data) => {
                this.populateDisplayData(data);
            })
            .catch(console.log)
    }

    populateInstanceTbl(instance) {
        fetch('http://localhost:9000' + '/tenants/' + instance +'/tables')
            .then(res => res.json())
            .then((data) => {
                this.populateTblDisplayData(data);
            })
            .catch(console.log)
    }



    render() {


        return (


            <div>


                <Card style={{background:"#f5f5f5"}}>
                    <CardContent >
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>

                                        <TableCell> TENANT NAME</TableCell>
                                        <TableCell align="right"> Number of Servers</TableCell>
                                        <TableCell align="right"> Number of Brokers</TableCell>
                                        <TableCell align="right"> Number of Tables</TableCell>



                                   </TableRow>
                                </TableHead>
                                <TableBody>

                                    {/*{this.state.instances.map(instance => (*/}
                                        <TableRow>
                                            <TableCell component="th" scope="row">
                                                {this.state.instances.tents_name}
                                            </TableCell>
                                            <TableCell align="right">{this.state.instances.numServers}</TableCell>
                                            <TableCell align="right">{this.state.instances.numBrokers}</TableCell>
                                            <TableCell align="right">{this.state.instances.numTables}</TableCell>

                                        </TableRow>
                                        {/*))}*/}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
            </div>
        );
    }
    componentDidMount() {
        fetch('http://localhost:9000/tenants ')
            .then(res => res.json())
            .then((data) => {
                data.SERVER_TENANTS.forEach((ins) => {
                    this.populateInstance(ins);
                    this.populateInstanceTbl(ins);


                });

            })


            .catch(console.log)



            };




    }


export default withStyles(useStyles) (Cluster);
