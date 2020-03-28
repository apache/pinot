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


class Instance extends Component {

    classes = useStyles();
    instances = [];

    constructor(props) {
        super(props);
        this.state = {instances:[]};
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
                                                {this.props.instanceName} Summary Table
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
                                        <TableCell>Instance Name</TableCell>
                                        <TableCell align="right">Enabled</TableCell>
                                        <TableCell align="right">Hostname</TableCell>
                                        <TableCell align="right">Port</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.props.instances.map(instance => (
                                        <TableRow key={instance.name}>
                                            <TableCell component="th" scope="row">
                                                {instance.name}
                                            </TableCell>
                                            <TableCell align="right">{instance.enabled}</TableCell>
                                            <TableCell align="right">{instance.hostName}</TableCell>
                                            <TableCell align="right">{instance.port}</TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </CardContent>
                </Card>
            </div>
        );
    }

}

export default withStyles(useStyles) (Instance);
