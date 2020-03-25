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

const useStyles = theme => ({
    table: {
        minWidth: 650,
        maxWidth:1000,
    },
});


class Cluster extends Component {

    classes = useStyles();

    constructor(props) {
        super(props);
    }

    state = {
        tabl : 'cluster',
        rowss : 'cluster'
    };

    createData(name, calories, fat, carbs, protein) {
        return { name, calories, fat, carbs, protein };
    }

    rows = [
        this.createData('Frozen yoghurt', 159, 6.0, 24, 4.0),
        this.createData('Ice cream sandwich', 237, 9.0, 37, 4.3),
        this.createData('Eclair', 262, 16.0, 24, 6.0),
        this.createData('Cupcake', 305, 3.7, 67, 4.3),
        this.createData('Gingerbread', 356, 16.0, 49, 3.9),
    ];

    render() {
        return (
            <div>
                <Card style={{background:"#f5f5f5"}}>
                    <CardContent >
                        <TableContainer component={Paper} >
                            <Table  aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        <TableCell> SERVER TENANT NAME</TableCell>
                                        <TableCell align="right"> SERVER TENANT IP</TableCell>
                                        <TableCell align="right"> BROKER TENANT NAME</TableCell>
                                        <TableCell align="right"> SERVER TENANT IP</TableCell>



                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.rows.map(row => (
                                        <TableRow key={this.state.tabl.SERVER_TENANTS}>
                                            <TableCell component="th" scope="row">
                                                {this.state.tabl.SERVER_TENANTS}
                                            </TableCell>
                                            <TableCell align="right">{this.state.rowss.ServerInstances}</TableCell>
                                            <TableCell align="right">{this.state.tabl.BROKER_TENANTS}</TableCell>
                                            <TableCell align="right">{this.state.rowss.BrokerInstances}</TableCell>

                                        </TableRow>
                                    ))}
                                </TableBody>
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
                                        <TableCell>Dessert (100g serving)</TableCell>
                                        <TableCell align="right">Calories</TableCell>
                                        <TableCell align="right">Fat&nbsp;(g)</TableCell>
                                        <TableCell align="right">Carbs&nbsp;(g)</TableCell>
                                        <TableCell align="right">Protein&nbsp;(g)</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {this.rows.map(row => (
                                        <TableRow key={row.name}>
                                            <TableCell component="th" scope="row">
                                                {row.name}
                                            </TableCell>
                                            <TableCell align="right">{row.calories}</TableCell>
                                            <TableCell align="right">{row.fat}</TableCell>
                                            <TableCell align="right">{row.carbs}</TableCell>
                                            <TableCell align="right">{row.protein}</TableCell>
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
    componentDidMount() {
        fetch('http://localhost:9000/tenants ')
            .then(res => res.json())
            .then((data) => {
                this.setState({ tabl: data })

            })
            .catch(console.log)

        fetch('http://localhost:9000/tenants/DefaultTenant ')
            .then(res1 => res1.json())
            .then((data1) => {
                this.setState({ rowss: data1 })

            })
            .catch(console.log)
            };
    }


export default withStyles(useStyles) (Cluster);
