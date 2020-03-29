import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import React, {Component} from "react";

export default class  TabPanel extends Component{

    constructor(props) {
        super(props);
        const { children, value, index, ...other } = props;
    }

    render() {
        return (
            <Typography
                component="div"
                role="tabpanel"
                hidden={this.value !== this.index}
                id={`simple-tabpanel-${this.index}`}
                aria-labelledby={`simple-tab-${this.index}`}
                {...this.other}
            >
                {this.value === this.index && <Box p={3}>{this.children}</Box>}
            </Typography>
        );
    }
}