import React, {useEffect} from 'react';
import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ChevronRightIcon from "@material-ui/icons/ChevronRight";
import Utils from "./Utils";
import TreeView from "@material-ui/lab/TreeView";
import App from "../App";

function TabPanel(props) {
    const { children, value, index, ...other } = props;

    return (
        <Typography
            component="div"
            role="tabpanel"
            hidden={value !== index}
            id={`simple-tabpanel-${index}`}
            aria-labelledby={`simple-tab-${index}`}
            {...other}
        >
            {value === index && <Box p={3}>{children}</Box>}
        </Typography>
    );
}

function a11yProps(index) {
    return {
        id: `simple-tab-${index}`,
        'aria-controls': `simple-tabpanel-${index}`,
    };
}

const useStyles = makeStyles((theme) => ({
    root: {
        flexGrow: 1,
        backgroundColor: theme.palette.background.paper,
    },
}));

export default function SimpleTabs(props) {
    const classes = useStyles();
    const [value, setValue] = React.useState(0);
    const [treeData, setTree] = React.useState({});


    function populateView(viewAddress) {
        fetch(App.serverAddress + '/tables/baseballStats/'+ viewAddress)
            .then(res => res.json())
            .then((data) => {
                console.log(data);
                setTree(Utils.populateNode(data, 1, 'View'));
            })
            .catch(console.log);
    }

    const handleChange = (event, newValue) => {
        setValue(newValue);
        if(newValue == 0) populateExternalView();
        if(newValue == 1) populateIdealStateView();
    };

    const populateExternalView = () => {
        populateView('externalview');
    };

    const populateIdealStateView = () => {
        populateView('idealstate');
    };

    return (
        <div className={classes.root} >
            <AppBar position="static" >
                <Tabs value={value} onChange={handleChange} aria-label="simple tabs example" >
                    <Tab label="External View" {...a11yProps(0)} />
                    <Tab label="Ideal State" {...a11yProps(1)} />
                </Tabs>
            </AppBar>
            <TabPanel value={value} index={0}>
                <TreeView
                    defaultCollapseIcon={<ExpandMoreIcon />}
                    defaultExpandIcon={<ChevronRightIcon />}
                    defaultExpanded={[1,2,3,4,5]}>
                    {Utils.renderTree(treeData)}
                </TreeView>
            </TabPanel>
            <TabPanel value={value} index={1}>
                <TreeView
                    defaultCollapseIcon={<ExpandMoreIcon />}
                    defaultExpandIcon={<ChevronRightIcon />}
                    defaultExpanded={[1,2,3,4,5]}>
                    {Utils.renderTree(treeData)}
                </TreeView>
            </TabPanel>
        </div>
    );
}
