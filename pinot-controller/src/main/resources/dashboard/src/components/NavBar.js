import React, {Component} from 'react';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import TypoGraphy from '@material-ui/core/Typography'


class NavBar extends Component {

    constructor(props) {
        super(props);
        this.dsiplayPage = this.dsiplayPage.bind(this);
        this.displayServers = this.displayServers.bind(this);
    }


    dsiplayPage() {
        this.props.mutateState('post1');
    };

    displayServers() {
        this.props.mutateState('servers');
    };

    render() {
        return (
            <List component="nav">
                <ListItem component="div" >
                    <ListItemText inset >
                        <TypoGraphy color="inherit" variant="title" >
                            Cluster
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={this.dsiplayPage}>
                        <TypoGraphy color="inherit" variant="title">
                            Tenants
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset>
                        <TypoGraphy color="inherit" variant="title">
                            Tables
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset>
                        <TypoGraphy color="inherit" variant="title">
                            Controllers
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset onClick={this.displayServers}>
                        <TypoGraphy color="inherit" variant="title">
                            Servers
                        </TypoGraphy>
                    </ListItemText>

                    <ListItemText inset>
                        <TypoGraphy color="inherit" variant="title">
                            Brokers
                        </TypoGraphy>
                    </ListItemText>
                </ListItem>

            </List>)
    }
}


export default NavBar;