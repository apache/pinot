import React from 'react'
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import NavBar from "./components/NavBar";
import Posts from "./components/Posts";
import Posts1 from "./components/Posts1";
import Posts2 from "./components/Posts2";
import Cluster from "./components/Cluster";
import Servers from "./components/Servers";

class App extends React.Component {

    static serverAddress = "http://localhost:9000";

    constructor(props) {
        super(props);
        this.processState = this.processState.bind(this);
    }

    state = {
        currentPost : 'cluster'
    };

    processState(value) {
        this.setState({currentPost:value})
    }

    render() {
        let displayPost;
        if(this.state.currentPost === 'cluster') {
            displayPost = <Cluster></Cluster>
        }
        if(this.state.currentPost === 'post') {
            displayPost = <Posts></Posts>
        }
        if(this.state.currentPost === 'servers') {
            displayPost = <Servers></Servers>
        }
        if(this.state.currentPost === 'post2') {
            displayPost = <Posts2></Posts2>
        }

        return (
            <div>
                <AppBar color="primary" position="static">
                    <Toolbar>
                        <NavBar mutateState={this.processState}/>
                    </Toolbar>
                </AppBar>
                {displayPost}
            </div>
        )
    }
}
export default App