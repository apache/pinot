import React, { Component } from 'react'
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import TypoGraphy from '@material-ui/core/Typography';
import NavBar from "./components/NavBar";
import Posts from "./components/Posts";
import Posts1 from "./components/Posts1";
import Posts2 from "./components/Posts2";
import { posts } from "./components/dummy-posts";
import Cluster from "./components/Cluster";
class App extends React.Component {

    constructor(props) {
        super(props);
        this.processState = this.processState.bind(this);
    }

    state = {
        currentPost : 'cluster'
    };

    processState(value) {
        alert(value);
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
        if(this.state.currentPost === 'post1') {
            displayPost = <Posts1></Posts1>
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