/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, {Component} from "react";
import {Grid, Typography} from "@material-ui/core";
import Card from "@material-ui/core/Card";
import CardActionArea from "@material-ui/core/CardActionArea";
import CardActions from "@material-ui/core/CardActions";
import CardContent from "@material-ui/core/CardContent";
import CardMedia from "@material-ui/core/CardMedia";
import Button from "@material-ui/core/Button";
import {posts} from "./dummy-posts";

class Posts extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div style={{marginTop: 20, padding: 30}}>
                <Grid container spacing={40} justify="center">
                    {posts.map(post => (
                        <Grid item key={post.title}>
                            <Card>
                                <CardActionArea>
                                    <CardMedia
                                        component="img"
                                        alt="Contemplative Reptile"
                                        image={post.image}
                                        title="Contemplative Reptile"
                                    />
                                    <CardContent>
                                        <Typography gutterBottom variant="h5" component="h2">
                                            {post.title}
                                        </Typography>
                                        <Typography component="p">{post.excerpt}</Typography>
                                    </CardContent>
                                </CardActionArea>
                                <CardActions>
                                    <Button size="small" color="primary">
                                        Share
                                    </Button>
                                    <Button size="small" color="primary">
                                        Learn More
                                    </Button>
                                </CardActions>
                            </Card>
                        </Grid>
                    ))}
                </Grid>
            </div>
        );
    }
}

export default Posts;