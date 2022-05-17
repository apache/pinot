/**
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

import React from 'react';
import { Box, Button, createStyles, makeStyles, TextField, Theme } from '@material-ui/core';
import Logo from '../components/SvgIcons/Logo';
import { useForm } from 'react-hook-form';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import app_state from '../app_state';
import { AuthWorkflow } from 'Models';

interface FormData {
  username: string;
  password: string;
}

const useStyles = makeStyles((theme: Theme) => {
  return createStyles({
    container: {
      height: "100vh",
      width: "100vw",
      backgroundColor: "#ebebeb",
      boxSizing: "border-box",
      paddingTop: "5%",
      margin: -8,
    },
    loginForm: {
      width: 420,
      margin: "0 auto",
      zIndex: 99,
      display: "block",
      background: "#fff",
      borderRadius: ".25rem",
      textAlign: "center",
      boxShadow: "0 0 20px rgba(0, 0, 0, 0.2)",
      color: theme.palette.common.white,
    },
    logoContainer: {
      textAlign: "center",
      opacity: 0.7,
      padding: "30px",
      backgroundColor: theme.palette.primary.main,
      color: theme.palette.common.white,
      borderRadius: "0.25rem 0.25rem 0 0",
    },
    marginTop1: {
      marginTop: theme.spacing(1),
    },
    paddingBottom3: {
      paddingBottom: theme.spacing(3),
    },
  });
});

const LoginPage = (props) => {
  const { handleSubmit, register } = useForm<FormData>();
  const [invalidToken, setInvalidToken] = React.useState(null);

  const onSubmit = handleSubmit(async (data) => {
    const authToken = "Basic "+btoa(data.username+":"+data.password);
    const isUserAuthenticated = await PinotMethodUtils.verifyAuth(authToken);
    if(isUserAuthenticated){
      setInvalidToken(false);
      props.setIsAuthenticated(true);
      app_state.authWorkflow = AuthWorkflow.BASIC;
      app_state.authToken = authToken;
      props.history.push(app_state.queryConsoleOnlyView ? '/query' : '/');
      app_state.username = data.username;
    } else {
      setInvalidToken(true);
    }
  });

  const classes = useStyles();
  return (
    <div className={classes.container}>
      <div className={classes.loginForm}>
        <div className={classes.logoContainer}>
          <Logo fulllogo="true" />
        </div>
        <form onSubmit={onSubmit}>
          <TextField
            inputRef={register}
            className={classes.marginTop1}
            label="Username"
            name="username"
            required
          />
          <TextField
            inputRef={register}
            className={classes.marginTop1}
            name="password"
            label="Password"
            type="password"
            required
          />
          {invalidToken &&
            <Box mt={3}>
              <span style={{color: "red"}}>Invalid Username/Password</span>
            </Box>
          }
          <Box paddingY={3}>
            <Button type="submit" variant="contained" color="primary">
              Login
            </Button>
          </Box>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;