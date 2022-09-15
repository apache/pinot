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

import * as React from 'react';
import * as ReactDOM from 'react-dom';
import { CircularProgress, createStyles, makeStyles, MuiThemeProvider } from '@material-ui/core';
import { Switch, Route, HashRouter as Router, Redirect } from 'react-router-dom';
import theme from './theme';
import Layout from './components/Layout';
import RouterData from './router';
import PinotMethodUtils from './utils/PinotMethodUtils';
import CustomNotification from './components/CustomNotification';
import { NotificationContextProvider } from './components/Notification/NotificationContextProvider';
import app_state from './app_state';
import { AuthWorkflow } from 'Models';

const useStyles = makeStyles(() =>
  createStyles({
    loader: {
      position: 'fixed',
      left: '50%',
      top: '30%'
    },
  })
);

const App = () => {
  const [clusterName, setClusterName] = React.useState('');
  const [loading, setLoading] = React.useState(true);
  const oidcSignInFormRef = React.useRef<HTMLFormElement>(null);
  const [isAuthenticated, setIsAuthenticated] = React.useState(null);
  const [issuer, setIssuer] = React.useState(null);
  const [redirectUri, setRedirectUri] = React.useState(null);
  const [clientId, setClientId] = React.useState(null);
  const [authWorkflow, setAuthWorkflow] = React.useState(null);
  const [authorizationEndpoint, setAuthorizationEndpoint] = React.useState(
    null
  );
  const [role, setRole] = React.useState('');

  const fetchUserRole = async()=>{
    const userListResponse = await PinotMethodUtils.getUserList();
    let userObj = userListResponse.users;
    let userData = [];
    for (let key in userObj) {
      if(userObj.hasOwnProperty(key)){
        userData.push(userObj[key]);
      }
    }
    let role = "";

    for (let item of userData) {
      if (item.username === app_state.username && item.component === 'CONTROLLER') {
        role = item.role;
      }
    }
    if (role === 'ADMIN') {
      app_state.role = "ADMIN";
      setRole('ADMIN')
    }
  }

  const fetchClusterName = async () => {
    const clusterNameResponse = await PinotMethodUtils.getClusterName();
    localStorage.setItem('pinot_ui:clusterName', clusterNameResponse);
    setClusterName(clusterNameResponse);
  };

  const fetchClusterConfig = async () => {
    const clusterConfig = await PinotMethodUtils.getClusterConfigJSON();
    app_state.queryConsoleOnlyView = clusterConfig?.queryConsoleOnlyView === 'true';
    app_state.hideQueryConsoleTab = clusterConfig?.hideQueryConsoleTab === 'true';

    setLoading(false);
  };

  const getRouterData = () => {
    if(app_state.queryConsoleOnlyView){
      return RouterData.filter((routeObj)=>{return routeObj.path === '/query'});
    }
    if (app_state.hideQueryConsoleTab) {
      return RouterData.filter((routeObj) => routeObj.path !== '/query');
    }
    return RouterData;
  };

  const getAuthInfo = async () => {
    const authInfoResponse = await PinotMethodUtils.getAuthInfo();
    // Issuer URL, if available
    setIssuer(
      authInfoResponse && authInfoResponse.issuer ? authInfoResponse.issuer : ''
    );
    // Redirect URI, if available
    setRedirectUri(
      authInfoResponse && authInfoResponse.redirectUri
        ? authInfoResponse.redirectUri
        : ''
    );
    // Client Id, if available
    setClientId(
      authInfoResponse && authInfoResponse.clientId
        ? authInfoResponse.clientId
        : ''
    );
    // Authentication workflow
    setAuthWorkflow(
      authInfoResponse && authInfoResponse.workflow
        ? authInfoResponse.workflow
        : AuthWorkflow.NONE
    );
  };

  const initAuthWorkflow = async () => {
    switch (authWorkflow) {
      case AuthWorkflow.NONE: {
        // No authentication required
        setIsAuthenticated(true);

        break;
      }
      case AuthWorkflow.BASIC: {
        // Basic authentication, handled by login page
        setLoading(false);

        break;
      }
      case AuthWorkflow.OIDC: {
        // OIDC authentication, check to see if access token is available in the URL
        const accessToken = PinotMethodUtils.getAccessTokenFromHashParams();
        if (accessToken) {
          app_state.authWorkflow = AuthWorkflow.OIDC;
          app_state.authToken = accessToken;

          setIsAuthenticated(true);
        } else {
          // Set authorization endpoint
          setAuthorizationEndpoint(`${issuer}/auth`);

          setLoading(false);
        }

        break;
      }
      default: {
        // Empty
      }
    }
  };

  React.useEffect(() => {
    getAuthInfo();
  }, []);

  React.useEffect(() => {
    initAuthWorkflow();
  }, [authWorkflow]);

  React.useEffect(() => {
    if (authorizationEndpoint && oidcSignInFormRef && oidcSignInFormRef.current) {
      // Authorization endpoint available; submit sign in form
      oidcSignInFormRef.current.submit();
    }
  }, [authorizationEndpoint]);

  React.useEffect(() => {
    if (isAuthenticated) {
      fetchClusterConfig();
      fetchClusterName();
      fetchUserRole();
    }
  }, [isAuthenticated]);

  const loginRender = (Component, props) => {
    return (
      <div className="p-8">
        <Component {...props} setIsAuthenticated={setIsAuthenticated}/>
      </div>
    )
  };

  const componentRender = (Component, props, role) => {
    return (
      <div className="p-8">
        <Layout clusterName={clusterName} {...props} role={role}>
          <Component {...props} />
        </Layout>
      </div>
    )
  };

  const classes = useStyles();

  return (
    <MuiThemeProvider theme={theme}>
      <NotificationContextProvider>
        <CustomNotification />
        {/* OIDC auth workflow */}
        {authWorkflow && authWorkflow === AuthWorkflow.OIDC && !isAuthenticated ? (
          <>
            {/* OIDC sign in form */}
            <form
              hidden
              action={authorizationEndpoint}
              method="post"
              ref={oidcSignInFormRef}
            >
              <input readOnly name="client_id" value={clientId} />
              <input readOnly name="redirect_uri" value={redirectUri} />
              <input readOnly name="scope" value="email openid" />
              <input readOnly name="state" value="true-redirect-uri" />
              <input readOnly name="response_type" value="id_token token" />
              <input readOnly name="nonce" value="random_string" />
              <input type="submit" value="" />
            </form>
          </>
        ) : (
          <>
            {/* Non-OIDC/authenticated workflow */}
            {loading ? (
              <CircularProgress className={classes.loader} size={80} />
            ) : (
              <Router>
                <Switch>
                  {getRouterData().map(({ path, Component }, key) => (
                    <Route
                      exact
                      path={path}
                      key={key}
                      render={(props) => {
                        if (path === '/login') {
                          return loginRender(Component, props);
                        } else if (isAuthenticated) {
                          // default render
                          return componentRender(Component, props, role);
                        } else {
                          return <Redirect to="/login" />;
                        }
                      }}
                    />
                  ))}
                  <Route path="*">
                    <Redirect
                      to={PinotMethodUtils.getURLWithoutAccessToken(
                        app_state.queryConsoleOnlyView ? '/query' : '/'
                      )}
                    />
                  </Route>
                </Switch>
              </Router>
            )}
          </>
        )}
      </NotificationContextProvider>
    </MuiThemeProvider>
  );
};

ReactDOM.render(<App />, document.getElementById('app'));
