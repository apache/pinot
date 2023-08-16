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

import React, { useEffect, useState } from 'react';
import { Switch, Route, Redirect, useHistory } from 'react-router-dom';
import Layout from './components/Layout';
import RouterData from './router';
import PinotMethodUtils from './utils/PinotMethodUtils';
import app_state from './app_state';
import { useAuthProvider } from './components/auth/AuthProvider';
import { AppLoadingIndicator } from './components/AppLoadingIndicator';
import { AuthWorkflow } from 'Models';

export const App = () => {
  const [clusterName, setClusterName] = useState('');
  const [loading, setLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(null);
  const [role, setRole] = useState('');
  const { authUserName, authUserEmail, authenticated, authWorkflow } = useAuthProvider();
  const history = useHistory();

  useEffect(() => {
    // authentication already handled by authProvider
    if (authUserEmail && authenticated) {
      // Authenticated with an auth method that supports user identity
      // Any code that needs user identity can go here
    }

    if (authenticated) {
      setIsAuthenticated(true);
    }
  }, [authUserName, authUserEmail, authenticated]);

  useEffect(() => {
    if(authWorkflow === AuthWorkflow.BASIC) {
      setLoading(false);
    }
  }, [authWorkflow])

  const fetchUserRole = async () => {
    const userListResponse = await PinotMethodUtils.getUserList();
    let userObj = userListResponse.users;
    let userData = [];
    for (let key in userObj) {
      if (userObj.hasOwnProperty(key)) {
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
    if (app_state.queryConsoleOnlyView) {
      return RouterData.filter((routeObj) => { return routeObj.path === '/query' });
    }
    if (app_state.hideQueryConsoleTab) {
      return RouterData.filter((routeObj) => routeObj.path !== '/query');
    }
    return RouterData;
  };

  useEffect(() => {
    if (isAuthenticated) {
      fetchClusterConfig();
      fetchClusterName();
      fetchUserRole();
    }
  }, [isAuthenticated]);

  const loginRender = (Component, props) => {
    if(isAuthenticated) {
      history.push("/");
      return;
    }

    return (
      <div className="p-8">
        <Component {...props} setIsAuthenticated={setIsAuthenticated} />
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

  if (loading) {
    return <AppLoadingIndicator />;
  }

  return (
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
  );
};
