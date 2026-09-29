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
import { TimezoneProvider } from './contexts/TimezoneContext';
import { runBootstrapRequest } from './utils/bootstrap';

const INITIAL_BOOTSTRAP_TIMEOUT_MS = 3000;

export const App = () => {
  const [clusterName, setClusterName] = useState('');
  const [queryConsoleOnlyView, setQueryConsoleOnlyView] = useState(app_state.queryConsoleOnlyView);
  const [hideQueryConsoleTab, setHideQueryConsoleTab] = useState(app_state.hideQueryConsoleTab);
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

  const applyClusterConfig = (clusterConfig?: Record<string, string>) => {
    const nextQueryConsoleOnlyView = clusterConfig?.queryConsoleOnlyView === 'true';
    const nextHideQueryConsoleTab = clusterConfig?.hideQueryConsoleTab === 'true';

    app_state.queryConsoleOnlyView = nextQueryConsoleOnlyView;
    app_state.hideQueryConsoleTab = nextHideQueryConsoleTab;
    setQueryConsoleOnlyView(nextQueryConsoleOnlyView);
    setHideQueryConsoleTab(nextHideQueryConsoleTab);
  };

  const fetchUserRole = () => {
    if (!app_state.username) {
      return () => undefined;
    }

    return runBootstrapRequest<{ users: Record<string, any> }>({
      request: PinotMethodUtils.getUserList() as Promise<{ users: Record<string, any> }>,
      onSuccess: (userListResponse) => {
        const userObj = userListResponse.users;
        const userData = [];
        for (const key in userObj) {
          if (userObj.hasOwnProperty(key)) {
            userData.push(userObj[key]);
          }
        }

        let nextRole = '';

        for (const item of userData) {
          if (item.username === app_state.username && item.component === 'CONTROLLER') {
            nextRole = item.role;
          }
        }

        if (nextRole === 'ADMIN') {
          app_state.role = 'ADMIN';
          setRole('ADMIN');
        }
      },
      onError: (error) => {
        console.warn('Unable to load user role during initial app bootstrap.', error);
      },
    });
  };

  const fetchClusterName = () => {
    return runBootstrapRequest<string>({
      request: PinotMethodUtils.getClusterName(),
      onSuccess: (clusterNameResponse) => {
        localStorage.setItem('pinot_ui:clusterName', clusterNameResponse);
        setClusterName(clusterNameResponse);
      },
      onError: (error) => {
        console.warn('Unable to load cluster name during initial app bootstrap.', error);
      },
    });
  };

  const fetchClusterConfig = () => {
    return runBootstrapRequest<Record<string, string>>({
      request: PinotMethodUtils.getClusterConfigJSON(),
      timeoutMs: INITIAL_BOOTSTRAP_TIMEOUT_MS,
      onInitialTimeout: () => {
        applyClusterConfig();
        setLoading(false);
      },
      onSuccess: (clusterConfig) => {
        applyClusterConfig(clusterConfig);
        setLoading(false);
      },
      onError: (error) => {
        console.warn('Unable to load cluster config during initial app bootstrap.', error);
        applyClusterConfig();
        setLoading(false);
      },
    });
  };

  const getRouterData = () => {
    if (queryConsoleOnlyView) {
      return RouterData.filter((routeObj) => {
        return routeObj.path === '/query' || routeObj.path === '/query/timeseries';
      });
    }
    if (hideQueryConsoleTab) {
      return RouterData.filter((routeObj) => routeObj.path !== '/query' && routeObj.path !== '/query/timeseries');
    }
    return RouterData;
  };

  useEffect(() => {
    if (isAuthenticated) {
      const cancelClusterConfig = fetchClusterConfig();
      const cancelClusterName = fetchClusterName();
      const cancelUserRole = fetchUserRole();

      return () => {
        cancelClusterConfig();
        cancelClusterName();
        cancelUserRole();
      };
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
        <Layout clusterName={clusterName} {...props} role={role} authWorkflow={authWorkflow}>
          <Component {...props} />
        </Layout>
      </div>
    )
  };

  if (loading) {
    return <AppLoadingIndicator />;
  }

  return (
    <TimezoneProvider>
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
              queryConsoleOnlyView ? '/query' : '/'
            )}
          />
        </Route>
      </Switch>
    </TimezoneProvider>
  );
};
