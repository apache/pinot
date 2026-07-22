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

import React, { useEffect, useRef, useState } from 'react';
import { Switch, Route, Redirect, useHistory } from 'react-router-dom';
import Layout from './components/Layout';
import RouterData from './router';
import PinotMethodUtils from './utils/PinotMethodUtils';
import app_state from './app_state';
import { useAuthProvider } from './components/auth/AuthProvider';
import { AppLoadingIndicator } from './components/AppLoadingIndicator';
import { AuthWorkflow } from 'Models';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
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
  const { authUserName, authUserEmail, authenticated, authWorkflow, inactivityTimeoutSeconds } = useAuthProvider();
  const history = useHistory();

  useEffect(() => {
    // authentication already handled by authProvider
    if (authUserEmail && authenticated) {
      // Authenticated with an auth method that supports user identity
    }

    if (authenticated) {
      setIsAuthenticated(true);
    }
  }, [authUserName, authUserEmail, authenticated]);

  useEffect(() => {
    // Both BASIC and SESSION workflows are handled by the login page.
    if (authWorkflow === AuthWorkflow.BASIC || authWorkflow === AuthWorkflow.SESSION) {
      setLoading(false);
    }
  }, [authWorkflow]);

  // ─── SESSION INACTIVITY TIMEOUT ───────────────────────────────────────────
  // Timeout duration comes from /auth/info (controller.ui.session.inactivity.timeout.seconds).
  // Falls back to 300s (5 min) if not set.
  //
  // Timeline:
  //   t=0           user active
  //   t=timeout-60s show warning dialog with 60s countdown
  //   t=timeout     call /auth/logout (server invalidates session), redirect to /login
  //
  // Any user interaction (mouse/key/click/scroll) resets the full timer.

  const [showTimeoutWarning, setShowTimeoutWarning] = useState<boolean>(false);
  const [warningCountdown, setWarningCountdown] = useState<number>(60);
  const inactivityTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const warningTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const performSessionLogout = async () => {
    setShowTimeoutWarning(false);
    // Clear all timers so no further callbacks fire after logout
    if (inactivityTimerRef.current) clearTimeout(inactivityTimerRef.current);
    if (warningTimerRef.current) clearTimeout(warningTimerRef.current);
    if (countdownIntervalRef.current) clearInterval(countdownIntervalRef.current);
    try {
      await fetch('/auth/logout', { method: 'GET', credentials: 'include' });
    } catch (e) {
      // Even on network error, clear local state and redirect
    }
    app_state.authToken = null;
    app_state.authWorkflow = null;
    app_state.username = null;
    setIsAuthenticated(false);
    window.location.href = '/#/login';
  };

  const resetInactivityTimer = () => {
    if (inactivityTimerRef.current) clearTimeout(inactivityTimerRef.current);
    if (warningTimerRef.current) clearTimeout(warningTimerRef.current);
    if (countdownIntervalRef.current) clearInterval(countdownIntervalRef.current);
    setShowTimeoutWarning(false);

    const timeoutMs = (inactivityTimeoutSeconds > 0 ? inactivityTimeoutSeconds : 300) * 1000;
    const warningMs = Math.max(0, timeoutMs - 60000);

    warningTimerRef.current = setTimeout(() => {
      setWarningCountdown(60);
      setShowTimeoutWarning(true);
      countdownIntervalRef.current = setInterval(() => {
        setWarningCountdown((c) => c - 1);
      }, 1000);
    }, warningMs);

    inactivityTimerRef.current = setTimeout(performSessionLogout, timeoutMs);
  };

  useEffect(() => {
    if (authWorkflow !== AuthWorkflow.SESSION || !isAuthenticated) return;

    const events = ['mousedown', 'keydown', 'click', 'scroll', 'touchstart'];
    events.forEach((e) => window.addEventListener(e, resetInactivityTimer));
    resetInactivityTimer();

    return () => {
      events.forEach((e) => window.removeEventListener(e, resetInactivityTimer));
      if (inactivityTimerRef.current) clearTimeout(inactivityTimerRef.current);
      if (warningTimerRef.current) clearTimeout(warningTimerRef.current);
      if (countdownIntervalRef.current) clearInterval(countdownIntervalRef.current);
    };
  }, [authWorkflow, isAuthenticated, inactivityTimeoutSeconds]);

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
      {/* SESSION inactivity warning dialog */}
      <Dialog open={showTimeoutWarning} disableBackdropClick disableEscapeKeyDown>
        <DialogTitle>Session Expiring Soon</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Your session will expire in {warningCountdown} second{warningCountdown !== 1 ? 's' : ''}.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={async () => {
              // Renew the server-side session (rotates token, resets TTL on both server and cookie)
              const res = await fetch('/auth/session/renew', { method: 'POST', credentials: 'include' });
              if (res.ok) {
                resetInactivityTimer();
              } else {
                // Session expired before the user responded — redirect to login
                performSessionLogout();
              }
            }}
            color="primary"
          >
            Stay Logged In
          </Button>
          <Button onClick={performSessionLogout} color="secondary">
            Logout Now
          </Button>
        </DialogActions>
      </Dialog>
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
