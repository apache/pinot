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

  const fetchClusterName = async () => {
    const clusterNameResponse = await PinotMethodUtils.getClusterName();
    localStorage.setItem('pinot_ui:clusterName', clusterNameResponse);
    setClusterName(clusterNameResponse);
  };

  const fetchClusterConfig = async () => {
    const clusterConfig = await PinotMethodUtils.getClusterConfigJSON();
    app_state.queryConsoleOnlyView = clusterConfig?.queryConsoleOnlyView === 'true';
    setLoading(false);
  };

  React.useEffect(()=>{
    fetchClusterConfig();
    fetchClusterName();
  }, []);

  const getRouterData = () => {
    if(app_state.queryConsoleOnlyView){
      return RouterData.filter((routeObj)=>{return routeObj.path === '/query'});
    }
    return RouterData;
  };

  const classes = useStyles();
  return (
    <MuiThemeProvider theme={theme}>
      <NotificationContextProvider>
        <CustomNotification />
        {
          loading ?
            <CircularProgress className={classes.loader} size={80}/>
          :
          <Router>
            <Switch>
              {getRouterData().map(({ path, Component }, key) => (
                <Route
                  exact
                  path={path}
                  key={key}
                  render={props => {
                    return (
                      <div className="p-8">
                        <Layout clusterName={clusterName} {...props}>
                          <Component {...props} />
                        </Layout>
                      </div>
                    );
                  }}
                />
              ))}
              <Route path="*">
                <Redirect to={app_state.queryConsoleOnlyView ? "/query" : "/"} />
              </Route>
            </Switch>
          </Router>
        }
      </NotificationContextProvider>
    </MuiThemeProvider>
  );
};

ReactDOM.render(<App />, document.getElementById('app'));
