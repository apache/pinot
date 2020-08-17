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
import { MuiThemeProvider } from '@material-ui/core';
import { Switch, Route, HashRouter as Router } from 'react-router-dom';
import theme from './theme';
import Layout from './components/Layout';
import RouterData from './router';
import PinotMethodUtils from './utils/PinotMethodUtils';

const App = () => {
  const fetchClusterName = async () => {
    const clusterNameResponse = await PinotMethodUtils.getClusterName();
    localStorage.setItem('pinot_ui:clusterName', clusterNameResponse);
  };
  React.useEffect(()=>{
    fetchClusterName();
  }, []);
  return (
    <MuiThemeProvider theme={theme}>
      <Router>
        <Switch>
          {RouterData.map(({ path, Component }, key) => (
            <Route
              exact
              path={path}
              key={key}
              render={props => {
                return (
                  <div className="p-8">
                    <Layout {...props}>
                      <Component {...props} />
                    </Layout>
                  </div>
                );
              }}
            />
          ))}
        </Switch>
      </Router>
    </MuiThemeProvider>
  )
};

ReactDOM.render(<App />, document.getElementById('app'));
