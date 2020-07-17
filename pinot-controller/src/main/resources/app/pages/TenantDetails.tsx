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

import React, { useState, useEffect } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { Grid } from '@material-ui/core';
import { RouteComponentProps } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import AppLoader from '../components/AppLoader';
import { getTenantTableDetails } from '../requests';
import EnhancedTableToolbar from '../components/EnhancedTableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';

const useStyles = makeStyles((theme) => ({
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 800 },
  },
  sqlDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default',
};

type Props = {
  tenantName: string;
  tableName: string;
};

const TenantPageDetails = ({ match }: RouteComponentProps<Props>) => {
  const classes = useStyles();
  const [fetching, setFetching] = useState(true);
  const [value, setValue] = useState('');

  useEffect(() => {
    getTenantTableDetails(match.params.tableName).then(
      ({ data }) => {
        setValue(JSON.stringify(data, null, 2));
        setFetching(false);
      }
    );
  }, []);
  return fetching ? (
    <AppLoader />
  ) : (
    <Grid
      item
      xs
      style={{
        padding: 20,
        backgroundColor: 'white',
        maxHeight: 'calc(100vh - 70px)',
        overflowY: 'auto',
      }}
    >
      <div className={classes.sqlDiv}>
        <EnhancedTableToolbar name={match.params.tableName} showSearchBox={false} />
        <CodeMirror
          options={jsonoptions}
          value={value}
          className={classes.queryOutput}
          autoCursor={false}
        />
      </div>
    </Grid>
  );
};

export default TenantPageDetails;
