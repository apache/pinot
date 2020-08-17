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
import TableToolbar from '../components/TableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomizedTables from '../components/Table';
import PinotMethodUtils from '../utils/PinotMethodUtils';

const useStyles = makeStyles((theme) => ({
  root: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  highlightBackground: {
    border: '1px #4285f4 solid',
    backgroundColor: 'rgba(66, 133, 244, 0.05)',
    borderRadius: 4,
    marginBottom: '20px',
  },
  body: {
    borderTop: '1px solid #BDCCD9',
    fontSize: '16px',
    lineHeight: '3rem',
    paddingLeft: '15px',
  },
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
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
  segmentName: string;
};

type Summary = {
  segmentName: string;
  totalDocs: string | number;
  createTime: unknown;
};

const SegmentDetails = ({ match }: RouteComponentProps<Props>) => {
  const classes = useStyles();
  const { tableName, segmentName } = match.params;

  const [fetching, setFetching] = useState(true);
  const [segmentSummary, setSegmentSummary] = useState<Summary>({
    segmentName,
    totalDocs: '',
    createTime: '',
  });

  const [replica, setReplica] = useState({
    columns: [],
    records: []
  });

  const [value, setValue] = useState('');
  const fetchData = async () => {
    const result = await PinotMethodUtils.getSegmentDetails(tableName, segmentName);
    setSegmentSummary(result.summary);
    setReplica(result.replicaSet);
    setValue(JSON.stringify(result.JSON, null, 2));
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
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
      <div className={classes.highlightBackground}>
        <TableToolbar name="Summary" showSearchBox={false} />
        <Grid container className={classes.body}>
          <Grid item xs={6}>
            <strong>Segment Name:</strong> {segmentSummary.segmentName}
          </Grid>
          <Grid item xs={3}>
            <strong>Total Docs:</strong> {segmentSummary.totalDocs}
          </Grid>
          <Grid item xs={3}>
            <strong>Create Time:</strong>  {segmentSummary.createTime}
          </Grid>
        </Grid>
      </div>

      <Grid container spacing={2}>
        <Grid item xs={6}>
          <CustomizedTables
            title="Replica Set"
            data={replica}
            isPagination={true}
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Metadata"
              showSearchBox={false}
            >
              <CodeMirror
                options={jsonoptions}
                value={value}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>
      </Grid>
    </Grid>
  );
};

export default SegmentDetails;
