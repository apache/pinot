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
import { get, find } from 'lodash';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { Grid, makeStyles, Box } from '@material-ui/core';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true
};

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  },
  body: {
    borderTop: '1px solid #BDCCD9',
    fontSize: '16px',
    lineHeight: '3rem',
    paddingLeft: '15px',
  },
  highlightBackground: {
    border: '1px #4285f4 solid',
    backgroundColor: 'rgba(66, 133, 244, 0.05)',
    borderRadius: 4,
    marginBottom: '20px',
  },
  sqlDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
  },
}));

const TaskDetail = (props) => {
  const classes = useStyles();
  const { subTaskID, taskID } = props.match.params;

  const [fetching, setFetching] = useState(true);
  const [taskDebugData, setTaskDebugData] = useState({});

  const fetchData = async () => {
    setFetching(true);
    const debugRes = await PinotMethodUtils.getTaskDebugData(taskID);
    const subTaskData = find(debugRes.data.subtaskInfos, (subTask) => get(subTask, 'taskId', '') === subTaskID);
    setTaskDebugData(subTaskData);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.highlightBackground}>
        <Grid container className={classes.body}>
          <Grid item xs={12}>
            <strong>Name:</strong> {subTaskID}
          </Grid>
          <Grid item xs={12}>
            <strong>Status:</strong> {get(taskDebugData, 'state', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Start Time:</strong> {get(taskDebugData, 'startTime', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Finish Time:</strong> {get(taskDebugData, 'finishTime', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Minion Host Name:</strong> {get(taskDebugData, 'participant', '')}
          </Grid>
        </Grid>
      </div>
      <Grid container spacing={2}>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Task Config"
              showSearchBox={false}
            >
              <CodeMirror
                options={jsonoptions}
                value={JSON.stringify(get(taskDebugData, `taskConfig`, {}), null, '  ')}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Info"
              showSearchBox={false}
            >
              <Box p={3}>
                {get(taskDebugData, `info`, '')}
              </Box>
            </SimpleAccordion>
          </div>
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TaskDetail;