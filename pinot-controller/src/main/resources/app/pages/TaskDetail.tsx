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
import { get, each } from 'lodash';
import { Grid, makeStyles } from '@material-ui/core';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from '../components/Table';
import { TaskRuntimeConfig } from 'Models';
import AppLoader from '../components/AppLoader';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomCodemirror from '../components/CustomCodemirror';

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
  runtimeConfigContainer: {
    '& .CodeMirror': { fontSize: "1rem", height: "100%" },
    maxHeight: 300
  }
}));

const TaskDetail = (props) => {
  const classes = useStyles();
  const { taskID, taskType, queueTableName } = props.match.params;

  const [fetching, setFetching] = useState(true);
  const [taskDebugData, setTaskDebugData] = useState({});
  const [subtaskTableData, setSubtaskTableData] = useState({ columns: ['Task ID', 'Status', 'Start Time', 'Finish Time', 'Minion Host Name'], records: [] });
  const [taskRuntimeConfig, setTaskRuntimeConfig] = useState<TaskRuntimeConfig | null>(null);

  const fetchData = async () => {
    setFetching(true);
    const [debugRes, runtimeConfig] = await Promise.all([
      PinotMethodUtils.getTaskDebugData(taskID), 
      PinotMethodUtils.getTaskRuntimeConfigData(taskID)
    ]);
    const subtaskTableRecords = [];
    each(get(debugRes, 'data.subtaskInfos', {}), (subTask) => {
      subtaskTableRecords.push([
        get(subTask, 'taskId'),
        get(subTask, 'state'),
        get(subTask, 'startTime'),
        get(subTask, 'finishTime'),
        get(subTask, 'participant'),
      ])
    });
    setSubtaskTableData(prevState => {
      return { ...prevState, records: subtaskTableRecords };
    });
    setTaskDebugData(debugRes.data);
    setTaskRuntimeConfig(runtimeConfig)

    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  if(fetching) {
    return <AppLoader />
  }

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.highlightBackground}>
        <Grid container className={classes.body}>
          <Grid item xs={12}>
            <strong>Name:</strong> {taskID}
          </Grid>
          <Grid item xs={12}>
            <strong>Status:</strong> {get(taskDebugData, 'taskState', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Start Time:</strong> {get(taskDebugData, 'startTime', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Finish Time:</strong> {get(taskDebugData, 'finishTime', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Triggered By:</strong> {get(taskDebugData, 'triggeredBy', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Number of Sub Tasks:</strong> {get(taskDebugData, 'subtaskCount.total', '')}
          </Grid>
        </Grid>
      </div>
      <Grid container spacing={2}>
        {/* Runtime config - JSON */}
        <Grid item xs={12}>
          <SimpleAccordion
            headerTitle="Runtime Config"
            showSearchBox={false}
            detailsContainerClass={classes.runtimeConfigContainer}
          >
            <CustomCodemirror
              data={taskRuntimeConfig}
            />
          </SimpleAccordion>
        </Grid>
      
        {/* Sub task table */}
        <Grid item xs={12}>
          <CustomizedTables
            title="Sub Tasks"
            data={subtaskTableData}
            showSearchBox={true}
            inAccordionFormat={true}
            addLinks
            baseURL={`/task-queue/${taskType}/tables/${queueTableName}/task/${taskID}/sub-task/`}
          />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TaskDetail;