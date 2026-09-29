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
import { Grid, makeStyles, Typography, Box, Paper } from '@material-ui/core';
import { get } from 'lodash';
import useTaskTypesTable from '../components/Homepage/useTaskTypesTable';
import PinotMethodUtils from '../utils/PinotMethodUtils';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
  summaryPaper: {
    padding: '12px 0',
    textAlign: 'center',
    color: '#4285f4',
    backgroundColor: 'rgba(66, 133, 244, 0.08)',
    border: '1px solid rgba(66, 133, 244, 0.4)',
    borderRadius: 4,
    '& h2, h4': {
      margin: 0,
    },
    '& h4': {
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 600,
      fontSize: 12,
    },
    '& h2': {
      fontSize: 28,
    },
  },
}));

// Subset of /tasks/summary fields used by the dashboard tiles.
type TasksSummary = {
  totalRunningTasks?: number;
  totalWaitingTasks?: number;
};

const MinionTaskManager = () => {
  const classes = useStyles();
  const { taskTypes, taskTypesTable } = useTaskTypesTable();

  const [minionCount, setMinionCount] = useState<number | null>(null);
  const [tasksSummary, setTasksSummary] = useState<TasksSummary | null>(null);

  useEffect(() => {
    let isMounted = true;
    Promise.allSettled([
      PinotMethodUtils.getAllInstances(),
      PinotMethodUtils.getTasksSummaryData()
    ]).then(([instancesRes, summaryRes]) => {
      if (!isMounted) {
        return;
      }
      if (instancesRes.status === 'fulfilled') {
        setMinionCount(get(instancesRes.value, 'MINION', []).length);
      }
      if (summaryRes.status === 'fulfilled') {
        setTasksSummary(summaryRes.value as TasksSummary);
      }
    });
    return () => { isMounted = false; };
  }, []);

  const taskTypeCount = taskTypes?.records?.length || 0;
  const runningCount = tasksSummary?.totalRunningTasks ?? '-';
  const waitingCount = tasksSummary?.totalWaitingTasks ?? '-';

  return (
    <Grid item xs className={classes.gridContainer}>
      <Box mb={2}>
        <Typography variant='h5'>Minion Task Manager</Typography>
        <Typography variant='caption'>Manage minion task queues, drill into tasks and sub-tasks, and view per-minion log files</Typography>
      </Box>

      <Grid container spacing={2}>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Task Types</h4>
            <h2>{taskTypeCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Minion Instances</h4>
            <h2>{minionCount === null ? '-' : minionCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Running Tasks</h4>
            <h2>{runningCount}</h2>
          </Paper>
        </Grid>
        <Grid item xs={3}>
          <Paper className={classes.summaryPaper} elevation={0}>
            <h4>Waiting Tasks</h4>
            <h2>{waitingCount}</h2>
          </Paper>
        </Grid>
      </Grid>

      <Box mt={3} />

      {taskTypesTable}
    </Grid>
  );
};

export default MinionTaskManager;
