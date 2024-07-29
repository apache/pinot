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
import { get, map } from 'lodash';
import { TableData } from 'Models';
import { Grid, makeStyles } from '@material-ui/core';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { useConfirm } from '../components/Confirm';
import CustomizedTables from '../components/Table';
import { cleanupTasks } from '../requests';

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
}));

const TaskQueue = (props) => {
  const classes = useStyles();
  const { taskType } = props.match.params;

  const [fetching, setFetching] = useState(true);
  const [taskInfo, setTaskInfo] = useState([]);
  const [tables, setTables] = useState<TableData>({ records: [], columns: ['Name'] });

  const fetchData = async () => {
    setFetching(true);
    const taskInfoRes = await PinotMethodUtils.getTaskInfo(taskType);
    const tablesResponse:any = await PinotMethodUtils.getTableData({ taskType });
    setTaskInfo(taskInfoRes);
    setTables((prevState): TableData => {
      const _records = map(get(tablesResponse, 'tables', []), table => [table]);
      return { ...prevState, records: _records };
    });
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleStopAll = async () => {
    await PinotMethodUtils.stopAllTasks(taskType);
    await fetchData();
    stopAllConfirm.setConfirmDialog(false);
  };

  const stopAllConfirm = useConfirm({
    dialogTitle: 'Stop all tasks',
    dialogContent: 'Are you sure want to stop all the tasks?',
    successCallback: handleStopAll
  });

  const handleResumeAll = async () => {
    await PinotMethodUtils.resumeAllTasks(taskType);
    await fetchData();
    resumeAllConfirm.setConfirmDialog(false);
  };

  const resumeAllConfirm = useConfirm({
    dialogTitle: 'Resume all tasks',
    dialogContent: 'Are you sure want to resume all the tasks?',
    successCallback: handleResumeAll
  });

  const handleCleanupAll = async () => {
    await PinotMethodUtils.cleanupAllTasks(taskType);
    cleanupAllConfirm.setConfirmDialog(false);
  };

  const cleanupAllConfirm = useConfirm({
    dialogTitle: 'Cleanup all tasks',
    dialogContent: 'Are you sure want to cleanup all the tasks?',
    successCallback: handleCleanupAll
  });

  const handleDeleteAll = async () => {
    await PinotMethodUtils.deleteAllTasks(taskType);
    deleteAllConfirm.setConfirmDialog(false);
  };

  const deleteAllConfirm = useConfirm({
    dialogTitle: 'Delete all tasks',
    dialogContent: 'Are you sure want to delete all the tasks?',
    successCallback: handleDeleteAll
  });

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
            <CustomButton
              onClick={() => stopAllConfirm.setConfirmDialog(true)}
              tooltipTitle="Stop all running/pending tasks (as well as the task queue) for the given task type. Any new task added will not be picked up until Task Queue is resumed. This is not an instant operation, and the task may take some more time to actually stop."
              enableTooltip={true}
            >
              Stop All
            </CustomButton>
            <CustomButton
              onClick={() => resumeAllConfirm.setConfirmDialog(true)}
              tooltipTitle="Resume all stopped tasks (as well as the task queue) for the given task type. Resumed tasks start from the beginning of the task."
              enableTooltip={true}
            >
              Resume All
            </CustomButton>
            <CustomButton
              onClick={() => cleanupAllConfirm.setConfirmDialog(true)}
              tooltipTitle="Clean up finished tasks (COMPLETED, FAILED) for the given task type. This has no impact on the executing tasks. This happens periodically "
              enableTooltip={true}
            >
              Cleanup All
            </CustomButton>
            <CustomButton
              onClick={() => deleteAllConfirm.setConfirmDialog(true)}
              tooltipTitle="Delete all tasks (as well as the task queue) for the given task type."
              enableTooltip={true}
            >
              Delete All
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>
      <div className={classes.highlightBackground}>
        {/* <TableToolbar name="Summary" showSearchBox={false} /> */}
        <Grid container className={classes.body}>
          <Grid item xs={4}>
            <strong>Task Type:</strong> {taskType}
          </Grid>
          <Grid item xs={4}>
            <strong>Task Queue Status:</strong> {get(taskInfo, '1', null)}
          </Grid>
          <Grid item xs={4}>
            <strong>Num Tasks: </strong>
            {get(taskInfo, '0', null)}
          </Grid>
        </Grid>
      </div>
      {!fetching && (
        <CustomizedTables
          title="Tables"
          data={tables}
          showSearchBox={true}
          inAccordionFormat={true}
          addLinks
          baseURL={`/task-queue/${taskType}/tables/`}
        />
      )}
      {stopAllConfirm.confirmComponent}
      {resumeAllConfirm.confirmComponent}
      {cleanupAllConfirm.confirmComponent}
      {deleteAllConfirm.confirmComponent}
    </Grid>
  );
};

export default TaskQueue;