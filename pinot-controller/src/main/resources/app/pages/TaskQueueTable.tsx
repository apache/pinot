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
import { get } from 'lodash';
import { TableData } from 'Models';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { Grid, makeStyles } from '@material-ui/core';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
// import TableToolbar from '../components/TableToolbar';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { useConfirm } from '../components/Confirm';
import CustomizedTables from '../components/Table';

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

const TaskQueueTable = (props) => {
  const classes = useStyles();
  const { taskType, tableName } = props.match.params;

  const [fetching, setFetching] = useState(true);
  // const [taskInfo, setTaskInfo] = useState([]);
  const [jobDetail, setJobDetail] = useState({});
  const [tableDetail, setTableDetail] = useState({});
  // const [tables, setTables] = useState<TableData>({ records: [], columns: ['Name'] });

  const fetchData = async () => {
    setFetching(true);
    const detail = await PinotMethodUtils.getScheduleJobDetail(tableName, taskType);
    const tableDetailRes = await PinotMethodUtils.getTableDetails(tableName);
    console.log('tableDetailRes', tableDetailRes);
    setTableDetail(tableDetailRes);
    setJobDetail(detail);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleScheduleNow = async () => {
    await PinotMethodUtils.scheduleTaskAction(tableName, taskType);
  };

  const handleExecute = async () => {
    await PinotMethodUtils.executeTaskAction({ tableName, taskType });
  };

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
            <CustomButton
              onClick={() => handleScheduleNow()}
              tooltipTitle="Schedule now"
              enableTooltip={true}
            >
              Schedule Now
            </CustomButton>
            <CustomButton
              onClick={() => handleExecute()}
              tooltipTitle="Execute ADHOC"
              enableTooltip={true}
            >
              Schedule ADHOC
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>
      <div className={classes.highlightBackground}>
        {/* <TableToolbar name="Summary" showSearchBox={false} /> */}
        <Grid container className={classes.body}>
          <Grid item xs={12}>
            <strong>Table Name:</strong> {tableName}
          </Grid>
          <Grid item xs={12}>
            <strong>Cron Schedule :</strong> {get(jobDetail, 'expression', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Previous Fire Time: </strong> {get(jobDetail, 'expression', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Next Fire Time: </strong> {get(jobDetail, 'expression', '')}
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
                value={get(tableDetail, `task.taskTypeConfigsMap.${taskType}`, `{}`)}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>
      </Grid>
      {/* {!fetching && (
        <CustomizedTables
          title="Tables"
          data={tables}
          showSearchBox={true}
          inAccordionFormat={true}
          isPagination={false}
          addLinks
          baseURL='/task-queue/tables/'
        />
      )} */}
    </Grid>
  );
};

export default TaskQueueTable;