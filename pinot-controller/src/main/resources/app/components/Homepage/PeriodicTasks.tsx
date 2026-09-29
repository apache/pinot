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

import React, { useContext, useEffect, useState } from 'react';
import {
  Box,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Typography,
  makeStyles
} from '@material-ui/core';
import { get } from 'lodash';
import { TableData } from 'Models';
import SimpleAccordion from '../SimpleAccordion';
import CustomButton from '../CustomButton';
import CustomDialog from '../CustomDialog';
import CustomizedTables from '../Table';
import PinotMethodUtils from '../../utils/PinotMethodUtils';
import { NotificationContext } from '../Notification/NotificationContext';

const useStyles = makeStyles(() => ({
  box: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
  formField: {
    width: '100%',
  },
}));

const PeriodicTasks = () => {
  const classes = useStyles();
  const { dispatch } = useContext(NotificationContext);

  const [periodicTaskNames, setPeriodicTaskNames] = useState<TableData>({ records: [], columns: ['Periodic Task Name'] });
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [selectedPeriodicTask, setSelectedPeriodicTask] = useState<string>('');
  const [periodicTaskTableName, setPeriodicTaskTableName] = useState<string>('');
  const [periodicTaskTableType, setPeriodicTaskTableType] = useState<string>('');
  const [running, setRunning] = useState(false);

  useEffect(() => {
    let isMounted = true;
    PinotMethodUtils.getAllPeriodicTaskNames()
      .then((res) => {
        if (!isMounted) {
          return;
        }
        setPeriodicTaskNames(res);
      })
      .catch((err) => {
        if (!isMounted) {
          return;
        }
        dispatch({
          type: 'error',
          message: `Failed to load periodic tasks: ${get(err, 'response.data.error') || (err as Error).message || 'unknown error'}`,
          show: true,
        });
      });
    return () => { isMounted = false; };
  }, [dispatch]);

  const handleRunPeriodicTask = async () => {
    if (!selectedPeriodicTask) {
      return;
    }
    setRunning(true);
    try {
      const response = await PinotMethodUtils.runPeriodicTaskAction(
        selectedPeriodicTask,
        periodicTaskTableName.trim() || undefined,
        periodicTaskTableType.trim() || undefined
      );
      dispatch({
        type: 'success',
        message: get(response, 'description') || `Periodic task ${selectedPeriodicTask} triggered`,
        show: true,
      });
      setRunDialogOpen(false);
      setPeriodicTaskTableName('');
      setPeriodicTaskTableType('');
      setSelectedPeriodicTask('');
    } catch (e) {
      dispatch({
        type: 'error',
        message: `Failed to trigger periodic task: ${get(e, 'response.data.error') || (e as Error).message}`,
        show: true,
      });
    } finally {
      setRunning(false);
    }
  };

  const periodicTaskCount = periodicTaskNames?.records?.length || 0;

  return (
    <>
      <div className={classes.box}>
        <SimpleAccordion
          headerTitle={`Periodic Tasks${periodicTaskCount ? ` (${periodicTaskCount})` : ''}`}
          showSearchBox={false}
        >
          <Box p={2}>
            <CustomButton
              onClick={() => setRunDialogOpen(true)}
              tooltipTitle="Manually trigger a controller periodic task. Optionally scope to a single table."
              enableTooltip={true}
              isDisabled={periodicTaskCount === 0}
            >
              Run Periodic Task
            </CustomButton>
            {periodicTaskCount > 0 && (
              <Box mt={2}>
                <CustomizedTables
                  title="Registered Periodic Tasks"
                  data={periodicTaskNames}
                  showSearchBox={true}
                  inAccordionFormat={false}
                />
              </Box>
            )}
            {periodicTaskCount === 0 && (
              <Typography variant='body2'>No periodic tasks registered.</Typography>
            )}
          </Box>
        </SimpleAccordion>
      </div>

      <CustomDialog
        open={runDialogOpen}
        title="Run Periodic Task"
        handleClose={() => setRunDialogOpen(false)}
        handleSave={handleRunPeriodicTask}
        btnOkText={running ? 'Running...' : 'Run'}
        okBtnDisabled={running || !selectedPeriodicTask}
      >
        <Box display="flex" flexDirection="column" pt={1} pb={1}>
          <FormControl variant="outlined" size="small" className={classes.formField} fullWidth>
            <InputLabel>Periodic Task</InputLabel>
            <Select
              label="Periodic Task"
              value={selectedPeriodicTask}
              onChange={(e) => setSelectedPeriodicTask(e.target.value as string)}
            >
              {periodicTaskNames.records.map((row) => {
                const name = Array.isArray(row) ? row[0] : row;
                return <MenuItem key={String(name)} value={String(name)}>{String(name)}</MenuItem>;
              })}
            </Select>
          </FormControl>
          <Box mt={2}>
            <TextField
              label="Table name (optional, raw or with type suffix)"
              variant="outlined"
              size="small"
              fullWidth
              value={periodicTaskTableName}
              onChange={(e) => setPeriodicTaskTableName(e.target.value)}
            />
          </Box>
          <Box mt={2}>
            <TextField
              label="Table type (optional: OFFLINE / REALTIME)"
              variant="outlined"
              size="small"
              fullWidth
              value={periodicTaskTableType}
              onChange={(e) => setPeriodicTaskTableType(e.target.value)}
            />
          </Box>
          <Box mt={1}>
            <Typography variant='caption'>
              Leaving the table name empty runs the periodic task across all tables.
            </Typography>
          </Box>
        </Box>
      </CustomDialog>
    </>
  );
};

export default PeriodicTasks;
