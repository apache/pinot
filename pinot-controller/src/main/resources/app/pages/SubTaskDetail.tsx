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
import { get, find } from 'lodash';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { Grid, makeStyles, Box, List, ListItem, ListItemText, Typography, Divider } from '@material-ui/core';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
import { NotificationContext } from '../components/Notification/NotificationContext';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { TaskProgressStatus } from 'Models';
import moment from 'moment';
import { formatTimeInTimezone } from '../utils/TimezoneUtils';
import { useTimezone } from '../contexts/TimezoneContext';

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
    '& .CodeMirror': { maxHeight: 400 },
  },
  taskDetailContainer: {
    overflow: "auto",
    whiteSpace: "pre",
    height: 600
  }
}));

const TaskDetail = (props) => {
  const classes = useStyles();
  const { currentTimezone } = useTimezone();
  const { dispatch } = useContext(NotificationContext);
  const { subTaskID, taskID, queueTableName } = props.match.params;
  const [taskDebugData, setTaskDebugData] = useState({});
  const [taskProgressData, setTaskProgressData] = useState<TaskProgressStatus[] | string>("");
  const [logFiles, setLogFiles] = useState<string[]>([]);
  const [logFilesError, setLogFilesError] = useState<string | null>(null);
  const [logFilesLoading, setLogFilesLoading] = useState<boolean>(false);
  const [downloadingFile, setDownloadingFile] = useState<string | null>(null);

  const fetchTaskDebugData = async () => {
    const debugRes = await PinotMethodUtils.getTaskDebugData(taskID, queueTableName);
    const subTaskData = find(debugRes.data.subtaskInfos, (subTask) => get(subTask, 'taskId', '') === subTaskID);
    setTaskDebugData(subTaskData);
  };

  const fetchTaskProgressData = async () => {
      const taskProgressData = await PinotMethodUtils.getTaskProgressData(taskID, subTaskID);
      setTaskProgressData(get(taskProgressData, subTaskID, "No Status"));
  }

  const fetchLogFiles = async (participant: string) => {
    if (!participant) {
      return;
    }
    setLogFilesLoading(true);
    setLogFilesError(null);
    try {
      const files = await PinotMethodUtils.getInstanceLogFilesData(participant);
      setLogFiles(files);
    } catch (e) {
      setLogFilesError(
        get(e, 'response.data.error')
          || get(e, 'response.data.message')
          || (e as Error).message
          || 'Unable to fetch logs'
      );
      setLogFiles([]);
    } finally {
      setLogFilesLoading(false);
    }
  };

  const handleDownloadLog = async (participant: string, filePath: string) => {
    setDownloadingFile(filePath);
    try {
      await PinotMethodUtils.downloadInstanceLogFileToBrowser(participant, filePath);
    } catch (e) {
      dispatch({
        type: 'error',
        message: `Failed to download ${filePath}: ${get(e, 'response.data.error') || (e as Error).message || 'unknown error'}`,
        show: true,
      });
    } finally {
      setDownloadingFile(null);
    }
  };

  useEffect(() => {
    fetchTaskDebugData();
    fetchTaskProgressData();
  }, [currentTimezone]);

  const participant: string = get(taskDebugData, 'participant', '');

  useEffect(() => {
    if (participant) {
      fetchLogFiles(participant);
    }
  }, [participant]);

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
          {get(taskDebugData, 'startTime') && (
            <Grid item xs={12}>
              <strong>Start Time:</strong> {formatTimeInTimezone(get(taskDebugData, 'startTime'), 'MMMM Do YYYY, HH:mm:ss z')}
            </Grid>
          )}
          {get(taskDebugData, 'finishTime') && (
            <Grid item xs={12}>
              <strong>Finish Time:</strong> {formatTimeInTimezone(get(taskDebugData, 'finishTime'), 'MMMM Do YYYY, HH:mm:ss z')}
            </Grid>
          )}
          <Grid item xs={12}>
            <strong>Triggered By:</strong> {get(taskDebugData, 'triggeredBy', '')}
          </Grid>
          <Grid item xs={12}>
            <strong>Minion Host Name:</strong> {get(taskDebugData, 'participant', '')}
          </Grid>
        </Grid>
      </div>
      <Grid container spacing={2}>
        <Grid item xs={12}>
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
              <Box p={3} className={classes.taskDetailContainer}>
                {get(taskDebugData, `info`, '')}
              </Box>
            </SimpleAccordion>
          </div>
        </Grid>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Progress"
              showSearchBox={false}
            >
              <div style={{overflow: "auto"}}>
                <List style={{width: "max-content"}} className={classes.taskDetailContainer}>
                  {typeof taskProgressData === "string" && (
                    <ListItem >
                      <ListItemText>{taskProgressData}</ListItemText>
                    </ListItem>
                  )}
                  {typeof taskProgressData !== "string" && taskProgressData.map((data, index) => (
                    <div key={index}>
                      <ListItem >
                        <ListItemText
                          primary={formatTimeInTimezone(+data.ts, 'YYYY-MM-DD HH:mm:ss z')}
                          secondary={<Typography variant='body2'>{data.status}</Typography>}
                        />
                      </ListItem>
                      <Divider />
                    </div>
                  ))}
                </List>
              </div>
            </SimpleAccordion>
          </div>
        </Grid>
        <Grid item xs={12}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle={`Minion Log Files${participant ? ` — ${participant}` : ''}`}
              showSearchBox={false}
            >
              <Box p={2}>
                {!participant && (
                  <Typography variant='body2'>This subtask has not been assigned to a minion yet.</Typography>
                )}
                {participant && (
                  <>
                    <Box mb={1}>
                      <CustomButton
                        onClick={() => fetchLogFiles(participant)}
                        tooltipTitle="Re-fetch log file list from the minion"
                        enableTooltip={true}
                        isDisabled={logFilesLoading}
                      >
                        {logFilesLoading ? 'Refreshing...' : 'Refresh'}
                      </CustomButton>
                    </Box>
                    {logFilesError && (
                      <Typography variant='body2' color='error'>
                        Failed to load log files: {logFilesError}
                      </Typography>
                    )}
                    {!logFilesError && logFiles.length === 0 && !logFilesLoading && (
                      <Typography variant='body2'>No log files reported by minion.</Typography>
                    )}
                    {logFiles.length > 0 && (
                      <List dense>
                        {logFiles.map((filePath) => {
                          const isDownloading = downloadingFile === filePath;
                          return (
                            <ListItem key={filePath} disableGutters>
                              <ListItemText
                                primary={
                                  <a
                                    href="#"
                                    onClick={(e: React.MouseEvent<HTMLAnchorElement>) => {
                                      e.preventDefault();
                                      if (!isDownloading) {
                                        handleDownloadLog(participant, filePath);
                                      }
                                    }}
                                    style={{
                                      color: isDownloading ? '#9e9e9e' : '#4285f4',
                                      cursor: isDownloading ? 'default' : 'pointer',
                                      textDecoration: 'underline',
                                    }}
                                  >
                                    {filePath}{isDownloading ? ' (downloading...)' : ''}
                                  </a>
                                }
                              />
                            </ListItem>
                          );
                        })}
                      </List>
                    )}
                  </>
                )}
              </Box>
            </SimpleAccordion>
          </div>
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TaskDetail;
