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
import { Grid, makeStyles, Typography } from '@material-ui/core';
import SimpleAccordion from '../SimpleAccordion';
import PinotMethodUtils from '../../utils/PinotMethodUtils';
import { formatTimeInTimezone } from '../../utils/TimezoneUtils';
import { useTimezone } from '../../contexts/TimezoneContext';

const useStyles = makeStyles(() => ({
  box: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
  body: {
    padding: '12px 16px',
    fontSize: 14,
    lineHeight: '2rem',
  },
}));

// Shape returned by /tasks/scheduler/information. The controller currently
// emits the leaked Java method name `getThreadPoolSize`; we tolerate both that
// and a future cleaner `threadPoolSize` key for forward compatibility.
type SchedulerInfo = {
  Version?: string;
  SchedulerName?: string;
  SchedulerInstanceId?: string;
  InStandbyMode?: boolean;
  RunningSince?: number;
  NumberOfJobsExecuted?: number;
  JobDetails?: Array<unknown>;
  getThreadPoolSize?: number;
  threadPoolSize?: number;
};

const CronSchedulerInformation = () => {
  const classes = useStyles();
  const { currentTimezone } = useTimezone();
  const [schedulerInfo, setSchedulerInfo] = useState<SchedulerInfo | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    PinotMethodUtils.getCronSchedulerInformationData()
      .then((res) => {
        if (!isMounted) {
          return;
        }
        setSchedulerInfo(res as SchedulerInfo | null);
        setError(null);
      })
      .catch((err) => {
        if (!isMounted) {
          return;
        }
        setError(
          String(get(err, 'response.data.error')
            || get(err, 'response.data.message')
            || (err && err.message)
            || 'Unable to load cron scheduler information')
        );
      });
    return () => { isMounted = false; };
  }, []);

  const status = error
    ? 'Unknown'
    : (schedulerInfo
      ? (schedulerInfo.InStandbyMode ? 'Standby' : 'Running')
      : 'Disabled');
  const runningSince = schedulerInfo?.RunningSince
    ? formatTimeInTimezone(schedulerInfo.RunningSince, 'YYYY-MM-DD HH:mm:ss z', currentTimezone)
    : '-';
  const threadPoolSize = schedulerInfo?.getThreadPoolSize ?? schedulerInfo?.threadPoolSize ?? '-';

  return (
    <div className={classes.box}>
      <SimpleAccordion headerTitle="Cron Scheduler Information" showSearchBox={false}>
        <Grid container className={classes.body}>
          <Grid item xs={6}><strong>Scheduler Name:</strong> {schedulerInfo?.SchedulerName || '-'}</Grid>
          <Grid item xs={6}><strong>Instance Id:</strong> {schedulerInfo?.SchedulerInstanceId || '-'}</Grid>
          <Grid item xs={6}><strong>Version:</strong> {schedulerInfo?.Version || '-'}</Grid>
          <Grid item xs={6}><strong>Thread Pool Size:</strong> {threadPoolSize}</Grid>
          <Grid item xs={6}><strong>Jobs Executed:</strong> {schedulerInfo?.NumberOfJobsExecuted ?? '-'}</Grid>
          <Grid item xs={6}><strong>Running Since:</strong> {runningSince}</Grid>
          <Grid item xs={6}><strong>State:</strong> {status}</Grid>
          <Grid item xs={6}><strong>Scheduled Jobs:</strong> {schedulerInfo?.JobDetails?.length ?? '-'}</Grid>
          {error && (
            <Grid item xs={12}>
              <Typography variant='caption' color='error'>
                Failed to load cron scheduler information: {error}
              </Typography>
            </Grid>
          )}
          {!error && !schedulerInfo && (
            <Grid item xs={12}>
              <Typography variant='caption'>Cron scheduler is disabled on this controller.</Typography>
            </Grid>
          )}
        </Grid>
      </SimpleAccordion>
    </div>
  );
};

export default CronSchedulerInformation;
