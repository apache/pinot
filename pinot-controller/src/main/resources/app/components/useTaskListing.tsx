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

import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { Box } from '@material-ui/core';
import { TableData } from 'Models';
import CustomizedTables from './Table';
import TaskStatusFilter, { TaskStatus } from './TaskStatusFilter';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { useTimezone } from '../contexts/TimezoneContext';

export default function useTaskListing(props) {
  const { taskType, tableName } = props;
  const { currentTimezone } = useTimezone();
  const [fetching, setFetching] = useState(true);
  const [tasks, setTasks] = useState<TableData>({ records: [], columns: [] });
  const [statusFilter, setStatusFilter] = useState<'ALL' | TaskStatus>('ALL');

  const fetchData = useCallback(async () => {
    setFetching(true);
    const tasksRes = await PinotMethodUtils.getTasksList(tableName, taskType);
    setTasks(tasksRes);
    setFetching(false);
  }, [tableName, taskType]);

  useEffect(() => {
    fetchData();
  }, [currentTimezone, fetchData]);

  const filteredTasks = useMemo(() => {
    if (statusFilter === 'ALL') {
      return tasks;
    }

    const filtered = tasks.records.filter(([_, status]) => {
      const rawStatus = (typeof status === 'object' && status !== null && 'value' in status)
        ? (status as { value?: unknown }).value
        : status;
      const statusString = typeof rawStatus === 'string' ? rawStatus : '';
      const upperStatus = statusString.toUpperCase();
      const normalized = upperStatus === 'TIMEDOUT' ? 'TIMED_OUT' : upperStatus;
      return normalized === statusFilter;
    });

    return { ...tasks, records: filtered };
  }, [tasks, statusFilter]);

  // Minion task page statuses
  const statusFilterOptions = [
    { label: 'All', value: 'ALL' as const },
    { label: 'Not Started', value: 'NOT_STARTED' as const },
    { label: 'In Progress', value: 'IN_PROGRESS' as const },
    { label: 'Stopped', value: 'STOPPED' as const },
    { label: 'Stopping', value: 'STOPPING' as const },
    { label: 'Failed', value: 'FAILED' as const },
    { label: 'Completed', value: 'COMPLETED' as const },
    { label: 'Aborted', value: 'ABORTED' as const },
    { label: 'Timed Out', value: 'TIMED_OUT' as const },
    { label: 'Timing Out', value: 'TIMING_OUT' as const },
    { label: 'Failing', value: 'FAILING' as const },
  ];

  const statusFilterElement = (
    <TaskStatusFilter
      value={statusFilter}
      onChange={setStatusFilter}
      options={statusFilterOptions}
    />
  );

  return {
    tasks,
    setTasks,
    content: !fetching && (
      <CustomizedTables
        title="Tasks"
        data={filteredTasks}
        showSearchBox={true}
        inAccordionFormat={true}
        addLinks
        baseURL={`/task-queue/${taskType}/tables/${tableName}/task/`}
        additionalControls={<Box display="flex" alignItems="center">{statusFilterElement}</Box>}
      />
    )
  };
}
