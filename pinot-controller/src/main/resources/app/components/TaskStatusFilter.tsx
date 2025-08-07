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
import React from 'react';
import {
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  makeStyles,
  Chip
} from '@material-ui/core';

const useStyles = makeStyles((theme) => ({
  formControl: {
    minWidth: 180,
    marginRight: theme.spacing(2),
  },
  inputLabel: {
    backgroundColor: 'white',
    paddingLeft: theme.spacing(1),
    paddingRight: theme.spacing(1),
    color: theme.palette.text.secondary,
  },
  select: {
    '& .MuiSelect-select': {
      padding: '8px 12px',
    },
  },
  menuPaper: {
    maxHeight: 300,
    marginTop: 4,
  },
  menuItem: {
    padding: '8px 16px',
    '&:hover': {
      backgroundColor: theme.palette.action.hover,
    },
  },
  statusChip: {
    fontSize: '0.75rem',
    height: 20,
    borderRadius: 10,
    fontWeight: 500,
  },
  completed: {
    backgroundColor: '#e8f5e8',
    borderColor: '#4caf50',
    color: '#2e7d32',
  },
  running: {
    backgroundColor: '#e3f2fd',
    borderColor: '#2196f3',
    color: '#1565c0',
  },
  waiting: {
    backgroundColor: '#fff3e0',
    borderColor: '#ff9800',
    color: '#ef6c00',
  },
  error: {
    backgroundColor: '#ffebee',
    borderColor: '#f44336',
    color: '#c62828',
  },
  unknown: {
    backgroundColor: '#f3e5f5',
    borderColor: '#9c27b0',
    color: '#7b1fa2',
  },
  dropped: {
    backgroundColor: '#fce4ec',
    borderColor: '#e91e63',
    color: '#ad1457',
  },
  timedout: {
    backgroundColor: '#fff8e1',
    borderColor: '#ffc107',
    color: '#f57c00',
  },
  aborted: {
    backgroundColor: '#efebe9',
    borderColor: '#795548',
    color: '#5d4037',
  },
}));

export type TaskStatus = 'COMPLETED' | 'RUNNING' | 'WAITING' | 'ERROR' | 'UNKNOWN' | 'DROPPED' | 'TIMED_OUT' | 'ABORTED';

type TaskStatusFilterOption = {
  label: string;
  value: 'ALL' | TaskStatus;
};

type TaskStatusFilterProps = {
  value: 'ALL' | TaskStatus;
  onChange: (value: 'ALL' | TaskStatus) => void;
  options: TaskStatusFilterOption[];
};

export const getTaskStatusChipClass = (status: string, classes?: any) => {
  if (!classes) return '';

  switch (status.toUpperCase()) {
    case 'COMPLETED':
      return classes.completed;
    case 'RUNNING':
      return classes.running;
    case 'WAITING':
      return classes.waiting;
    case 'ERROR':
      return classes.error;
    case 'UNKNOWN':
      return classes.unknown;
    case 'DROPPED':
      return classes.dropped;
    case 'TIMED_OUT':
    case 'TIMEDOUT':
      return classes.timedout;
    case 'ABORTED':
      return classes.aborted;
    default:
      return '';
  }
};

const TaskStatusFilter: React.FC<TaskStatusFilterProps> = ({ value, onChange, options }) => {
  const classes = useStyles();

  const renderValue = (selected: string) => {
    const selectedOption = options.find(option => option.value === selected);
    const label = selectedOption ? selectedOption.label : 'All';

    if (selected === 'ALL') {
      return label;
    }

    return (
      <div style={{ display: 'flex', alignItems: 'center' }}>
        <Chip
          size="small"
          label={label}
          variant="outlined"
          className={`${classes.statusChip} ${getTaskStatusChipClass(selected, classes)}`}
        />
      </div>
    );
  };

  return (
    <FormControl variant="outlined" className={classes.formControl} size="small">
      <InputLabel className={classes.inputLabel}>Status Filter</InputLabel>
      <Select
        value={value}
        onChange={(event) => onChange(event.target.value as 'ALL' | TaskStatus)}
        label="Status Filter"
        className={classes.select}
        renderValue={renderValue}
        MenuProps={{
          PaperProps: {
            className: classes.menuPaper,
          },
          anchorOrigin: {
            vertical: 'bottom',
            horizontal: 'left',
          },
          transformOrigin: {
            vertical: 'top',
            horizontal: 'left',
          },
          getContentAnchorEl: null,
        }}
      >
        {options.map((option) => (
          <MenuItem
            key={option.value}
            value={option.value}
            className={classes.menuItem}
          >
            <div style={{
              display: 'flex',
              alignItems: 'center',
              width: '100%',
              justifyContent: 'space-between'
            }}>
              <Chip
                size="small"
                label={option.label}
                variant="outlined"
                className={`${classes.statusChip} ${getTaskStatusChipClass(option.value, classes)}`}
              />
            </div>
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default TaskStatusFilter;
