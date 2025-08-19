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
import { DISPLAY_SEGMENT_STATUS } from 'Models';

const useStyles = makeStyles((theme) => ({
  formControl: {
    minWidth: 140,
    height: 32, // Match search bar height
  },
  select: {
    height: 32,
    fontSize: '0.875rem',
    backgroundColor: '#fff',
    '& .MuiSelect-select': {
      paddingTop: 6,
      paddingBottom: 6,
      paddingLeft: 12,
      paddingRight: 32,
      display: 'flex',
      alignItems: 'center',
      height: 'auto',
      minHeight: 'unset',
    },
    '& .MuiOutlinedInput-root': {
      borderRadius: 4,
      '&:hover .MuiOutlinedInput-notchedOutline': {
        borderColor: '#4285f4',
      },
      '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
        borderColor: '#4285f4',
        borderWidth: 1,
      },
    },
    '& .MuiOutlinedInput-notchedOutline': {
      borderColor: '#BDCCD9',
    },
  },
  inputLabel: {
    fontSize: '0.75rem',
    color: '#666',
    transform: 'translate(12px, 9px) scale(1)',
    '&.MuiInputLabel-shrink': {
      transform: 'translate(12px, -6px) scale(0.75)',
      color: '#4285f4',
    },
    '&.Mui-focused': {
      color: '#4285f4',
    },
  },
  menuItem: {
    padding: '6px 12px',
    fontSize: '0.875rem',
    minHeight: 'auto',
    '&:hover': {
      backgroundColor: 'rgba(66, 133, 244, 0.08)',
    },
    '&.Mui-selected': {
      backgroundColor: 'rgba(66, 133, 244, 0.12)',
      '&:hover': {
        backgroundColor: 'rgba(66, 133, 244, 0.16)',
      },
    },
  },
  statusChip: {
    height: 18,
    fontSize: '0.7rem',
    fontWeight: 600,
    marginLeft: 6,
    '& .MuiChip-label': {
      paddingLeft: 6,
      paddingRight: 6,
    }
  },
  // Status styles
  cellStatusGood: {
    color: '#4CAF50',
    backgroundColor: 'rgba(76, 175, 80, 0.1)',
    border: '1px solid #4CAF50',
  },
  cellStatusBad: {
    color: '#f44336',
    backgroundColor: 'rgba(244, 67, 54, 0.1)',
    border: '1px solid #f44336',
  },
  cellStatusConsuming: {
    color: '#ff9800',
    backgroundColor: 'rgba(255, 152, 0, 0.1)',
    border: '1px solid #ff9800',
  },
  cellStatusError: {
    color: '#a11',
    backgroundColor: 'rgba(170, 17, 17, 0.1)',
    border: '1px solid #a11',
  },
  menuPaper: {
    marginTop: 2,
    boxShadow: '0px 2px 8px rgba(0, 0, 0, 0.15)',
    border: '1px solid #BDCCD9',
    maxHeight: 200,
  }
}));

type StatusFilterOption = {
  label: string;
  value: 'ALL' | DISPLAY_SEGMENT_STATUS | 'BAD_OR_UPDATING';
};

type StatusFilterProps = {
  value: 'ALL' | DISPLAY_SEGMENT_STATUS | 'BAD_OR_UPDATING';
  onChange: (value: 'ALL' | DISPLAY_SEGMENT_STATUS | 'BAD_OR_UPDATING') => void;
  options: StatusFilterOption[];
};

export const getStatusChipClass = (status: string, classes?: any) => {
  const normalizedStatus = status.toLowerCase();
  switch (normalizedStatus) {
    case DISPLAY_SEGMENT_STATUS.GOOD.toLowerCase():
      return classes.cellStatusGood;
    case DISPLAY_SEGMENT_STATUS.BAD.toLowerCase():
      return classes.cellStatusBad;
    case DISPLAY_SEGMENT_STATUS.UPDATING.toLowerCase():
      return classes.cellStatusConsuming;
    case 'error':
      return classes.cellStatusError;
    case 'bad_or_updating':
      return classes.cellStatusBad;
    default:
      return '';
  }
};

const StatusFilter: React.FC<StatusFilterProps> = ({ value, onChange, options }) => {
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
          className={`${classes.statusChip} ${getStatusChipClass(selected, classes)}`}
        />
      </div>
    );
  };

  return (
    <FormControl variant="outlined" className={classes.formControl} size="small">
      <InputLabel className={classes.inputLabel}>Filter</InputLabel>
      <Select
        value={value}
        onChange={(e) => onChange(e.target.value as 'ALL' | DISPLAY_SEGMENT_STATUS | 'BAD_OR_UPDATING')}
        label="Filter"
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
                className={`${classes.statusChip} ${getStatusChipClass(option.value, classes)}`}
              />
            </div>
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default StatusFilter;
