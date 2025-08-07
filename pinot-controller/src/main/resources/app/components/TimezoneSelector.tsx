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

import React, { useState, useEffect } from 'react';
import {
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  makeStyles,
  Typography,
  Box,
  TextField,
  ListItemText,
  Divider,
} from '@material-ui/core';
import { AccessTime as AccessTimeIcon } from '@material-ui/icons';
import { useTimezone } from '../contexts/TimezoneContext';
import { getStandardTimezones, standardizeTimezone } from '../utils/TimezoneUtils';

const useStyles = makeStyles((theme) => ({
  formControl: {
    minWidth: 200,
    margin: theme.spacing(1),
  },
  select: {
    '& .MuiSelect-select': {
      display: 'flex',
      alignItems: 'center',
    },
  },
  menuItem: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  searchField: {
    margin: theme.spacing(1),
    width: '100%',
  },
  divider: {
    margin: theme.spacing(1, 0),
  },
  timezoneLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: theme.spacing(1),
  },
}));

interface TimezoneSelectorProps {
  variant?: 'outlined' | 'filled' | 'standard';
  size?: 'small' | 'medium';
  showIcon?: boolean;
}

const TimezoneSelector: React.FC<TimezoneSelectorProps> = ({
  variant = 'outlined',
  size = 'small',
  showIcon = true,
}) => {
  const classes = useStyles();
  const { currentTimezone, setTimezone } = useTimezone();
  const [searchTerm, setSearchTerm] = useState('');
  const [standardTimezones, setStandardTimezones] = useState<Array<{ value: string; label: string }>>([]);
  const [filteredTimezones, setFilteredTimezones] = useState<Array<{ value: string; label: string }>>([]);

  useEffect(() => {
    // Load standardized timezones
    const timezones = getStandardTimezones();
    setStandardTimezones(timezones);
    setFilteredTimezones(timezones);
  }, []);

  useEffect(() => {
    // Filter timezones based on search term
    if (!searchTerm.trim()) {
      setFilteredTimezones(standardTimezones);
    } else {
      const filtered = standardTimezones.filter(tz =>
        tz.value.toLowerCase().includes(searchTerm.toLowerCase()) ||
        tz.label.toLowerCase().includes(searchTerm.toLowerCase())
      );
      setFilteredTimezones(filtered);
    }
  }, [searchTerm, standardTimezones]);

  const handleTimezoneChange = (event: React.ChangeEvent<{ value: unknown }>) => {
    const newTimezone = event.target.value as string;
    setTimezone(newTimezone);
  };

  const renderMenuItem = (timezone: { value: string; label: string }) => (
    <MenuItem key={timezone.value} value={timezone.value} className={classes.menuItem}>
      <Box className={classes.timezoneLabel}>
        {showIcon && <AccessTimeIcon fontSize="small" />}
        <Typography variant="body2">{timezone.value}</Typography>
      </Box>
      <Typography variant="caption" color="textSecondary">
        {timezone.label.split('(')[1]?.replace(')', '')}
      </Typography>
    </MenuItem>
  );

  return (
    <FormControl variant={variant} size={size} className={classes.formControl}>
      <InputLabel>Timezone</InputLabel>
      <Select
        value={standardizeTimezone(currentTimezone)}
        onChange={handleTimezoneChange}
        className={classes.select}
        MenuProps={{
          PaperProps: {
            style: {
              maxHeight: 400,
            },
          },
          keepMounted: true,
          disablePortal: true,
        }}
      >
        {/* Search field */}
        <Box p={1} onClick={(e) => e.stopPropagation()}>
          <TextField
            placeholder="Search timezones..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            onClick={(e) => e.stopPropagation()}
            onMouseDown={(e) => e.stopPropagation()}
            onKeyDown={(e) => e.stopPropagation()}
            onKeyUp={(e) => e.stopPropagation()}
            size="small"
            fullWidth
            variant="outlined"
            className={classes.searchField}
          />
        </Box>

        <Divider className={classes.divider} />

        {/* Standardized timezones section */}
        <MenuItem disabled>
          <Typography variant="subtitle2" color="textSecondary">
            Timezones (UTC Offsets & 3-Letter Codes)
          </Typography>
        </MenuItem>
        {filteredTimezones.map(renderMenuItem)}
      </Select>
    </FormControl>
  );
};

export default TimezoneSelector;
