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

import * as React from 'react';
import { Typography, Toolbar, Tooltip } from '@material-ui/core';
import {
  makeStyles
} from '@material-ui/core/styles';
import SearchBar from './SearchBar';
import HelpOutlineIcon from '@material-ui/icons/HelpOutline';

type Props = {
  name: string;
  showSearchBox: boolean;
  searchValue?: string;
  handleSearch?: Function;
  recordCount?: number;
  showTooltip?: boolean;
  tooltipText?: string;
};

const useToolbarStyles = makeStyles((theme) => ({
  root: {
    paddingLeft: '15px',
    paddingRight: '15px',
    minHeight: 48,
    backgroundColor: 'rgba(66, 133, 244, 0.1)'
  },
  title: {
    flex: '1 1 auto',
    fontWeight: 600,
    letterSpacing: '1px',
    fontSize: '1rem',
    color: '#4285f4'
  },
}));

export default function TableToolbar({
  name,
  showSearchBox,
  searchValue,
  handleSearch,
  recordCount,
  showTooltip,
  tooltipText
}: Props) {
  const classes = useToolbarStyles();

  return (
    <Toolbar className={classes.root}>
      <Typography
        className={classes.title}
        variant="h6"
        id="tableTitle"
        component="div"
      >
        {name.toUpperCase()}
      </Typography>
      {showSearchBox ? <SearchBar
        value={searchValue}
        onChange={(e) => handleSearch(e.target.value)}
      /> : <strong>{(recordCount)}</strong>}
      {showTooltip &&
        <Tooltip title={tooltipText}>
          <HelpOutlineIcon />
        </Tooltip>
      }
    </Toolbar>
  );
}