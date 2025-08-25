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
import { Link } from 'react-router-dom';
import { AppBar, Box, makeStyles, Paper } from '@material-ui/core';
import MenuIcon from '@material-ui/icons/Menu';
import Logo from './SvgIcons/Logo';
import BreadcrumbsComponent from './Breadcrumbs';
import TimezoneSelector from './TimezoneSelector';

type Props = {
  highlightSidebarLink: (id: number) => void;
  showHideSideBarHandler: () => void;
  openSidebar: boolean;
  clusterName: string;
};

const useStyles = makeStyles((theme) => ({
  breadcrumbRoot:{
    flexGrow: 1
  },
  paper:{
    padding: '0 0.5rem',
    color: '#fff',
    textAlign: 'center',
    backgroundColor: 'rgba(66, 133, 244, 0.1)',
    boxShadow: 'none',
    fontSize: 'smaller',
    '& h2, h4': {
      margin: 0,
    },
    '& h2':{
      fontWeight: 600
    },
    '& h4':{
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 500
    }
  },
  timezoneContainer: {
    display: 'flex',
    alignItems: 'center',
    marginRight: theme.spacing(2),
    '& .MuiFormControl-root': {
      margin: 0,
    },
    '& .MuiInputLabel-root': {
      color: 'rgba(255, 255, 255, 0.7)',
    },
    '& .MuiSelect-select': {
      color: '#fff',
    },
    '& .MuiSelect-icon': {
      color: 'rgba(255, 255, 255, 0.7)',
    },
    '& .MuiOutlinedInput-notchedOutline': {
      borderColor: 'rgba(255, 255, 255, 0.3)',
    },
    '& .MuiOutlinedInput-root:hover .MuiOutlinedInput-notchedOutline': {
      borderColor: 'rgba(255, 255, 255, 0.5)',
    },
    '& .MuiOutlinedInput-root.Mui-focused .MuiOutlinedInput-notchedOutline': {
      borderColor: 'rgba(255, 255, 255, 0.7)',
    },
  }
}));

const Header = ({ highlightSidebarLink, showHideSideBarHandler, openSidebar, clusterName, ...props }: Props) => {
  const classes = useStyles();
  return (
    <AppBar position="static">
      <Box display="flex">
        <Box textAlign="center" marginY="12.5px" width={openSidebar ? 250 : 90} borderRight="1px solid rgba(255,255,255,0.5)">
          <Link to="/" style={{color: '#ffffff'}}><Logo onClick={() => highlightSidebarLink(1)} fulllogo={openSidebar.toString()} /></Link>
        </Box>
        <Box display="flex" alignItems="center" className={classes.breadcrumbRoot}>
          <Box marginY="auto" padding="0.25rem 0 0.25rem 1.5rem" display="flex" style={{cursor: 'pointer'}}>
            <MenuIcon onClick={() => showHideSideBarHandler()} />
          </Box>
          <BreadcrumbsComponent {...props} />
        </Box>
        <Box display="flex" alignItems="center" marginRight={2}>
          <Box className={classes.timezoneContainer}>
            <TimezoneSelector variant="outlined" size="small" showIcon={false} />
          </Box>
        </Box>
        <Box textAlign="center" marginY="11.5px" borderLeft="1px solid rgba(255,255,255,0.5)">
          <Paper className={classes.paper}>
            <h4>Cluster Name</h4>
            <h2>{clusterName}</h2>
          </Paper>
        </Box>
      </Box>
    </AppBar>
  )
};

export default Header;
