/* eslint-disable no-nested-ternary */
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

import React, {  } from 'react';
import { createStyles, Theme, makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import CssBaseline from '@material-ui/core/CssBaseline';
import RefreshOutlinedIcon from '@material-ui/icons/RefreshOutlined';
import NoteAddOutlinedIcon from '@material-ui/icons/NoteAddOutlined';
import DeleteOutlineOutlinedIcon from '@material-ui/icons/DeleteOutlineOutlined';
import EditOutlinedIcon from '@material-ui/icons/EditOutlined';
import { Grid, ButtonGroup, Button, Tooltip, Popover, Typography } from '@material-ui/core';
import MaterialTree from '../MaterialTree';

const drawerWidth = 400;

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    drawer: {
      width: drawerWidth,
      height: 'calc(100vh - 70px)',
      flexShrink: 0,
      backgroundColor: '#333333',
    },
    drawerPaper: {
      position: 'unset',
      width: drawerWidth,
    },
    drawerContainer: {
      overflow: 'auto',
      paddingTop: '20px'
    },
    leftPanel: {
      width: drawerWidth,
      padding: '0 20px'
    },
    buttonGrpDiv:{
      textAlign: 'center'
    },
    btnGroup: {
      marginBottom: '20px'
    },
    typography: {
      padding: theme.spacing(1),
    },
    popover: {
      '& .MuiPopover-paper': {
        backgroundColor: '#4285f4',
        color: 'white',
        overflow: 'visible',
        '&:after': {
          bottom: '100%',
          left: '50%',
          border: 'solid transparent',
          content: '" "',
          height: '0',
          width: '0',
          position: 'absolute',
          pointerEvents: 'none',
          borderBottomColor: '#4285f4',
          borderWidth: '8px',
          marginLeft: '-8px',
          zIndex: '9',
        }
      },
    }
  }),
);

type Props = {
  treeData: Array<any> | null;
  showChildEvent: (event: React.MouseEvent<HTMLLIElement, MouseEvent>) => void;
  selectedNode: String | null;
  expanded: any;
  selected: any;
  handleToggle: any;
  handleSelect: any;
  refreshAction: Function;
};

const TreeDirectory = ({treeData, showChildEvent, expanded, selected, handleToggle, handleSelect, refreshAction}: Props) => {
  const classes = useStyles();

  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(null);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const open = Boolean(anchorEl);
  const id = open ? 'simple-popover' : undefined;

  return (
    <>
      <CssBaseline />
      <Drawer
        open={false}
        className={classes.drawer}
        variant="permanent"
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        <div className={classes.drawerContainer}>
          <Grid item xs className={classes.leftPanel}>
            <div className={classes.buttonGrpDiv}>
              <ButtonGroup color="primary" aria-label="outlined primary button group" className={classes.btnGroup}>
                <Tooltip title="Refresh">
                  <Button onClick={(e)=>{refreshAction();}}><RefreshOutlinedIcon/></Button>
                </Tooltip>
                <Tooltip title="Add">
                  <Button onClick={handleClick}><NoteAddOutlinedIcon/></Button>
                </Tooltip>
                <Tooltip title="Delete">
                  <Button onClick={handleClick}><DeleteOutlineOutlinedIcon/></Button>
                </Tooltip>
                <Tooltip title="Edit">
                  <Button onClick={handleClick}><EditOutlinedIcon/></Button>
                </Tooltip>
              </ButtonGroup>
            </div>
            <Popover
              id={id}
              open={open}
              anchorEl={anchorEl}
              onClose={handleClose}
              anchorOrigin={{
                vertical: 'bottom',
                horizontal: 'center',
              }}
              transformOrigin={{
                vertical: 'top',
                horizontal: 'center',
              }}
              className={classes.popover}
            >
              <Typography className={classes.typography}>Functionality Coming Soon.</Typography>
            </Popover>
            <MaterialTree
              treeData={treeData}
              showChildEvent={showChildEvent}
              expanded={expanded}
              selected={selected}
              handleToggle={handleToggle}
              handleSelect={handleSelect}
            />
          </Grid>
        </div>
      </Drawer>
    </>
  );
};

export default TreeDirectory;