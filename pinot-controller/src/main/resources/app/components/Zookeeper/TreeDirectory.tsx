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
import { Grid, ButtonGroup, Button, Tooltip, Popover, Typography, Snackbar } from '@material-ui/core';
import MaterialTree from '../MaterialTree';
import Confirm from '../Confirm';
import CustomCodemirror from '../CustomCodemirror';
import PinotMethodUtils from '../../utils/PinotMethodUtils';
import Utils from '../../utils/Utils';
import { NotificationContext } from '../Notification/NotificationContext';

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
  isLeafNodeSelected: boolean;
  currentNodeData: Object;
  currentNodeMetadata: any;
  showInfoEvent: Function;
  fetchInnerPath: Function;
};

const TreeDirectory = ({
  treeData, showChildEvent, selectedNode, expanded, selected, handleToggle, fetchInnerPath,
  handleSelect, isLeafNodeSelected, currentNodeData, currentNodeMetadata, showInfoEvent
}: Props) => {
  const classes = useStyles();

  let newCodeMirrorData = null;
  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogTitle, setDialogTitle] = React.useState(null);
  const [dialogContent, setDialogContent] = React.useState(null);
  const [dialogSuccessCb, setDialogSuccessCb] = React.useState(null);
  const [dialogYesLabel, setDialogYesLabel] = React.useState(null);
  const [dialogNoLabel, setDialogNoLabel] = React.useState(null);
  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(null);
  const {dispatch} = React.useContext(NotificationContext);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleEditClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    if(!isLeafNodeSelected){
      return;
    }
    newCodeMirrorData = JSON.stringify(currentNodeData);
    setDialogTitle('Update Node Data');
    setDialogContent(<CustomCodemirror
      data={currentNodeData}
      isEditable={true}
      returnCodemirrorValue={(val)=>{ newCodeMirrorData = val;}}
    />);
    setDialogYesLabel('Update');
    setDialogNoLabel('Cancel');
    setDialogSuccessCb(() => confirmUpdate);
    setConfirmDialog(true);
  };

  const handleDeleteClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    if(!isLeafNodeSelected){
      return;
    }
    setDialogContent('Delete this node?');
    setDialogSuccessCb(() => deleteNode);
    setConfirmDialog(true);
  };

  const confirmUpdate = () => {
    setDialogYesLabel('Yes');
    setDialogNoLabel('No');
    setDialogContent('Are you sure want to update this node?');
    setDialogSuccessCb(() => updateNode);
  };

  const updateNode = async () => {
    const nodeData = {
      path: selectedNode,
      data: newCodeMirrorData.trim(),
      expectedVersion: currentNodeMetadata.version,
      accessOption: currentNodeMetadata.ephemeralOwner === 0 ? 1 : 10
    };
    const result = await PinotMethodUtils.putNodeData(nodeData);
    if(result.data.status){
      dispatch({
        type: 'success',
        message: result.data.status,
        show: true
      });
      showInfoEvent(selectedNode);
    } else {
      dispatch({
        type: 'error',
        message: result.data.error,
        show: true
      });
    }
    closeDialog();
  };

  const deleteNode = async () => {
    const parentPath = selectedNode.split('/').slice(0, selectedNode.split('/').length-1).join('/');
    const treeObj = Utils.findNestedObj(treeData, 'fullPath', parentPath);
    const result = await PinotMethodUtils.deleteNode(selectedNode);
    if(result.data.status){
      dispatch({
        type: 'success',
        message: result.data.status,
        show: true
      });
      showInfoEvent(selectedNode);
      fetchInnerPath(treeObj);
    } else {
      dispatch({
        type: 'error',
        message: result.data.error,
        show: true
      });
    }
    closeDialog();
  };

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogContent(null);
    setDialogTitle(null);
    setDialogYesLabel(null);
    setDialogNoLabel(null);
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
                  <Button onClick={(e)=>{showInfoEvent(selectedNode);}}><RefreshOutlinedIcon /></Button>
                </Tooltip>
                <Tooltip title="Add">
                  <Button onClick={handleClick}><NoteAddOutlinedIcon /></Button>
                </Tooltip>
                <Tooltip title="Delete" open={false}>
                  <Button onClick={handleDeleteClick} disabled={!isLeafNodeSelected}><DeleteOutlineOutlinedIcon /></Button>
                </Tooltip>
                <Tooltip title="Edit" open={false}>
                  <Button onClick={handleEditClick} disabled={!isLeafNodeSelected}><EditOutlinedIcon /></Button>
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
      <Confirm
        openDialog={confirmDialog}
        dialogTitle={dialogTitle}
        dialogContent={dialogContent}
        successCallback={dialogSuccessCb}
        closeDialog={closeDialog}
        dialogYesLabel={dialogYesLabel}
        dialogNoLabel={dialogNoLabel}
      />
    </>
  );
};

export default TreeDirectory;