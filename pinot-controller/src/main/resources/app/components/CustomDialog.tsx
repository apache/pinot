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
import { Button, Dialog, DialogActions, DialogTitle, makeStyles, withStyles } from '@material-ui/core';
import { red } from '@material-ui/core/colors';

const useStyles = makeStyles((theme) => ({
  root: {
    '& .MuiDialog-container > .MuiPaper-root':{
      minWidth: '600px'
    }
  },
  dialogTitle: {
    padding: '10px 24px',
  }
}));

const CancelButton = withStyles(() => ({
  root: {
    color: red[500],
    borderColor: red[500],
    '&:hover': {
      borderColor: red[700],
    },
  },
}))(Button);

type Props = {
  open: boolean,
  handleClose: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  handleSave?: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  title: string,
  children: any,
  btnCancelText?: string,
  btnOkText?: string,
  showCancelBtn?: boolean,
  showOkBtn?: boolean,
  largeSize?: boolean
};

export default function CustomDialog({
  open,
  handleClose,
  handleSave,
  title,
  children,
  btnCancelText,
  btnOkText,
  showCancelBtn = true,
  showOkBtn = true,
  largeSize = false
}: Props) {

  const classes = useStyles();

  return (
    <Dialog open={open} onClose={handleClose} aria-labelledby="form-dialog-title" className={classes.root} maxWidth={largeSize ? "lg" : false} fullWidth={largeSize}>
      <DialogTitle className={classes.dialogTitle}>{title}</DialogTitle>
      {children}
      <DialogActions>
        {showCancelBtn &&
        <CancelButton onClick={handleClose} variant="outlined">
          {btnCancelText || 'Cancel'}
        </CancelButton>}
        {showOkBtn &&
        <Button onClick={handleSave} variant="outlined" color="primary">
          {btnOkText || 'Save'}
        </Button>}
      </DialogActions>
    </Dialog>
  );
}