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

import React, {ReactNode} from 'react';
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  makeStyles,
  withStyles
} from '@material-ui/core';
import { red } from '@material-ui/core/colors';

const useStyles = makeStyles((theme) => ({
  root: {
    '& .MuiDialog-container > .MuiPaper-root':{
      minWidth: '600px'
    },
    "& .MuiDialogContent-root": {
      padding: '10px 24px',
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
  title: any,
  children: any,
  btnCancelText?: string,
  btnOkText?: string,
  showCancelBtn?: boolean,
  showOkBtn?: boolean,
  size?: false | "xs" | "sm" | "md" | "lg" | "xl",
  disableBackdropClick?: boolean,
  disableEscapeKeyDown?: boolean,
  showTitleDivider?: boolean,
  showFooterDivider?: boolean,
  moreActions?: ReactNode
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
  size,
  disableBackdropClick = false,
  disableEscapeKeyDown = false,
  showTitleDivider = false,
  showFooterDivider = false,
  moreActions
}: Props) {

  const classes = useStyles();

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      aria-labelledby="form-dialog-title"
      className={classes.root}
      maxWidth={size}
      fullWidth={size ? true : false}
      disableBackdropClick={disableBackdropClick}
      disableEscapeKeyDown={disableEscapeKeyDown}
    >
      <DialogTitle className={classes.dialogTitle}>{title}</DialogTitle>
      {showTitleDivider && <Divider style={{ marginBottom: 10 }} />}
      <DialogContent>
        {children}
      </DialogContent>
      {showFooterDivider && <Divider style={{ marginBottom: 10 }} />}
      <DialogActions>
        {showCancelBtn &&
        <CancelButton onClick={handleClose}  style={{ textTransform: 'none' }} variant="outlined">
          {btnCancelText || 'Cancel'}
        </CancelButton>}
        {moreActions}
        {showOkBtn &&
        <Button onClick={handleSave} variant="contained" style={{ textTransform: 'none' }} color="primary">
          {btnOkText || 'Save'}
        </Button>}
      </DialogActions>
    </Dialog>
  );
}