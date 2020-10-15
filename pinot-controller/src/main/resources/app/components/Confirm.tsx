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

import React, { useEffect } from 'react';
import { Button, Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions, makeStyles } from '@material-ui/core';
import { green, red } from '@material-ui/core/colors';

const useStyles = makeStyles((theme) => ({
  dialogContent: {
    minWidth: 900
  },
  dialogTextContent: {
    fontWeight: 600
  },
  dialogActions: {
    justifyContent: 'center'
  },
  green: {
    fontWeight: 600,
    color: green[500],
    borderColor: green[500],
    '&:hover': {
      backgroundColor: green[50],
      borderColor: green[500]
    }
  },
  red: {
    fontWeight: 600,
    color: red[500],
    borderColor: red[500],
    '&:hover': {
      backgroundColor: red[50],
      borderColor: red[500]
    }
  }
}));


type Props = {
  openDialog: boolean,
  dialogTitle?: string,
  dialogContent: string,
  successCallback: (event: React.MouseEvent<HTMLButtonElement, MouseEvent>) => void;
  closeDialog: (event: React.MouseEvent<HTMLButtonElement, MouseEvent>) => void;
  dialogYesLabel?: string,
  dialogNoLabel?: string
};

const Confirm = ({openDialog, dialogTitle, dialogContent, successCallback, closeDialog, dialogYesLabel, dialogNoLabel}: Props) => {
  const classes = useStyles();
  const [open, setOpen] = React.useState(openDialog);

  useEffect(()=>{
    setOpen(openDialog);
  }, [openDialog]);

  const isStringDialog = typeof dialogContent === 'string';

  return (
    <div>
      <Dialog
        open={open}
        onClose={closeDialog}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
        maxWidth={false}
      >
        {dialogTitle && <DialogTitle id="alert-dialog-title">{dialogTitle}</DialogTitle>}
        <DialogContent className={`${!isStringDialog ? classes.dialogContent : ''}`}>
          {isStringDialog ?
            <DialogContentText id="alert-dialog-description" className={classes.dialogTextContent}>
              {dialogContent}
            </DialogContentText>
            : dialogContent}
        </DialogContent>
        <DialogActions style={{paddingBottom: 20}} className={`${isStringDialog ? classes.dialogActions : ''}`}>
          <Button variant="outlined" onClick={closeDialog} color="secondary" className={classes.red}>
            {dialogNoLabel || 'No'}
          </Button>
          <Button variant="outlined" onClick={successCallback} color="primary" autoFocus className={classes.green}>
            {dialogYesLabel || 'Yes'}
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
};

export default Confirm;