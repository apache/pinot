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
import { CircularProgress, createStyles, DialogContent, makeStyles, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Theme, withStyles} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import CloseIcon from '@material-ui/icons/Close';
import CheckIcon from '@material-ui/icons/Check';
import { red } from '@material-ui/core/colors';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'
    },
    container: {
      maxHeight: 540,
    },
    greenColor: {
      color: theme.palette.success.main
    },
    redColor: {
      color: theme.palette.error.main
    },
  })
);

const StyledTableCell = withStyles((theme: Theme) =>
  createStyles({
    head: {
      backgroundColor: '#ecf3fe',
      color: theme.palette.primary.main,
      fontWeight: 600
    }
  }),
)(TableCell);

type Props = {
  data: any,
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void
};

export default function ReloadStatusOp({
  data,
  hideModal
}: Props) {
  const classes = useStyles();
  const segmentNames = data && Object.keys(data);
  const indexes = data && data[segmentNames[0]]?.indexes;
  const indexesKeys = indexes && Object.keys(indexes);
  const indexObjKeys = indexes && indexes[indexesKeys[0]] && Object.keys(indexes[indexesKeys[0]]);
  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      title="Reload Status"
      showOkBtn={false}
      largeSize={true}
    >
      {!data ?
        <div className={classes.root}><CircularProgress/></div>
      :
        <DialogContent>
          <TableContainer component={Paper} className={classes.container}>
            <Table stickyHeader aria-label="sticky table" size="small">
              <TableHead>
                <TableRow>
                  <StyledTableCell></StyledTableCell>
                  {indexObjKeys.map((o, i)=>{
                    return (
                      <StyledTableCell key={i} align="right">{o}</StyledTableCell>
                    );
                  })}
                </TableRow>
              </TableHead>
              <TableBody>
                {indexesKeys.map((indexName, i) => {
                  const indexObj = indexes[indexName];
                  return (
                    <TableRow key={i}>
                      <StyledTableCell component="th" scope="row">
                        {indexName}
                      </StyledTableCell>
                      {indexObjKeys.map((o, i)=>{
                        let iconElement = null;
                        if(indexObj[o].toLowerCase() === 'yes'){
                          iconElement = <CheckIcon className={classes.greenColor}/>;
                        } else if(indexObj[o].toLowerCase() === 'no'){
                          iconElement = <CloseIcon className={classes.redColor}/>;
                        } else {
                          iconElement = indexObj[o];
                        }
                        return (
                          <StyledTableCell align="center" key={i}>
                            {iconElement}
                          </StyledTableCell>
                        )
                      })}
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </DialogContent>
      }
    </Dialog>
  );
}