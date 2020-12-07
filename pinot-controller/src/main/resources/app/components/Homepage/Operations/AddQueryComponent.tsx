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

import React, { useEffect, useState } from 'react';
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, Theme, Tooltip} from '@material-ui/core';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    }
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function
};

export default function AddQueryComponent({
  tableObj,
  setTableObj
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
        case 'timeoutMs':
          newTableObj.query[fieldName] = value;
        break;
        case 'maxQueriesPerSecond':
          newTableObj.quota[fieldName] = value;
        break;
    }
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
    setTableDataObj(tableObj);
  }, [tableObj]);

  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
        <FormControl className={classes.formControl} >
          <InputLabel htmlFor="timeoutMs">Query Timeout Ms</InputLabel>
          <Input
            id="timeoutMs"
            key="timeoutMs"
            value={tableDataObj.query.timeoutMs || ""}
            onChange={(e)=> changeHandler('timeoutMs', e.target.value)}
            type="number"
          />
        </FormControl>
        <Tooltip title="Queries exceeding this QPS will be rejected." arrow placement="top-start" disableHoverListener={tableDataObj.tableType === "REALTIME"}>
        <FormControl className={classes.formControl} >
          <InputLabel htmlFor="maxQueriesPerSecond">Queries Per Second</InputLabel>
          <Input
            id="maxQueriesPerSecond"
            key="maxQueriesPerSecond"
            value={tableDataObj.quota.maxQueriesPerSecond || ""}
            onChange={(e)=> changeHandler('maxQueriesPerSecond', e.target.value)}
            type="number"
          />
        </FormControl>
        </Tooltip>
      </Grid>
    </Grid>
  );
}