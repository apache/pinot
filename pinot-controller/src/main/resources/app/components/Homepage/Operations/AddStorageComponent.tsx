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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, Theme, Tooltip} from '@material-ui/core';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 170
    }
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function
};

export default function AddStorageComponent({
  tableObj,
  setTableObj
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
      case 'retentionTimeUnit':
        newTableObj.segmentsConfig.retentionTimeUnit = value;
      break;
      case 'retentionTimeValue':
        newTableObj.segmentsConfig.retentionTimeValue = value;
      break;
      case 'maxQueriesPerSecond':
        newTableObj.quota.storage = value;
      break;
    };
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
    setTableDataObj(tableObj);
  }, [tableObj]);

  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
      <Tooltip title="Data will be deleted after this time period." arrow placement="top-start">
      <FormControl className={classes.formControl} >
          <InputLabel htmlFor="retentionTimeValue">Retention Value</InputLabel>
          <Input
            id="retentionTimeValue"
            key="retentionTimeValue"
            value={tableDataObj.segmentsConfig.retentionTimeValue || ""}
            onChange={(e)=>
                changeHandler('retentionTimeValue', e.target.value)
            }
            type="number"
        />
        </FormControl>
        </Tooltip>
        <FormControl className={classes.selectFormControl}>
          <InputLabel htmlFor="retentionTimeUnit">Retention unit</InputLabel>
          <Select
            labelId="retentionTimeUnit"
            id="retentionTimeUnit"
            key="retentionTimeUnit"
            value={tableDataObj.segmentsConfig.retentionTimeUnit && tableDataObj.segmentsConfig.retentionTimeUnit !== "" ? tableDataObj.segmentsConfig.retentionTimeUnit : ''}
            onChange={(e)=>
                changeHandler('retentionTimeUnit', e.target.value)
            }
          >
            <MenuItem value="HOURS">HOURS</MenuItem>
            <MenuItem value="DAYS">DAYS</MenuItem>
          </Select>
        </FormControl>
        <Tooltip title="Data pushes which exceed this size will fail." arrow placement="top-start">
        <FormControl className={classes.formControl} >
          <InputLabel htmlFor="maxQueriesPerSecond">Storage Quota</InputLabel>
          <Input
            id="maxQueriesPerSecond"
            key="maxQueriesPerSecond"
            value={tableDataObj?.quota?.storage || ""}
            onChange={(e)=>
                changeHandler('maxQueriesPerSecond', e.target.value)
            }
          />
        </FormControl>
        </Tooltip>
      </Grid>
    </Grid>
  );
}