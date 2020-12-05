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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, TextField, Theme} from '@material-ui/core';
import { Autocomplete } from '@material-ui/lab';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 170
    },
    autoCompleteControl: {
      '& .MuiFormControl-marginNormal': {
        marginTop: 0
      }
    },
    redColor: {
      color: theme.palette.error.main
    }
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function,
  dateTimeFieldSpecs: Array<any>
  disable:boolean
};

export default function AddTableComponent({
  tableObj,
  setTableObj,
  dateTimeFieldSpecs,
  disable
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);
  const [dateFields, setDateFields] = useState([]);
  const [timeColumn, setTimeColumn] = useState('');

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
      case 'tableName':
        newTableObj[fieldName] = value;
        newTableObj.segmentsConfig.schemaName = value;
      break;
      case 'tableType':
        newTableObj[fieldName] = value;
      break;
      case 'timeColumnName':
        setTimeColumn(value);
        newTableObj.segmentsConfig[fieldName] = value || null;
      break;
      case 'replication':
        newTableObj.segmentsConfig[fieldName] = value;
        newTableObj.segmentsConfig['replicasPerPartition'] = value;
      break;
    };
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
    setTableDataObj(tableObj);
  }, [tableObj]);

  useEffect(()=>{
    let newTableObj = {...tableDataObj};
    let colName = dateTimeFieldSpecs.length ? dateTimeFieldSpecs[0].name : '';
    let dateOptions = [];
    dateTimeFieldSpecs.map((field)=>{
      dateOptions.push(field.name);
    });
    setDateFields(dateOptions);
    if(!timeColumn){
      setTimeColumn(colName);
      newTableObj.segmentsConfig.timeColumnName = colName || null;
      setTableDataObj(newTableObj);
      setTableObj(newTableObj);
    } else {
      const isDatetimeColAvailable = dateTimeFieldSpecs.find((field)=>{return field.name === timeColumn;});
      if(!isDatetimeColAvailable){
        setTimeColumn(colName);
        newTableObj.segmentsConfig.timeColumnName = colName || null;
        setTableDataObj(newTableObj);
        setTableObj(newTableObj);
      }
    }
  }, [dateTimeFieldSpecs]);

  const requiredAstrix = <span className={classes.redColor}>*</span>;
  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
        <FormControl className={classes.formControl}>
          <InputLabel htmlFor="tableName">Table Name {requiredAstrix}</InputLabel>
          <Input
            id="tableName"
            value={tableDataObj.tableName}
            onChange={(e)=> changeHandler('tableName', e.target.value)}
          />
        </FormControl>
        
        <FormControl className={classes.selectFormControl}>
          <InputLabel htmlFor="tableType">Table Type {requiredAstrix}</InputLabel>
          <Select
            labelId="tableType"
            id="tableType"
            value={tableDataObj.tableType}
            onChange={(e)=> changeHandler('tableType', e.target.value)}
            disabled={disable}
          >
            <MenuItem value="OFFLINE">OFFLINE</MenuItem>
            <MenuItem value="REALTIME">REALTIME</MenuItem>
          </Select>
        </FormControl>
        <FormControl className={classes.selectFormControl}>
          <Autocomplete
            className={classes.autoCompleteControl}
            value={timeColumn}
            options={dateFields}
            onChange={(e, value)=> changeHandler('timeColumnName', value ? value: '')}
            disableClearable={true}
            autoHighlight={true}
            renderInput={(params) => (
              <TextField
                {...params}
                label={<>Time Column Name{tableDataObj.tableType === 'REALTIME' && requiredAstrix}</>}
                margin="normal"
              />
            )}
          />
        </FormControl>

        <FormControl className={classes.selectFormControl}>
          <InputLabel htmlFor="replication">Replication</InputLabel>
          <Select
            labelId="replication"
            id="replication"
            value={tableDataObj.segmentsConfig.replication}
            onChange={(e)=> changeHandler('replication', e.target.value)}
          >
            {[ ...Array(20).keys() ].map((num, index)=>(<MenuItem key={index} value={`${num+1}`}>{`${num+1}`}</MenuItem>))}
          </Select>
        </FormControl>
      </Grid>
    </Grid>
  );
}