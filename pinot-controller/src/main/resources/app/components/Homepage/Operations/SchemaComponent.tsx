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
import { createStyles, FormControl, Grid, IconButton, Input, InputLabel, makeStyles, MenuItem, Select, TextField, Theme} from '@material-ui/core';
import AddIcon from '@material-ui/icons/Add';
import ClearIcon from '@material-ui/icons/Clear';
import { Autocomplete } from '@material-ui/lab';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    rootContainer:{
      padding: '0 15px 15px'
    },
    iconDiv:{
      marginLeft: 'auto'
    },
    dateTimeDiv:{
      marginLeft: '20px',
      paddingLeft: '20px',
      borderLeft: '1px #ccc solid'
    },
    gridItem:{
      display: 'flex',
      alignItems: 'center',
      marginTop: '15px',
      padding: '5px',
      border: '1px #ccc solid',
      borderRadius: 5
    },
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 170
    },
    MVFFormControl: {
      margin: theme.spacing(1),
      width: 126
    },
    autoCompleteControl: {
      '& .MuiFormControl-marginNormal': {
        marginTop: 0
      }
    },
    greenColor: {
      color: theme.palette.success.main
    },
    redColor: {
      color: theme.palette.error.main
    },
  })
);

type Props = {
  schemaName: string,
  setSchemaObj: Function
};

export default function SchemaComponent({
  schemaName = '',
  setSchemaObj
}: Props) {
  const classes = useStyles();
  const defaultColumnObj = {
    columnName: '',
    type: '',
    dataType: '',
    multiValue: 'false',
    maxLength: '',
    virtualColumnProvider: '',
    defaultNullValue: '',
    timeUnit: '',
    timeFormat: '',
    timePattern: '',
    granularityUnit: '',
    granularityInterval: ''
  };
  const defaultSchemaObj = {
    schemaName,
    dimensionFieldSpecs: [],
    metricFieldSpecs: [],
    dateTimeFieldSpecs: []
  };
  const defaultDataTypeOptions = {
    dimension: ["INT", "LONG", "STRING", "FLOAT", "DOUBLE", "BYTES", "BOOLEAN"],
    metric: ["INT", "LONG", "DOUBLE", "FLOAT", "BYTES"],
    datetime: ["STRING", "INT", "LONG"]
  };
  
  const [columnList, setColumnList] = useState([{...defaultColumnObj}]);

  const changeHandler = (index, fieldName, value) => {
    let newColumnList = [...columnList];
    newColumnList[index][fieldName] = value;
    setColumnList(newColumnList);
  };

  useEffect(() => {
    let newSchemaObj = {...defaultSchemaObj};
    newSchemaObj.schemaName = schemaName;
    columnList.map((columnObj)=>{
      const {columnName, dataType, defaultNullValue, multiValue, maxLength, virtualColumnProvider,
        timeUnit, timeFormat, timePattern, granularityUnit, granularityInterval
      } = columnObj;
      let schemaColumnnObj = {
        name: columnName,
        dataType: dataType,
        format: undefined,
        granularity: undefined,
        defaultNullValue: defaultNullValue !== '' ? defaultNullValue : undefined,
        maxLength: maxLength !== '' ? parseInt(maxLength,10) : undefined,
        virtualColumnProvider: virtualColumnProvider !== '' ? (virtualColumnProvider === 'true') : undefined,
        singleValueField: undefined
      };
      switch(columnObj.type){
        case 'dimension':
          schemaColumnnObj.singleValueField = multiValue === 'true' ? false : undefined;
          newSchemaObj.dimensionFieldSpecs.push(schemaColumnnObj);
        break;
        case 'metric':
          newSchemaObj.metricFieldSpecs.push(schemaColumnnObj);
        break;
        case 'datetime':
          if(timeUnit && timeFormat){
            schemaColumnnObj.format = `1:${timeUnit}:${timeFormat}${timeFormat!=='EPOCH' ? `:${timePattern}` : ''}`;
          }
          if(granularityUnit && granularityInterval){
            schemaColumnnObj.granularity = `${granularityInterval}:${granularityUnit}`;
          }
          newSchemaObj.dateTimeFieldSpecs.push(schemaColumnnObj);
        break;
      };
    });
    setTimeout(()=>{setSchemaObj(newSchemaObj);},0);
  }, [columnList]);

  const addNewColumn = () => {
    let newColumnList = [...columnList];
    newColumnList.push({...defaultColumnObj});
    setColumnList(newColumnList);
  };

  const deleteColumn = (index) => {
    let newColumnList = [...columnList];
    newColumnList.splice(index, 1);
    setColumnList(newColumnList);
  }

  const requiredAstrix = <span className={classes.redColor}>*</span>;
  return (
    <Grid container className={classes.rootContainer}>
      {columnList.map((columnObj, index)=>{
        return(
          <Grid item xs={12} key={index} className={classes.gridItem}>
            <div>
              <FormControl className={classes.formControl}>
                <InputLabel htmlFor="columnName">Column Name {requiredAstrix}</InputLabel>
                <Input
                  id="columnName"
                  value={columnObj.columnName}
                  onChange={(e)=> changeHandler(index, 'columnName', e.target.value)}
                />
              </FormControl>
              
              <FormControl className={classes.formControl}>
                <InputLabel htmlFor="type">Type {requiredAstrix}</InputLabel>
                <Select
                  labelId="type"
                  id="type"
                  value={columnObj.type}
                  onChange={(e)=> changeHandler(index, 'type', e.target.value)}
                >
                  <MenuItem value="dimension">Dimension</MenuItem>
                  <MenuItem value="metric">Metric</MenuItem>
                  <MenuItem value="datetime">DateTime</MenuItem>
                </Select>
              </FormControl>

              <FormControl className={classes.formControl} disabled={!columnObj.type}>
                <InputLabel htmlFor="dataType">Data Type {requiredAstrix}</InputLabel>
                <Select
                  labelId="dataType"
                  id="dataType"
                  value={columnObj.dataType}
                  onChange={(e)=> changeHandler(index, 'dataType', e.target.value)}
                >
                  {defaultDataTypeOptions[columnObj.type || 'dimension'].map((dataType, i)=>(<MenuItem key={i} value={dataType}>{dataType}</MenuItem>))}
                </Select>
              </FormControl>

              <FormControl className={classes.formControl}>
                <InputLabel htmlFor="defaultNullValue">Default Null Value</InputLabel>
                <Input
                  id="defaultNullValue"
                  value={columnObj.defaultNullValue}
                  onChange={(e)=> changeHandler(index, 'defaultNullValue', e.target.value)}
                />
              </FormControl>

              {columnObj.type === 'dimension' &&
                <FormControl className={classes.MVFFormControl}>
                  <InputLabel htmlFor="multiValue">Multi Value Field</InputLabel>
                  <Select
                    labelId="multiValue"
                    id="multiValue"
                    value={columnObj.multiValue}
                    onChange={(e)=> changeHandler(index, 'multiValue', e.target.value)}
                  >
                    <MenuItem value="true">True</MenuItem>
                    <MenuItem value="false">False</MenuItem>
                  </Select>
                </FormControl>
              }

              <br/>
              
              <FormControl className={classes.formControl}>
                <InputLabel htmlFor="maxLength">Max Value Length</InputLabel>
                <Input
                  id="maxLength"
                  type="number"
                  value={columnObj.maxLength}
                  onChange={(e)=> changeHandler(index, 'maxLength', e.target.value)}
                />
              </FormControl>

              <FormControl className={classes.selectFormControl}>
                <Autocomplete
                  className={classes.autoCompleteControl}
                  options={["True", "False"]}
                  onChange={(e, value)=> changeHandler(index, 'virtualColumnProvider', value ? value.toLowerCase() : "")}
                  renderInput={(params) => (
                    <TextField {...params} label="Virtual Column" margin="normal"/>
                  )}
                />
              </FormControl>
            </div>

            {columnObj.type === 'datetime' &&
              <div className={classes.dateTimeDiv}>
                <FormControl className={classes.formControl}>
                  <InputLabel htmlFor="timeUnit">Time Unit {requiredAstrix}</InputLabel>
                  <Select
                    labelId="timeUnit"
                    id="timeUnit"
                    value={columnObj.timeUnit}
                    onChange={(e)=> changeHandler(index, 'timeUnit', e.target.value)}
                  >
                    <MenuItem value="MILLISECONDS">MILLISECONDS</MenuItem>
                    <MenuItem value="SECONDS">SECONDS</MenuItem>
                    <MenuItem value="MINUTES">MINUTES</MenuItem>
                    <MenuItem value="HOURS">HOURS</MenuItem>
                    <MenuItem value="DAYS">DAYS</MenuItem>
                  </Select>
                </FormControl>
                
                <FormControl className={classes.formControl}>
                  <InputLabel htmlFor="timeFormat">Time Format {requiredAstrix}</InputLabel>
                  <Select
                    labelId="timeFormat"
                    id="timeFormat"
                    value={columnObj.timeFormat}
                    onChange={(e)=> changeHandler(index, 'timeFormat', e.target.value)}
                  >
                    <MenuItem value="EPOCH">EPOCH</MenuItem>
                    <MenuItem value="SIMPLE_DATE_FORMAT">SIMPLE_DATE_FORMAT</MenuItem>
                  </Select>
                </FormControl>

                {columnObj.timeFormat === 'SIMPLE_DATE_FORMAT' &&
                  <FormControl className={classes.formControl}>
                    <InputLabel htmlFor="timePattern">Time Pattern {requiredAstrix}</InputLabel>
                    <Input
                      id="timePattern"
                      value={columnObj.timePattern}
                      onChange={(e)=> changeHandler(index, 'timePattern', e.target.value)}
                    />
                  </FormControl>
                }
                <br/>
                <FormControl className={classes.formControl}>
                  <InputLabel htmlFor="granularityInterval">Granularity Interval {requiredAstrix}</InputLabel>
                  <Input
                    id="granularityInterval"
                    value={columnObj.granularityInterval}
                    onChange={(e)=> changeHandler(index, 'granularityInterval', e.target.value)}
                  />
                </FormControl>

                <FormControl className={classes.selectFormControl}>
                  <InputLabel htmlFor="granularityUnit">Granularity Unit {requiredAstrix}</InputLabel>
                  <Select
                    labelId="granularityUnit"
                    id="granularityUnit"
                    value={columnObj.granularityUnit}
                    onChange={(e)=> changeHandler(index, 'granularityUnit', e.target.value)}
                  >
                    <MenuItem value="MILLISECONDS">MILLISECONDS</MenuItem>
                    <MenuItem value="SECONDS">SECONDS</MenuItem>
                    <MenuItem value="MINUTES">MINUTES</MenuItem>
                    <MenuItem value="HOURS">HOURS</MenuItem>
                    <MenuItem value="DAYS">DAYS</MenuItem>
                  </Select>
                </FormControl>
              </div>
            }
            <div className={classes.iconDiv}>
              <FormControl>
                <IconButton
                  aria-label="plus"
                  onClick={addNewColumn}
                >
                  <AddIcon className={classes.greenColor}/>
                </IconButton>
              </FormControl>
              
              {columnList.length >= 2 && <FormControl>
                <IconButton
                  aria-label="clear"
                  onClick={() => deleteColumn(index)}
                >
                  <ClearIcon className={classes.redColor}/>
                </IconButton>
              </FormControl>}
            </div>
          </Grid>
        );
      })}
    </Grid>
  );
}