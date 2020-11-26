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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, Theme} from '@material-ui/core';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    rootContainer:{
      padding: '0 15px 15px'
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
      width: 300
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
  setSchemaObj: Function,
  schemaName: string,
  schemaObj:any
};

export default function SchemaNameComponent({
  setSchemaObj,
  schemaName,
  schemaObj
}: Props) {
  const classes = useStyles();
  const [schemaObjTemp,setSchemaObjTemp] = useState(schemaObj);

  const changeHandler = (value) =>{
    let newSchemaObj = {...schemaObjTemp};
    newSchemaObj.schemaName = value;
    setSchemaObj(newSchemaObj);
  }


  useEffect(()=>{
    let newSchemaObj = {...schemaObjTemp};
    newSchemaObj.schemaName = schemaName;
    setSchemaObjTemp(newSchemaObj);
  },[schemaName])

  const requiredAstrix = <span className={classes.redColor}>*</span>;
  return (
    <Grid container className={classes.rootContainer}>
          <Grid item xs={12} key={"schemaName"}>
              <FormControl className={classes.formControl}>
                <InputLabel htmlFor="schemaName">Schema Name {requiredAstrix}</InputLabel>
                <Input
                  id="schemaName"
                  value={schemaObjTemp.schemaName}
                  onChange={(e)=> changeHandler(e.target.value)}
                />
              </FormControl>
          </Grid>
    </Grid>
  );
}