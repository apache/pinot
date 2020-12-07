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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, TextField, Theme, Tooltip} from '@material-ui/core';
import { Autocomplete } from '@material-ui/lab';
import InfoIcon from '@material-ui/icons/Info';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 305
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
  setTableObj: Function
};

export default function AddTenantComponent({
  tableObj,
  setTableObj
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);
  const [tenantServer,setTenantServer] = useState('');
  const [tenantBroker,setTenantBroker] = useState('');
  const [serverOptions,setServerOptions] = useState([]);
  const [brokerOptions,setBrokerOptions] = useState([]);
  const [showRealtimeCompleted,setShowRealtimeCompleted] = useState(0);

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
        case 'broker':
            setTenantBroker(value);
            newTableObj.tenants.broker = value;
        break;
        case 'server':
            setTenantServer(value);
            newTableObj.tenants.server = value;
        break;
        case 'tagOverrideConfig':
            setShowRealtimeCompleted(value);
            if(value){
                newTableObj.tenants.tagOverrideConfig = {};
                newTableObj.tenants.tagOverrideConfig.realtimeCompleted = `${tenantServer}_OFFLINE`;
            }else{
                delete newTableObj.tenants.tagOverrideConfig;
            }
            break;
        case 'showRealtimeCompleted':
            newTableObj.tenants.tagOverrideConfig.realtimeCompleted = value;
        break;
        }
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
      if(!(tableDataObj.tenants.tagOverrideConfig && tableDataObj.tenants.tagOverrideConfig.realtimeCompleted)){
        setShowRealtimeCompleted(0);
      }
      setTableDataObj(tableObj);
  }, [tableObj]);


  useEffect(()=>{
      let serverOptions = [],
      brokerOptions = [];
      setServerOptions(serverOptions);
      setBrokerOptions(brokerOptions);
      setTenantServer(tableDataObj.tenants.server);
      setTenantBroker(tableDataObj.tenants.broker);
  },[])

  const requiredAstrix = <span className={classes.redColor}>*</span>;
  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
        <FormControl className={classes.selectFormControl}>
          <Autocomplete
            key={'server'}
            className={classes.autoCompleteControl}
            value={tenantServer}
            options={serverOptions}
            onChange={(e, value)=> changeHandler('server', value ? value: '')}
            disableClearable={true}
            autoHighlight={true}
            renderInput={(params) => (
              <TextField
                {...params}
                label = {<>Server Tenant</>}
                margin="normal"
              />
            )}
          />
        </FormControl>
        <FormControl className={classes.selectFormControl}>
          <Autocomplete
            key={'broker'}
            className={classes.autoCompleteControl}
            value={tenantBroker}
            options={brokerOptions}
            onChange={(e, value)=> changeHandler('broker', value ? value: '')}
            disableClearable={true}
            autoHighlight={true}
            renderInput={(params) => (
              <TextField
                {...params}
                label = {<>Broker Tenant</>}
                margin="normal"
              />
            )}
          />
        </FormControl>
        <FormControl className={classes.selectFormControl}>
          <InputLabel htmlFor="tagOverrideConfig">Relocate realtime completed segments?</InputLabel>
          <Select
            labelId="tagOverrideConfig"
            id="tagOverrideConfig"
            value={showRealtimeCompleted}
            onChange={(e)=> changeHandler('tagOverrideConfig', e.target.value)}
          >
            <MenuItem value={1}>True</MenuItem>
            <MenuItem value={0}>False</MenuItem>
          </Select>
        </FormControl>
        { showRealtimeCompleted ?
        <Tooltip interactive title={(<><a href="https://docs.pinot.apache.org/operators/operating-pinot/tuning/realtime#moving-completed-segments-to-different-hosts" target="_blank" className={"tooltip-link"}>(Click here for more Details)</a></>)} arrow placement="top-start" disableHoverListener={tableDataObj.tableType === "REALTIME"}>
        <FormControl className={classes.formControl}>
          <InputLabel htmlFor="realtimeCompleted">Relocate to tag</InputLabel>
          <Input
            id="realtimeCompleted"
            value={tableDataObj.tenants.tagOverrideConfig.realtimeCompleted}
            onChange={(e)=> changeHandler('showRealtimeCompleted', e.target.value)}
          />
        </FormControl>
        </Tooltip> : null}
      </Grid>
    </Grid>
  );
}