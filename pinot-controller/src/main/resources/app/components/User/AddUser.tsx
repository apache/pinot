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

import React, { useState } from 'react';
import {
  Checkbox,
  DialogContent,
  FormControl,
  Input,
  InputLabel, ListItemText,
  MenuItem,
  Select,
  TextField,
  InputAdornment,
  IconButton,
} from '@material-ui/core';
import Visibility from '@material-ui/icons/Visibility';
import VisibilityOff from '@material-ui/icons/VisibilityOff';
import {cloneDeep} from "lodash";
type Props = {
  tableList: Array<{
    name: string,
    checked: boolean,
    disabled: boolean
  }>,
  setUserInfo: Function,
  userProp?: {
    username: string,
    password: string,
    component: string,
    role: string,
    tables: Array<string>,
    permissions: Array<string>
  }
}

export default function AddUser({
  tableList,
  setUserInfo,
  userProp
}: Props) {
  const PERMISSIONS = ["READ", "CREATE", "UPDATE", "DELETE"];
  const [user, setUser] = useState({
      ...userProp,
      tables: ["DUAL"]
  });
  const [tables, setTables] = useState(tableList);
  const [showPassword, setShowPassword] = useState(false);
  const handleClickShowPassword = () => {
    setShowPassword(!showPassword);
  };
  const handleMouseDownPassword = (event) => {
    event.preventDefault();
  };
  const changeHandler = (field, e)=>{
    let val = e.target.value;
    let userCopy = cloneDeep(user);
    switch (field) {
      case 'username':
        userCopy.username = val;
        break;
      case 'password':
        userCopy.password = val;
        break;
      case 'component':
        userCopy.component = val;
        break;
      case 'role':
        userCopy.role = val;
        break;
      case 'permissions':
        userCopy.permissions = val;
        break;
    }
    setUser(userCopy);
    setUserInfo(userCopy);
  }
  const tableCheckChange = (event)=>{
    let name = event.target.name;
    let checked = event.target.checked;
    if(name){
      let table = cloneDeep(tables);
      if(name === 'ALL' && checked === true){
        table = table.map(item =>{
          return {
            ...item,
            checked: item.name === 'ALL',
            disabled: item.name !== 'ALL'
          }
        })
      } else if(name === 'ALL' && checked === false){
        table = table.map(item =>{
          return {
            ...item,
            checked: item.name === 'ALL' ? false: item.checked,
            disabled: false
          }
        })
      } else{
        table = table.map(item =>{
          return {
            ...item,
            checked: item.name === name ? checked: item.checked
          }
        })
      }
      setTables(table);
      let checkedCollection = [];
      for(let item of table){
        if(item.checked){
          checkedCollection.push(item.name);
        }
      }
      let newUser = {
          ...user,
        tables: checkedCollection
      }
      setUser(newUser)
      setUserInfo(newUser);
    }
  }
  const menuItemStyle = {
    fontSize: '16px',
  }
  return (
      <DialogContent style={{fontSize: '14px'}}>
        <TextField
            autoFocus
            margin="dense"
            id="name"
            label="User Name"
            fullWidth
            value={user.username}
            onChange={(e)=>changeHandler('username', e)}
        />
        <InputLabel htmlFor="standard-adornment-password" style={{marginTop: '10px'}}>Password</InputLabel>
        <Input
            id="standard-adornment-password"
            type={showPassword ? 'text' : 'password'}
            autoComplete='new-password'
            value={user.password}
            onChange={(e)=>changeHandler('password', e)}
            endAdornment={
              <InputAdornment position="end">
                <IconButton
                    aria-label="toggle password visibility"
                    onClick={handleClickShowPassword}
                    onMouseDown={handleMouseDownPassword}
                >
                  {showPassword ? <Visibility /> : <VisibilityOff />}
                </IconButton>
              </InputAdornment>
            }
        />
        <FormControl fullWidth style={{margin: '10px 0'}}>
          <InputLabel id="component">Component</InputLabel>
          <Select
              autoWidth={true}
              labelId="component"
              id="component"
              value={user.component}
              label="Component"
              required
              onChange={(e)=>changeHandler('component', e)}

          >
            <MenuItem value={'CONTROLLER'} style={menuItemStyle}>CONTROLLER</MenuItem>
            <MenuItem value={'BROKER'} style={menuItemStyle}>BROKER</MenuItem>
            <MenuItem value={'SERVER'} style={menuItemStyle}>SERVER</MenuItem>
          </Select>
        </FormControl>
        <FormControl fullWidth style={{margin: '10px 0'}}>
          <InputLabel id="role">Role</InputLabel>
          <Select
              labelId="role"
              id="role"
              value={user.role}
              required
              onChange={(e)=>changeHandler('role', e)}
          >
            <MenuItem value={'ADMIN'} style={menuItemStyle}>ADMIN</MenuItem>
            <MenuItem value={'USER'} style={menuItemStyle}>USER</MenuItem>
          </Select>
        </FormControl>
        <FormControl fullWidth style={{margin: '10px 0'}}>
          <InputLabel id="permission">Permissions</InputLabel>
          <Select
              autoWidth={true}
              labelId="permission"
              id="permission"
              value={user.permissions}
              label="Permissions"
              multiple={true}
              required
              renderValue={(selected: Array<String>) => selected.join(', ')}
              onChange={(e)=>changeHandler('permissions', e)}
          >
            {PERMISSIONS.map((name) => (
                <MenuItem key={name} value={name} style={menuItemStyle}>
                  <Checkbox size="small" checked={user.permissions.indexOf(name) > -1} color="primary"/>
                  <span style={{fontSize: '16px'}}>{name}</span>
                </MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl fullWidth style={{margin: '10px 0'}}>
          <InputLabel id="tables">Tables</InputLabel>
          <Select
              labelId="tables"
              id="tables"
              value={user.tables}
              multiple={true}
              required
              renderValue={(selected: Array<String>) => selected.join(', ')}
              onChange={(e)=>changeHandler('tables', e)}
          >
            {tables.map(table=>(
                <MenuItem key={table.name} value={table.name} style={menuItemStyle}>
                  <Checkbox checked={table.checked} name={table.name} disabled={table.disabled} size="small" color="primary" onChange={tableCheckChange}/>
                  <ListItemText primary={table.name} style={{fontSize: '14px'}}/>
                </MenuItem>
            ))}
          </Select>
        </FormControl>
      </DialogContent>
  );
}