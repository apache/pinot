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

import React, {useEffect, useState} from 'react';
import {Button, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Dialog, DialogActions, Snackbar, DialogTitle} from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import { makeStyles } from '@material-ui/core/styles';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import '../styles/styles.css';
import AddUser from "../components/User/AddUser";
import UpdateUser from "../components/User/UpdateUser";
import * as _ from "lodash";
import AppLoader from "../components/AppLoader";

const initialUser = {
    username: "",
    password: "",
    component: "",
    role: "",
    tables: [],
    permissions: ['READ']
}
const PERMISSIONS = ["READ", "CREATE", "UPDATE", "DELETE"];
const columns = [
    { id: 'User', label: 'User'},
    { id: 'Component', label: 'Component'},
    { id: 'Role', label: 'Role'},
    { id: 'Permissions', label: 'Permissions'},
    { id: 'Tables', label: 'Tables'},
    { id: 'Actions', label: 'Actions'}
];
let oldPassword = "";
const UserPage = () => {
    const [userList, setUserList] = useState([]);
    const [addUserPanel, setAddUserPanel] = useState(false);
    const [deleteUserPanel, setDeleteUserPanel] = useState(false);
    const [updateUserPanel, setUpdateUserPanel] = useState(false);
    const [tableList, setTableList] = useState([]);
    const [userInfo, updateUserInfo] = useState(initialUser);
    const [errorTip, setErrorTip] = useState(null);
    const [showError, setShowError] = useState(false);
    const [successTip, setSuccessTip] = useState(null);
    const [showSuccess, setShowSuccess] = useState(false);
    const [checkedUser, setCheckedUser] = useState(initialUser);
    const [fetching, setFetching] = useState(false);
    const getUserList = async () => {
        setFetching(true);
        const tablesResponse = await PinotMethodUtils.getTable();
        let tables = tablesResponse.tables;
        tables.unshift("DUAL", "ALL");
        let tableList = tables.map(item=>{
            return {
                name: item,
                checked: item === 'DUAL',
                disabled: false
            }
        })
        setTableList(tableList);
        const userListResponse = await PinotMethodUtils.getUserList();
        let userObj = userListResponse.users;
        let userData = [];
        for (let key in userObj) {
            if(userObj.hasOwnProperty(key)){
                userData.push(userObj[key]);
            }
        }
        userData = userData.map(item => {
            return {
                component: item.component,
                role: item.role,
                username: item.username,
                usernameWithComponent: item.usernameWithComponent,
                tables: item.tables ? item.tables: ["ALL"],
                permissions: item.permissions ? item.permissions : PERMISSIONS,
                password: item.password
            }
        })
        userData.sort((prev, next)=>{
            let prevUser = prev.username.toUpperCase();
            let nextUser = next.username.toUpperCase();
            if (prevUser < nextUser) {
                return -1;
            }
            if (prevUser > nextUser) {
                return 1;
            }
            return 0;
        })
        setUserList(userData);
        setFetching(false);
    };

    const toggleAddUserPanel = ()=>{
        updateUserInfo(initialUser);
        setAddUserPanel(true);
    };
    const closeAddUserPanel = ()=>{
        setAddUserPanel(false)
    };

    const submitAddUser = async()=>{
        let addUserParam = _.cloneDeep(userInfo);
        if(addUserParam.tables.includes('ALL')){
            delete addUserParam.tables
        }
        const addUserResponse = await PinotMethodUtils.addUser(addUserParam);
        if(addUserResponse.code && addUserResponse.code !== 200){
            setShowError(true);
            setErrorTip(addUserResponse.error);
        }else{
            setAddUserPanel(false);
            setShowSuccess(true);
            setSuccessTip("Add user success");
            getUserList();
        }
    };

    const closeTip = (type)=>{
        type === 'error' ? setShowError(false): setShowSuccess(false);
    };

    const deleteUser = async()=>{
        const deleteUserResponse = await PinotMethodUtils.deleteUser(checkedUser);
        if(deleteUserResponse.code && deleteUserResponse.code !== 200){
            setShowError(true);
            setErrorTip(deleteUserResponse.error);
        }else{
            setDeleteUserPanel(false);
            setShowSuccess(true);
            setSuccessTip("delete user success");
            getUserList();
        }
    }

    const clickDeleteUser = (row)=>{
        setCheckedUser(row);
        setDeleteUserPanel(true);
    }

    const clickEditUser = (row)=>{
        oldPassword = row.password;
        updateUserInfo(row);
        setUpdateUserPanel(true);
    }

    const closeUpdateUserPanel = ()=>{
        setUpdateUserPanel(false);
    }

    const submitUpdateUser = async()=>{
        let updateUserParam = _.cloneDeep(userInfo);
        if(updateUserParam.tables.includes('ALL')){
            delete updateUserParam.tables
        }
        const passwordChanged = updateUserParam.password !== oldPassword ? true: false;
        const updateUserResponse = await PinotMethodUtils.updateUser(updateUserParam, passwordChanged);
        if(updateUserResponse.code && updateUserResponse.code !== 200){
            setShowError(true);
            setErrorTip(updateUserResponse.error);
        }else{
            setUpdateUserPanel(false);
            setShowSuccess(true);
            setSuccessTip("Update user success");
            getUserList();
        }
    }

    useEffect(() => {
        getUserList();
    }, []);

    const useStyles = makeStyles({
        userContainer: {
            width: 'calc(100% - 260px)', padding: '32px', height: 'calc(100vh - 70px)', fontSize: '14px'
        },
        tableBody: {
            maxHeight: 'calc(100vh - 254px)', overflowY: 'auto'
        },
        tableColItemStyle: {
            background: "#1170CF", borderRadius: '4px', padding: '4px 8px', display: 'inline-block', color: '#fff',
            fontSize: '14px', marginRight: '18px', marginTop: '5px', marginBottom: '5px'
        },
        delButton: {
            borderRadius: '16px',
            color: 'rgb(220, 0, 78)',
            border: '1px solid rgba(220, 0, 78, 0.5)',
            "&:hover": {
                background: 'rgba(220, 0, 78, 0.2)'
            }
        },

    });
    const classes = useStyles();
    return (
        <div className={classes.userContainer}>
            <Button variant="contained" onClick={toggleAddUserPanel} color="primary" style={{marginBottom: '20px', borderRadius: '16px'}}>Add User</Button>
            {fetching ? <AppLoader/>: <TableContainer component={Paper} style={{height: 'calc(100% - 60px)'}}>
                <Table aria-label="simple table">
                    <TableHead>
                        <TableRow>
                            {columns.map((column) => (
                                <TableCell style={{fontWeight: 700}} key={column.id}>
                                    {column.label}
                                </TableCell>
                            ))}
                        </TableRow>
                    </TableHead>
                    <TableBody className={classes.tableBody}>
                        {userList.map((row) => (
                            <TableRow key={row.usernameWithComponent}>
                                <TableCell component="th" scope="row">
                                    {row.username}
                                </TableCell>
                                <TableCell align="justify" >{row.component}</TableCell>
                                <TableCell align="justify">{row.role}</TableCell>
                                <TableCell align="justify">{
                                    row.permissions && row.permissions.length >0 && row.permissions.map(permission => {
                                        return <span className={classes.tableColItemStyle} key={permission}>{permission}</span>
                                    })}
                                </TableCell>
                                <TableCell align="justify" style={{wordWrap: "break-word"}}>
                                    {row.tables && row.tables.length > 0 && row.tables.map(table =>{
                                        return <span className={classes.tableColItemStyle} key={table}>{table}</span>
                                    })}
                                </TableCell>
                                <TableCell align="justify" style={{minWidth: '200px'}}>
                                    <Button variant="outlined" size="small" style={{marginRight: '10px', borderRadius: '16px'}} onClick={()=>clickEditUser(row)}>Edit</Button>&nbsp;
                                    <Button variant="outlined" onClick={()=>clickDeleteUser(row)} size="small" className={classes.delButton}>Delete</Button>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>}
            <Dialog open={addUserPanel} onClose={closeAddUserPanel}>
                <DialogTitle>Add User</DialogTitle>
                <AddUser tableList={tableList} userProp={userInfo} setUserInfo={(userInfo) =>{updateUserInfo(userInfo)}}/>
                <DialogActions>
                    <Button onClick={closeAddUserPanel} size="small">Cancel</Button>
                    <Button onClick={submitAddUser} size="small">Submit</Button>
                </DialogActions>
            </Dialog>
            <Dialog open={updateUserPanel} onClose={closeUpdateUserPanel}>
                <DialogTitle>Edit User</DialogTitle>
                <UpdateUser tableList={tableList} userProp={userInfo} setUserInfo={(userInfo) =>{updateUserInfo(userInfo)}}/>
                <DialogActions>
                    <Button onClick={closeUpdateUserPanel} size="small">Cancel</Button>
                    <Button onClick={submitUpdateUser} size="small">Submit</Button>
                </DialogActions>
            </Dialog>
            <Dialog
                open={deleteUserPanel}
                onClose={()=>{setDeleteUserPanel(false)}}
                aria-labelledby="alert-dialog-title"
            >
                <DialogTitle id="alert-dialog-title">{`Confirm Delete user ${checkedUser.username}_${checkedUser.component}?`}</DialogTitle>
                <DialogActions>
                    <Button onClick={()=>{setDeleteUserPanel(false)}} color="primary">
                        Cancel
                    </Button>
                    <Button onClick={deleteUser} color="primary" autoFocus>
                        Confirm
                    </Button>
                </DialogActions>
            </Dialog>
            <Snackbar open={showError} autoHideDuration={6000} onClose={()=>{closeTip('error')}}>
                <Alert onClose={()=>{closeTip('error')}} severity="error">
                    {errorTip}
                </Alert>
            </Snackbar>
            <Snackbar open={showSuccess} autoHideDuration={1000} onClose={()=>{closeTip('success')}}>
                <Alert onClose={()=>{closeTip('success')}} severity="success">
                    {successTip}
                </Alert>
            </Snackbar>
        </div>
    )
}
export default UserPage;
