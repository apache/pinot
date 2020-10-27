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

import * as React from 'react';
import { NotificationContext } from "./NotificationContext";
import { useReducer } from "react";


type NotificationContextReducers = {
  type: string,
  message: string,
  show: boolean,
  hide: Function
};

const NotificationContextValue : NotificationContextReducers = {
  type: "",
  message: "",
  show: false,
  hide: ()=>{}
};


const notificationReducer = (state) => {
  return state;
}

const mainReducer = ({ type, message, show, hide }, action) => ({
  type: notificationReducer(action.type),
  message: notificationReducer(action.message),
  show: notificationReducer(action.show),
  hide: notificationReducer(action.hide)
});
  

const NotificationContextProvider: React.FC  = (props) =>{
  const [state, dispatch] = useReducer(mainReducer, NotificationContextValue);
  return(
    <NotificationContext.Provider value={{...state,dispatch}}>
      {props.children}
    </NotificationContext.Provider>
  )
 }

 export { NotificationContextProvider };