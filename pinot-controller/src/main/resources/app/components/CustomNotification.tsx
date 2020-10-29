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

import React, {  } from 'react';
import { Snackbar } from '@material-ui/core';
import MuiAlert from '@material-ui/lab/Alert';
import { NotificationContext } from './Notification/NotificationContext';

const Alert = (props) => {
  return <MuiAlert elevation={6} variant="filled" {...props} />;
};

const CustomNotification = () => {
  return (
    <NotificationContext.Consumer>
      {context => 
        <Snackbar
          anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
          open={context && context.show}
          onClose={()=>{
            context.dispatch({type: '', message: "", show:false});
            context.hide && context.hide()
          }}
          autoHideDuration={10000}
        >
          <Alert
            onClose={context.hide && context.hide()}
            severity={context && context.type}
          >
            {context && context.message}
          </Alert>
        </Snackbar>
        }
    </NotificationContext.Consumer>
  );
};

export default CustomNotification;