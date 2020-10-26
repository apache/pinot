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


const Alert = (props) => {
  return <MuiAlert elevation={6} variant="filled" {...props} />;
};

type Props = {
  type: string,
  message: string,
  show: boolean,
  hide: Function
};

const CustomNotification = ({
  type, message, show, hide
}: Props) => {

  const [notificationData, setNotificationData] = React.useState({type, message});
  const [showNotification, setShowNotification] = React.useState(show);

  React.useEffect(()=>{
    setShowNotification(show);
  }, [show]);

  React.useEffect(()=>{
    setNotificationData({type, message});
  }, [type, message]);

  const hideNotification = () => {
    hide();
    setShowNotification(false);
  };

  return (
    <Snackbar
      anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
      open={showNotification}
      onClose={hideNotification}
      key="notification"
      autoHideDuration={3000}
    >
      <Alert severity={notificationData.type}>{notificationData.message}</Alert>
    </Snackbar>
  );
};

export default CustomNotification;