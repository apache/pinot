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
import { Grid } from '@material-ui/core';
import TenantsTable from '../components/Homepage/TenantsTable';
import Instances from '../components/Homepage/InstancesTables';
import ClusterConfig from '../components/Homepage/ClusterConfig';

const HomePage = () => {
  return (
    <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
      <TenantsTable />
      <Instances />
      <ClusterConfig />
    </Grid>
  );
};

export default HomePage;