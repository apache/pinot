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

import React, {useState, useEffect} from 'react';
import { Grid, makeStyles } from '@material-ui/core';
import { DataTable } from 'Models';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Instances from '../components/Homepage/InstancesTables';
import { getInstanceTypeFromString } from '../utils/Utils';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
}));

const InstanceListingPage = () => {
  const classes = useStyles();

  const [fetching, setFetching] = useState(true);
  const [clusterName, setClusterName] = useState('');
  const [instances, setInstances] = useState<DataTable>();
  const [liveInstanceNames, setLiveInstanceNames] = useState<string[]>();

  const fetchData = async () => {
    let clusterNameRes = localStorage.getItem('pinot_ui:clusterName');
    if(!clusterNameRes){
      clusterNameRes = await PinotMethodUtils.getClusterName();
    }
    setClusterName(clusterNameRes);

    const liveInstanceNames = await PinotMethodUtils.getLiveInstance(clusterNameRes);
    setLiveInstanceNames(liveInstanceNames.data || []);

    const instancesList = await PinotMethodUtils.getAllInstances();
    setInstances(instancesList);

    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const instanceType = getInstanceTypeFromString(window.location.hash.split('/')[1].slice(0, -1));

  return fetching ? (
    <AppLoader />
  ) : (
    <Grid item xs className={classes.gridContainer}>
      <Instances 
        liveInstanceNames={liveInstanceNames} 
        instances={instances} 
        clusterName={clusterName} 
        instanceType={instanceType} 
      />
    </Grid>
  );
};

export default InstanceListingPage;