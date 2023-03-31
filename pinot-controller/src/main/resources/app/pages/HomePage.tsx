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
import { Grid, makeStyles, Paper, Box } from '@material-ui/core';
import { TableData, DataTable } from 'Models';
import { Link } from 'react-router-dom';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import TenantsListing from '../components/Homepage/TenantsListing';
import Instances from '../components/Homepage/InstancesTables';
import ClusterConfig from '../components/Homepage/ClusterConfig';
import useTaskTypesTable from '../components/Homepage/useTaskTypesTable';

const useStyles = makeStyles((theme) => ({
  paper:{
    padding: '10px 0',
    height: '100%',
    color: '#4285f4',
    borderRadius: 4,
    marginBottom: 15,
    textAlign: 'center',
    backgroundColor: 'rgba(66, 133, 244, 0.1)',
    borderColor: 'rgba(66, 133, 244, 0.5)',
    borderStyle: 'solid',
    borderWidth: '1px',
    '& h2, h4': {
      margin: 0,
    },
    '& h4':{
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 600
    },
    '&:hover': {
      borderColor: '#4285f4'
    }
  },
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
  paperLinks: {
    textDecoration: 'none',
    height: '100%'
  }
}));

const HomePage = () => {
  const classes = useStyles();

  const [fetching, setFetching] = useState(true);
  const [tenantsData, setTenantsData] = useState<TableData>({ records: [], columns: [] });
  const [instances, setInstances] = useState<DataTable>();
  const [clusterName, setClusterName] = useState('');
  const [tables, setTables] = useState([]);

  const { taskTypes, taskTypesTable } = useTaskTypesTable();

  const fetchData = async () => {
    const instanceResponse = await PinotMethodUtils.getAllInstances();
    const taskTypes = await PinotMethodUtils.getAllTaskTypes();
    const tablesResponse = await PinotMethodUtils.getQueryTablesList({bothType: true});
    const tablesList = [];
    tablesResponse.records.map((record)=>{
      tablesList.push(...record);
    });
    setInstances(instanceResponse);
    setTables(tablesList);
    let clusterNameRes = localStorage.getItem('pinot_ui:clusterName');
    if(!clusterNameRes){
      clusterNameRes = await PinotMethodUtils.getClusterName();
    }
    setClusterName(clusterNameRes);
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
    async function fetchTenant () {
      const tenantsDataResponse = await PinotMethodUtils.getTenantsData();
      setTenantsData(tenantsDataResponse);
    }
    fetchTenant();
  }, []);
  
  return fetching ? (
    <AppLoader />
  ) : (
    <Grid item xs className={classes.gridContainer}>
      <Grid container spacing={3}>
        <Grid item xs={3}>
          <Link to="/controllers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Controllers</h4>
              <h2>{Array.isArray(instances.Controller) ? instances.Controller.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/brokers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Brokers</h4>
              <h2>{Array.isArray(instances.Broker) ? instances.Broker.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/servers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Servers</h4>
              <h2>{Array.isArray(instances.Server) ? instances.Server.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/minions" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Minions</h4>
              <h2>{Array.isArray(instances.Minion) ? instances.Minion.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/tenants" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Tenants</h4>
              <h2>{Array.isArray(tenantsData.records) ? tenantsData.records.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/tables" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Tables</h4>
              <h2>{Array.isArray(tables) ? tables.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/minion-task-manager" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Minion Task Manager</h4>
              <h2>{Array.isArray(taskTypes.records) ? taskTypes?.records?.length : 0}</h2>
            </Paper>
          </Link>
        </Grid>
      </Grid>
      <Box mb={3} />
      <TenantsListing tenantsData={tenantsData} />
      <Instances instances={instances} clusterName={clusterName} />

      {taskTypesTable}
      <ClusterConfig />
    </Grid>
  );
};

export default HomePage;