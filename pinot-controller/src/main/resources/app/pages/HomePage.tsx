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

import React, { useState, useEffect } from 'react';
import { get, union } from 'lodash';
import { Grid, makeStyles, Paper, Box } from '@material-ui/core';
import { Link } from 'react-router-dom';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import TenantsListing from '../components/Homepage/TenantsListing';
import Instances from '../components/Homepage/InstancesTables';
import ClusterConfig from '../components/Homepage/ClusterConfig';
import useTaskTypesTable from '../components/Homepage/useTaskTypesTable';
import Skeleton from '@material-ui/lab/Skeleton';
import { getTenants } from '../requests';

const useStyles = makeStyles((theme) => ({
  paper: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
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
    '& h4': {
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 600,
    },
    '&:hover': {
      borderColor: '#4285f4',
    },
  },
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto',
  },
  paperLinks: {
    textDecoration: 'none',
    height: '100%',
  },
}));

const HomePage = () => {
  const classes = useStyles();

  const [clusterName, setClusterName] = useState('');

  const [fetchingTenants, setFetchingTenants] = useState(true);
  const [tenantsCount, setTenantscount] = useState(0);

  const [fetchingInstances, setFetchingInstances] = useState(true);
  const [controllerCount, setControllerCount] = useState(0);
  const [brokerCount, setBrokerCount] = useState(0);
  const [serverCount, setServerCount] = useState(0);
  const [minionCount, setMinionCount] = useState(0);
  // const [instances, setInstances] = useState<DataTable>();

  const [fetchingTables, setFetchingTables] = useState(true);
  const [tablesCount, setTablesCount] = useState(0);

  const { taskTypes, taskTypesTable } = useTaskTypesTable();

  const fetchData = async () => {
    PinotMethodUtils.getAllInstances().then((res) => {
      setControllerCount(get(res, 'Controller', []).length);
      setBrokerCount(get(res, 'Broker', []).length);
      setServerCount(get(res, 'Server', []).length);
      setMinionCount(get(res, 'Minion', []).length);
      setFetchingInstances(false);
    });

    PinotMethodUtils.getQueryTablesList({ bothType: true }).then((res) => {
      setTablesCount(res.records.length);
      setFetchingTables(false);
    });

    getTenants().then((res) => {
      const tenantNames = union(
        res.data.SERVER_TENANTS,
        res.data.BROKER_TENANTS
      );
      setTenantscount(tenantNames.length);
      setFetchingTenants(false);
    });

    fetchClusterName().then((clusterNameRes) => {
      setClusterName(clusterNameRes);
    });
  };

  const fetchClusterName = () => {
    let clusterNameRes = localStorage.getItem('pinot_ui:clusterName');
    if (!clusterNameRes) {
      return PinotMethodUtils.getClusterName();
    } else {
      return Promise.resolve(clusterNameRes);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const loading = <Skeleton animation={'wave'} width={50} />;

  return (
    <Grid item xs className={classes.gridContainer}>
      <Grid container spacing={3}>
        <Grid item xs={3}>
          <Link to="/controllers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Controllers</h4>
              <h2>{fetchingInstances ? loading : controllerCount}</h2>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/brokers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Brokers</h4>
              <h2>{fetchingInstances ? loading : brokerCount}</h2>
              {/*<h2>{Array.isArray(instances.Broker) ? instances.Broker.length : 0}</h2>*/}
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/servers" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Servers</h4>
              <h2>{fetchingInstances ? loading : serverCount}</h2>
              {/*<h2>{Array.isArray(instances.Server) ? instances.Server.length : 0}</h2>*/}
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/minions" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Minions</h4>
              <h2>{fetchingInstances ? loading : minionCount}</h2>
              {/*<h2>{Array.isArray(instances.Minion) ? instances.Minion.length : 0}</h2>*/}
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/tenants" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Tenants</h4>
              <h2>{fetchingTenants ? loading : tenantsCount}</h2>
              {/*<h2>{Array.isArray(tenantsData.records) ? tenantsData.records.length : 0}</h2>*/}
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/tables" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Tables</h4>
              <h2>{fetchingTables ? loading : tablesCount}</h2>
              {/*<h2>{Array.isArray(tables) ? tables.length : 0}</h2>*/}
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={3}>
          <Link to="/minion-task-manager" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Minion Task Manager</h4>
              <h2>
                {Array.isArray(taskTypes.records)
                  ? taskTypes?.records?.length
                  : 0}
              </h2>
            </Paper>
          </Link>
        </Grid>
      </Grid>
      <Box mb={3} />
      <TenantsListing />
      <Instances clusterName={clusterName} />

      {taskTypesTable}
      <ClusterConfig />
    </Grid>
  );
};

export default HomePage;
