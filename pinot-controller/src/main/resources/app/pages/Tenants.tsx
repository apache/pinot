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
import { Grid, makeStyles } from '@material-ui/core';
import { TableData } from 'Models';
import { RouteComponentProps } from 'react-router-dom';
import CustomizedTables from '../components/Table';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';

const useStyles = makeStyles((theme) => ({
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  }
}));

type Props = {
  tenantName: string
};

const TableTooltipData = [
  null,
  "Uncompressed size of all data segments with replication",
  "Estimated size of all data segments with replication, in case any servers are not reachable for actual size",
  null,
  "GOOD if all replicas of all segments are up"
];

const TenantPage = ({ match }: RouteComponentProps<Props>) => {

  const {tenantName} = match.params;
  const columnHeaders = ['Table Name', 'Reported Size', 'Estimated Size', 'Number of Segments', 'Status'];
  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: columnHeaders,
    records: []
  });
  const [brokerData, setBrokerData] = useState(null);
  const [serverData, setServerData] = useState([]);

  const fetchData = async () => {
    const tenantData = await PinotMethodUtils.getTenantTableData(tenantName);
    const brokersData = await PinotMethodUtils.getBrokerOfTenant(tenantName);
    const serversData = await PinotMethodUtils.getServerOfTenant(tenantName);
    setTableData(tenantData);
    const separatedBrokers = Array.isArray(brokersData) ? brokersData.map((elm) => [elm]) : [];
    setBrokerData(separatedBrokers || []);
    const separatedServers = Array.isArray(serversData) ? serversData.map((elm) => [elm]) : [];
    setServerData(separatedServers || []);
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
  }, []);

  const classes = useStyles();

  return (
    fetching ? <AppLoader /> :
    <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
            <CustomButton
              onClick={()=>{console.log('rebalance');}}
              tooltipTitle="Recalculates the segment to server mapping for all tables in this tenant"
              enableTooltip={true}
              isDisabled={true}
            >
              Rebalance Server Tenant
            </CustomButton>
            <CustomButton
              onClick={()=>{console.log('rebuild');}}
              tooltipTitle="Rebuilds brokerResource mappings for all tables in this tenant"
              enableTooltip={true}
              isDisabled={true}
            >
              Rebuild Broker Resource
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>
      <CustomizedTables
        title={tenantName}
        data={tableData}
        tooltipData={TableTooltipData}
        addLinks
        baseURL={`/tenants/${tenantName}/table/`}
        showSearchBox={true}
        inAccordionFormat={true}
      />
      <Grid container spacing={2}>
        <Grid item xs={6}>
          <CustomizedTables
            title="Brokers"
            data={{
              columns: ['Instance Name'],
              records: brokerData.length > 0 ? brokerData : []
            }}
            addLinks
            baseURL="/instance/"
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
        <Grid item xs={6}>
          <CustomizedTables
            title="Servers"
            data={{
              columns: ['Instance Name'],
              records: serverData.length > 0 ? serverData : []
            }}
            addLinks
            baseURL="/instance/"
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TenantPage;
