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
import { Grid } from '@material-ui/core';
import { TableData } from 'Models';
import { RouteComponentProps } from 'react-router-dom';
import CustomizedTables from '../components/Table';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';

type Props = {
  tenantName: string
};

const TenantPage = ({ match }: RouteComponentProps<Props>) => {

  const tenantName = match.params.tenantName;
  const columnHeaders = ['Table Name', 'Reported Size', 'Estimated Size', 'Number of Segments', 'Status'];
  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: columnHeaders,
    records: []
  });
  const [brokerData, setBrokerData] = useState([]);
  const [serverData, setServerData] = useState([]);

  const fetchData = async () => {
    const tenantData = await PinotMethodUtils.getTenantTableData(tenantName);
    const brokersData = await PinotMethodUtils.getBrokerOfTenant(tenantName);
    const serversData = await PinotMethodUtils.getServerOfTenant(tenantName);
    setTableData(tenantData);
    setBrokerData(brokersData);
    setServerData(serversData);
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
  }, []);
  return (
    fetching ? <AppLoader /> :
    <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
      <CustomizedTables
        title={tenantName}
        data={tableData}
        isPagination
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
              records: [brokerData]
            }}
            isPagination
            addLinks
            baseURL={'/instance/'}
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
        <Grid item xs={6}>
          <CustomizedTables
            title="Servers"
            data={{
              columns: ['Instance Name'],
              records: [serverData]
            }}
            isPagination
            addLinks
            baseURL={'/instance/'}
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TenantPage;
