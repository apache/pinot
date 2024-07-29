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

import React, { useEffect, useState } from 'react';
import { union } from 'lodash';
import CustomizedTables from '../Table';
import { TableData } from 'Models';
import Loading from '../Loading';
import { getTenants, getTenantTable } from '../../requests';
import PinotMethodUtils from '../../utils/PinotMethodUtils';

const TenantsTable = () => {
  const columns = ['Tenant Name', 'Server', 'Broker', 'Tables'];
  const [tenantsData, setTenantsData] = useState<TableData>({
    records: [columns.map((_) => Loading)],
    columns: columns,
  });

  const fetchData = async () => {
    getTenants().then((res) => {
      const tenantNames = union(
        res.data.SERVER_TENANTS,
        res.data.BROKER_TENANTS
      );
      setTenantsData({
        columns: columns,
        records: tenantNames.map((tenantName) => {
          return [tenantName, Loading, Loading, Loading];
        }),
      });

      tenantNames.forEach((tenantName) => {
        Promise.all([
          PinotMethodUtils.getServerOfTenant(tenantName).then((res) => {
            return res?.length || 0;
          }),
          PinotMethodUtils.getBrokerOfTenant(tenantName).then((res) => {
            return Array.isArray(res) ? res?.length || 0 : 0;
          }),
          getTenantTable(tenantName).then((res) => {
            return res?.data?.tables?.length || 0;
          }),
        ]).then((res) => {
          const numServers = res[0];
          const numBrokers = res[1];
          const numTables = res[2];
          setTenantsData((prev) => {
            const newRecords = prev.records.map((record) => {
              if (record[0] === tenantName) {
                return [tenantName, numServers, numBrokers, numTables];
              }
              return record;
            });
            return {
              columns: prev.columns,
              records: newRecords,
            };
          });
        });
      });
    });
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <CustomizedTables
      title="Tenants"
      data={tenantsData}
      addLinks
      baseURL="/tenants/"
      showSearchBox={true}
      inAccordionFormat={true}
    />
  );
};

export default TenantsTable;
