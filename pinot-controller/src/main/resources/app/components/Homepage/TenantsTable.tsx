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
import { TableData } from 'Models';
import union from 'lodash/union';
import { getTenants } from '../../requests';
import AppLoader from '../AppLoader';
import CustomizedTables from '../Table';

const TenantsTable = () => {
  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({ records: [], columns: [] });

  useEffect(() => {
    getTenants().then(({ data }) => {
      const records = union(
        data.SERVER_TENANTS,
        data.BROKER_TENANTS
      );
      setTableData({
        columns: ['Name', 'Server', 'Broker', 'Tables'],
        records: [
          ...records.map(record => [
            record,
            data.SERVER_TENANTS.indexOf(record) > -1 ? 1 : 0,
            data.BROKER_TENANTS.indexOf(record) > -1 ? 1 : 0,
            '-'
          ])
        ]
      });
      setFetching(false);
    });
  }, []);

  return (
    fetching ? <AppLoader /> : <CustomizedTables title="Tenants" data={tableData} addLinks isPagination baseURL="/tenants/" />
  );
};

export default TenantsTable;