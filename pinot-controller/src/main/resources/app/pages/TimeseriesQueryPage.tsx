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
import TimeseriesQueryPage from '../components/Query/TimeseriesQueryPage';
import QuerySideBar from '../components/Query/QuerySideBar';
import { TableData } from 'Models';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Utils from '../utils/Utils';

const TimeseriesQueryPageWrapper = () => {
  const [tableList, setTableList] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [tableSchema, setTableSchema] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [selectedTable, setSelectedTable] = useState('');
  const [queryLoader, setQueryLoader] = useState(false);
  const [fetching, setFetching] = useState(true);

  const fetchSQLData = async (tableName) => {
    setQueryLoader(true);
    const result = await PinotMethodUtils.getTableSchemaData(tableName);
    const tableSchema = Utils.syncTableSchemaData(result, false);
    setTableSchema(tableSchema);
    setSelectedTable(tableName);
    setQueryLoader(false);
  };

  const fetchData = async () => {
    const result = await PinotMethodUtils.getQueryTablesList({bothType: false});
    setTableList(result);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (fetching) {
    return null;
  }

  return (
    <>
      <Grid item>
        <QuerySideBar
          tableList={tableList}
          fetchSQLData={fetchSQLData}
          tableSchema={tableSchema}
          selectedTable={selectedTable}
          queryLoader={queryLoader}
          queryType="timeseries"
        />
      </Grid>
      <Grid
        item
        xs
        style={{
          padding: 20,
          backgroundColor: 'white',
          maxHeight: 'calc(100vh - 70px)',
          overflowY: 'auto',
        }}
      >
        <TimeseriesQueryPage />
      </Grid>
    </>
  );
};

export default TimeseriesQueryPageWrapper;
