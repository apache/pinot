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
import { getTenantTable, getTableSize, getIdealState } from '../requests';

type Props = {
  name: string
};

const TenantPage = ({ match }: RouteComponentProps<Props>) => {

  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });

  useEffect(() => {
    getTenantTable(match.params.name).then(({ data }) => {
      const tableArr = data.tables.map(table => table);
      if(tableArr.length){
        const promiseArr = tableArr.map(name => getTableSize(name));
        const promiseArr2 = tableArr.map(name => getIdealState(name));

        Promise.all(promiseArr).then(results => {
          Promise.all(promiseArr2).then(response => {
            setTableData({
              columns: ['Name', 'Reported Size', 'Extimated Size', 'Number of Segments', 'Status'],
              records: [
                ...results.map(( result ) => {
                  let actualValue; let idealValue;
                  const tableSizeObj = result.data;
                  response.forEach((res) => {
                    const idealStateObj = res.data;
                    if(tableSizeObj.realtimeSegments !== null && idealStateObj.REALTIME !== null){
                      const { segments } = tableSizeObj.realtimeSegments;
                      actualValue = Object.keys(segments).length;
                      idealValue = Object.keys(idealStateObj.REALTIME).length;
                    }else
                    if(tableSizeObj.offlineSegments !== null && idealStateObj.OFFLINE !== null){
                      const { segments } = tableSizeObj.offlineSegments;
                      actualValue = Object.keys(segments).length;
                      idealValue = Object.keys(idealStateObj.OFFLINE).length;
                    }
                  });
                  return [tableSizeObj.tableName, tableSizeObj.reportedSizeInBytes, tableSizeObj.estimatedSizeInBytes,
                    `${actualValue} / ${idealValue}`, actualValue === idealValue ? 'Good' : 'Bad'];
                })
              ]
            });
            setFetching(false);
          });
        });
      }else {
        setTableData({
          columns: ['Name', 'Reported Size', 'Extimated Size', 'Number of Segments', 'Status'],
          records: []
        });
        setFetching(false);
      }
    });
  }, []);
  return (
    fetching ? <AppLoader /> :
    <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
      <CustomizedTables title={match.params.name} data={tableData} isPagination />
    </Grid>
  );
};

export default TenantPage;
