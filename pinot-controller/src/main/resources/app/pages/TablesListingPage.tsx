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

import React, { useState, useEffect, useCallback } from 'react';
import { Grid, makeStyles } from '@material-ui/core';
import { TableData } from 'Models';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from '../components/Table';
import SimpleAccordion from '../components/SimpleAccordion';
import Skeleton from '@material-ui/lab/Skeleton';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto',
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
}));

const TableTooltipData = [
  null,
  'Uncompressed size of all data segments',
  'Estimated size of all data segments, in case any servers are not reachable for actual size',
  null,
  'GOOD if all replicas of all segments are up',
];

const TablesListingPage = () => {
  const classes = useStyles();

  const [tableData, setTableData] = useState<TableData>({
    columns: PinotMethodUtils.allTableDetailsColumnHeader,
    records: [],
    isLoading: true,
  });

  const [currentlySelectedTables, setCurrentlySelectedTables] = useState([])

  const loading = { customRenderer: <Skeleton animation={'wave'} /> };

  useEffect(() => {
    async function initTables () {
      const tablesResponse = await PinotMethodUtils.getQueryTablesList({
        bothType: true,
      });
      const tablesList = tablesResponse.records.flat();
      const tableData = [];
      tablesList.map((table) => {
        tableData.push([table].concat([...Array(PinotMethodUtils.allTableDetailsColumnHeader.length - 1)].map((e) => loading)));
      });
      // Set the table data to "Loading..." at first as tableSize can take minutes to fetch
      // for larger tables.
      setTableData({
        columns: PinotMethodUtils.allTableDetailsColumnHeader,
        records: tableData,
        isLoading: false,
      });
      // get first 10
      const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList.slice(0, 10));
      const records = tableDetails.records.concat(tableData)
      setTableData({ columns: tableDetails.columns, records });
    }
    initTables();
  }, []);

  useEffect(() => {
    if (currentlySelectedTables.length === 0) return
    async function loadData () {
      const tablesList = currentlySelectedTables.map(row => row['Table Name#$%0'])
  
      // these implicitly set isLoading=false by leaving it undefined
      const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList);
      setTableData({ columns: tableDetails.columns, records: tableData.records.map(record => {
        const updatedTable = tableDetails.records.find(table => table[0] === record[0])
        if (updatedTable) {
          return updatedTable
        }
        return record
      })});
    }
    loadData()
  }, [currentlySelectedTables])

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.operationDiv}>
        <SimpleAccordion headerTitle="Operations" showSearchBox={false}>
          <div>
          </div>
        </SimpleAccordion>
      </div>
      <CustomizedTables
        title="Tables"
        data={tableData}
        addLinks
        baseURL="/tenants/table/"
        showSearchBox={true}
        inAccordionFormat={true}
        tooltipData={TableTooltipData}
        onRowsRendered={(rows) => setCurrentlySelectedTables(rows)}
      />
    </Grid>
  );
};

export default TablesListingPage;
