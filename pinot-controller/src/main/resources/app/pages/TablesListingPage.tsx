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
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from '../components/Table';
import CustomButton from '../components/CustomButton';
import SimpleAccordion from '../components/SimpleAccordion';
import AddSchemaOp from '../components/Homepage/Operations/AddSchemaOp';
import AddOfflineTableOp from '../components/Homepage/Operations/AddOfflineTableOp';
import AddRealtimeTableOp from '../components/Homepage/Operations/AddRealtimeTableOp';
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
  'Uncompressed size of all data segments with replication',
  'Estimated size of all data segments with replication, in case any servers are not reachable for actual size',
  null,
  'GOOD if all replicas of all segments are up',
];

const TablesListingPage = () => {
  const classes = useStyles();

  const [schemaDetails, setSchemaDetails] = useState<TableData>({
    columns: PinotMethodUtils.allSchemaDetailsColumnHeader,
    records: [],
    isLoading: true,
  });
  const [tableData, setTableData] = useState<TableData>({
    columns: PinotMethodUtils.allTableDetailsColumnHeader,
    records: [],
    isLoading: true,
  });
  const [showSchemaModal, setShowSchemaModal] = useState(false);
  const [showAddOfflineTableModal, setShowAddOfflineTableModal] = useState(
    false
  );
  const [showAddRealtimeTableModal, setShowAddRealtimeTableModal] = useState(
    false
  );

  const loading = { customRenderer: <Skeleton animation={'wave'} /> };

  const fetchData = async () => {
    const schemaResponse = await PinotMethodUtils.getQuerySchemaList();
    const schemaList = [];
    const schemaData = [];
    schemaResponse.records.map((record) => {
      schemaList.push(...record);
    });
    schemaList.map((schema) => {
      schemaData.push([schema].concat([...Array(PinotMethodUtils.allSchemaDetailsColumnHeader.length - 1)].map((e) => loading)));
    });
    const tablesResponse = await PinotMethodUtils.getQueryTablesList({
      bothType: true,
    });
    const tablesList = [];
    const tableData = [];
    tablesResponse.records.map((record) => {
      tablesList.push(...record);
    });
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

    // Set just the column headers so these do not have to load with the data
    setSchemaDetails({
      columns: PinotMethodUtils.allSchemaDetailsColumnHeader,
      records: schemaData,
      isLoading: false,
    });

    // these implicitly set isLoading=false by leaving it undefined
    const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList);
    setTableData(tableDetails);
    const schemaDetailsData = await PinotMethodUtils.getAllSchemaDetails(schemaList);
    setSchemaDetails(schemaDetailsData);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.operationDiv}>
        <SimpleAccordion headerTitle="Operations" showSearchBox={false}>
          <div>
            <CustomButton
              onClick={() => {
                setShowSchemaModal(true);
              }}
              tooltipTitle="Define the dimensions, metrics and date time columns of your data"
              enableTooltip={true}
            >
              Add Schema
            </CustomButton>
            <CustomButton
              onClick={() => {
                setShowAddOfflineTableModal(true);
              }}
              tooltipTitle="Create a Pinot table to ingest from batch data sources, such as S3"
              enableTooltip={true}
            >
              Add Offline Table
            </CustomButton>
            <CustomButton
              onClick={() => {
                setShowAddRealtimeTableModal(true);
              }}
              tooltipTitle="Create a Pinot table to ingest from stream data sources, such as Kafka"
              enableTooltip={true}
            >
              Add Realtime Table
            </CustomButton>
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
      />
      <CustomizedTables
        title="Schemas"
        data={schemaDetails}
        showSearchBox={true}
        inAccordionFormat={true}
        addLinks
        baseURL="/tenants/schema/"
      />
      {showSchemaModal && (
        <AddSchemaOp
          hideModal={() => {
            setShowSchemaModal(false);
          }}
          fetchData={fetchData}
        />
      )}
      {showAddOfflineTableModal && (
        <AddOfflineTableOp
          hideModal={() => {
            setShowAddOfflineTableModal(false);
          }}
          fetchData={fetchData}
          tableType={'OFFLINE'}
        />
      )}
      {showAddRealtimeTableModal && (
        <AddRealtimeTableOp
          hideModal={() => {
            setShowAddRealtimeTableModal(false);
          }}
          fetchData={fetchData}
          tableType={'REALTIME'}
        />
      )}
    </Grid>
  );
};

export default TablesListingPage;
