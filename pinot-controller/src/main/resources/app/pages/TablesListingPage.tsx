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

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  }
}));

const TableTooltipData = [
  null,
  "Uncompressed size of all data segments",
  "Estimated size of all data segments, in case any servers are not reachable for actual size",
  null,
  "GOOD if all replicas of all segments are up"
];

const TablesListingPage = () => {
  const classes = useStyles();

  const [fetching, setFetching] = useState(true);
  const [schemaDetails,setSchemaDetails] = useState<TableData>({
    columns: [],
    records: []
  });
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });
  const [showSchemaModal, setShowSchemaModal] = useState(false);
  const [showAddOfflineTableModal, setShowAddOfflineTableModal] = useState(false);
  const [showAddRealtimeTableModal, setShowAddRealtimeTableModal] = useState(false);

  const fetchData = async () => {
    !fetching && setFetching(true);
    const tablesResponse = await PinotMethodUtils.getQueryTablesList({bothType: true});
    const tablesList = [];
    tablesResponse.records.map((record)=>{
      tablesList.push(...record);
    });
    const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList);
    const schemaDetailsData = await PinotMethodUtils.getAllSchemaDetails();
    setTableData(tableDetails);
    setSchemaDetails(schemaDetailsData)
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return fetching ? (
    <AppLoader />
  ) : (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
            <CustomButton
              onClick={()=>{setShowSchemaModal(true)}}
              tooltipTitle="Define the dimensions, metrics and date time columns of your data"
              enableTooltip={true}
            >
              Add Schema
            </CustomButton>
            <CustomButton
              onClick={()=>{setShowAddOfflineTableModal(true)}}
              tooltipTitle="Create a Pinot table to ingest from batch data sources, such as S3"
              enableTooltip={true}
            >
              Add Offline Table
            </CustomButton>
            <CustomButton
              onClick={()=>{setShowAddRealtimeTableModal(true)}}
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
        isPagination
        addLinks
        baseURL="/tenants/table/"
        showSearchBox={true}
        inAccordionFormat={true}
        tooltipData={TableTooltipData}
      />
      <CustomizedTables
          title="Schemas"
          data={schemaDetails}
          isPagination
          showSearchBox={true}
          inAccordionFormat={true}
          addLinks
          baseURL="/tenants/schema/"
      />
      {
        showSchemaModal &&
        <AddSchemaOp
          hideModal={()=>{setShowSchemaModal(false);}}
          fetchData={fetchData}
        />
      }
      {
        showAddOfflineTableModal &&
        <AddOfflineTableOp
          hideModal={()=>{setShowAddOfflineTableModal(false);}}
          fetchData={fetchData}
          tableType={"OFFLINE"}
        />
      }
      {
        showAddRealtimeTableModal &&
        <AddRealtimeTableOp
          hideModal={()=>{setShowAddRealtimeTableModal(false);}}
          fetchData={fetchData}
          tableType={"REALTIME"}
        />
      }
    </Grid>
  );
};

export default TablesListingPage;