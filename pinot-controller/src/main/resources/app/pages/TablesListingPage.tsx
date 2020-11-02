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
import AddTableSchemaOp from '../components/Homepage/Operations/AddTableSchemaOp';
import AddTableOp from '../components/Homepage/Operations/AddTableOp';

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

const TablesListingPage = () => {
  const classes = useStyles();

  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });
  const [showAddTableSchemaModal, setShowAddTableSchemaModal] = useState(false);
  const [showAddTableModal, setShowAddTableModal] = useState(false);

  const fetchData = async () => {
    !fetching && setFetching(true);
    const tablesResponse = await PinotMethodUtils.getQueryTablesList({bothType: true});
    const tablesList = [];
    tablesResponse.records.map((record)=>{
      tablesList.push(...record);
    });
    const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList);
    setTableData(tableDetails);
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
              onClick={()=>{setShowAddTableSchemaModal(true)}}
              tooltipTitle="Add Schema & Table"
              enableTooltip={true}
            >
              Add Schema & Table
            </CustomButton>
            <CustomButton
              onClick={()=>{setShowAddTableModal(true)}}
              tooltipTitle="Add Table"
              enableTooltip={true}
            >
              Add Table
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
      />
      {
        showAddTableSchemaModal &&
        <AddTableSchemaOp
          hideModal={()=>{setShowAddTableSchemaModal(false);}}
          fetchData={fetchData}
        />
      }
      {
        showAddTableModal &&
        <AddTableOp
          hideModal={()=>{setShowAddTableModal(false);}}
          fetchData={fetchData}
        />
      }
    </Grid>
  );
};

export default TablesListingPage;