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

import React, { useState } from 'react';
import { Grid, makeStyles } from '@material-ui/core';
import CustomButton from '../components/CustomButton';
import SimpleAccordion from '../components/SimpleAccordion';
import AddSchemaOp from '../components/Homepage/Operations/AddSchemaOp';
import AddOfflineTableOp from '../components/Homepage/Operations/AddOfflineTableOp';
import AddRealtimeTableOp from '../components/Homepage/Operations/AddRealtimeTableOp';
import AsyncPinotTables from '../components/AsyncPinotTables';
import { AsyncPinotSchemas } from '../components/AsyncPinotSchemas';

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

const TablesListingPage = () => {
  const classes = useStyles();

  const [showSchemaModal, setShowSchemaModal] = useState(false);
  const [showAddOfflineTableModal, setShowAddOfflineTableModal] = useState(
    false
  );
  const [showAddRealtimeTableModal, setShowAddRealtimeTableModal] = useState(
    false
  );
  // This is used to refresh the tables and schemas data after a new table or schema is added.
  // This is quite hacky, but it's simpler than trying to useRef or useContext to maintain
  // a link between this component and the child table and schema components.
  const [tablesKey, setTablesKey] = useState(0);
  const [schemasKey, setSchemasKey] = useState(0);

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
      <AsyncPinotTables
        key={`table-${tablesKey}`}
        title="Tables"
        baseUrl="/tenants/table/"
      />
      <AsyncPinotSchemas key={`schema-${schemasKey}`} />
      {showSchemaModal && (
        <AddSchemaOp
          hideModal={() => {
            setShowSchemaModal(false);
          }}
          fetchData={() => {
            setSchemasKey((prevKey) => prevKey + 1);
          }}
        />
      )}
      {showAddOfflineTableModal && (
        <AddOfflineTableOp
          hideModal={() => {
            setShowAddOfflineTableModal(false);
          }}
          fetchData={() => {
            setTablesKey((prevKey) => prevKey + 1);
          }}
          tableType={'OFFLINE'}
        />
      )}
      {showAddRealtimeTableModal && (
        <AddRealtimeTableOp
          hideModal={() => {
            setShowAddRealtimeTableModal(false);
          }}
          fetchData={() => {
            setTablesKey((prevKey) => prevKey + 1);
          }}
          tableType={'REALTIME'}
        />
      )}
    </Grid>
  );
};

export default TablesListingPage;
