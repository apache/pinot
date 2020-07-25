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
import { makeStyles } from '@material-ui/core/styles';
import { Grid } from '@material-ui/core';
import { RouteComponentProps } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { TableData } from 'Models';
import _ from 'lodash';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import TableToolbar from '../components/TableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';

const useStyles = makeStyles((theme) => ({
  root: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  highlightBackground: {
    border: '1px #4285f4 solid',
    backgroundColor: 'rgba(66, 133, 244, 0.05)',
    borderRadius: 4,
    marginBottom: '20px',
  },
  body: {
    borderTop: '1px solid #BDCCD9',
    fontSize: '16px',
    lineHeight: '3rem',
    paddingLeft: '15px',
  },
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
  },
  sqlDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default',
};

type Props = {
  tenantName: string;
  tableName: string;
  instanceName: string;
};

type Summary = {
  tableName: string;
  reportedSize: string | number;
  estimatedSize: string | number;
};

const TenantPageDetails = ({ match }: RouteComponentProps<Props>) => {
  const { tenantName, tableName, instanceName } = match.params;
  const classes = useStyles();
  const [fetching, setFetching] = useState(true);
  const [tableSummary, setTableSummary] = useState<Summary>({
    tableName: match.params.tableName,
    reportedSize: '',
    estimatedSize: '',
  });

  const [segmentList, setSegmentList] = useState<TableData>({
    columns: [],
    records: [],
  });

  const [tableSchema, setTableSchema] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [value, setValue] = useState('');

  const fetchTableData = async () => {
    const result = await PinotMethodUtils.getTableSummaryData(tableName);
    setTableSummary(result);
    fetchSegmentData();
  };

  const fetchSegmentData = async () => {
    const result = await PinotMethodUtils.getSegmentList(tableName);
    setSegmentList(result);
    fetchTableSchema();
  };

  const fetchTableSchema = async () => {
    const result = await PinotMethodUtils.getTableSchemaData(tableName, true);
    setTableSchema(result);
    fetchTableJSON();
  };

  const fetchTableJSON = async () => {
    const result = await PinotMethodUtils.getTableDetails(tableName);
    setValue(JSON.stringify(result, null, 2));
    setFetching(false);
  };
  useEffect(() => {
    fetchTableData();
  }, []);
  return fetching ? (
    <AppLoader />
  ) : (
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
      <div className={classes.highlightBackground}>
        <TableToolbar name="Summary" showSearchBox={false} />
        <Grid container className={classes.body}>
          <Grid item xs={4}>
            <strong>Table Name:</strong> {tableSummary.tableName}
          </Grid>
          <Grid item xs={4}>
            <strong>Reported Size:</strong> {tableSummary.reportedSize}
          </Grid>
          <Grid item xs={4}>
            <strong>Estimated Size: </strong>
            {tableSummary.estimatedSize}
          </Grid>
        </Grid>
      </div>

      <Grid container spacing={2}>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Table Config"
              showSearchBox={false}
            >
              <CodeMirror
                options={jsonoptions}
                value={value}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
          <CustomizedTables
            title="Segments"
            data={segmentList}
            isPagination={false}
            noOfRows={segmentList.records.length}
            baseURL={
              tenantName ? `/tenants/${tenantName}/table/${tableName}/` :  `/instance/${instanceName}/table/${tableName}/`}
            addLinks
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
        <Grid item xs={6}>
          <CustomizedTables
            title="Table Schema"
            data={tableSchema}
            isPagination={false}
            noOfRows={tableSchema.records.length}
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TenantPageDetails;
