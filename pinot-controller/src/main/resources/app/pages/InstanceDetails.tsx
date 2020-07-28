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
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import { TableData } from 'Models';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import AppLoader from '../components/AppLoader';
import { RouteComponentProps } from 'react-router-dom';
import CustomizedTables from '../components/Table';
import SimpleAccordion from '../components/SimpleAccordion';

const useStyles = makeStyles((theme) => ({
  codeMirrorDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  codeMirror: {
    '& .CodeMirror': { maxHeight: 430, border: '1px solid #BDCCD9' },
  }
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default',
  readOnly: true
};

type Props = {
  instanceName: string
};

const InstanceDetails = ({ match }: RouteComponentProps<Props>) => {
  const classes = useStyles();
  const instanceName = match.params.instanceName;
  const clutserName = localStorage.getItem('pinot_ui:clusterName');
  const [fetching, setFetching] = useState(true);
  const [instanceConfig, setInstanceConfig] = useState(null);
  const [liveConfig, setLiveConfig] = useState(null);
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });
  
  const fetchData = async () => {
    const configResponse = await PinotMethodUtils.getInstanceConfig(clutserName, instanceName);
    const liveConfigResponse = await PinotMethodUtils.getLiveInstanceConfig(clutserName, instanceName);
    const tenantListResponse = await PinotMethodUtils.getTenantsFromInstance(instanceName);
    setInstanceConfig(JSON.stringify(configResponse, null , 2));
    setLiveConfig(JSON.stringify(liveConfigResponse, null , 2));
    if(tenantListResponse){
      fetchTableDetails(tenantListResponse);
    } else {
      setFetching(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const fetchTableDetails = (tenantList) => {
    const promiseArr = [];
    tenantList.map((tenantName) => {
      promiseArr.push(PinotMethodUtils.getTenantTableData(tenantName));
    });
    const tenantTableData = {
      columns: [],
      records: []
    };
    Promise.all(promiseArr).then((results)=>{
      results.map((result)=>{
        tenantTableData.columns = result.columns;
        tenantTableData.records.push(...result.records);
      });
      setTableData(tenantTableData);
      setFetching(false);
    });
  };

  return (
    fetching ? <AppLoader /> :
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
      <Grid container spacing={2}>
        <Grid item xs={liveConfig ? 6 : 12}>
          <div className={classes.codeMirrorDiv}>
            <SimpleAccordion
              headerTitle="Instance Config"
              showSearchBox={false}
            >
              <CodeMirror
                options={jsonoptions}
                value={instanceConfig}
                className={classes.codeMirror}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>
        {liveConfig ?
          <Grid item xs={6}>
            <div className={classes.codeMirrorDiv}>
              <SimpleAccordion
                headerTitle="LiveInstance Config"
                showSearchBox={false}
              >
                <CodeMirror
                  options={jsonoptions}
                  value={liveConfig}
                  className={classes.codeMirror}
                  autoCursor={false}
                />
              </SimpleAccordion>
            </div>
          </Grid>
        : null}
      </Grid>
      {tableData.columns.length ?
        <CustomizedTables
          title="Tables"
          data={tableData}
          isPagination
          addLinks
          baseURL={`/instance/${instanceName}/table/`}
          showSearchBox={true}
          inAccordionFormat={true}
        />
      : null}
    </Grid>
  );
};

export default InstanceDetails;