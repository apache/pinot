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

import React, { useState, useEffect, useRef } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { Box, Button, FormControlLabel, Grid, Switch, Tooltip, Typography } from '@material-ui/core';
import { RouteComponentProps, useHistory, useLocation } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { DISPLAY_SEGMENT_STATUS, InstanceState, TableData, TableSegmentJobs, TableType } from 'Models';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import TableToolbar from '../components/TableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomButton from '../components/CustomButton';
import EditConfigOp from '../components/Homepage/Operations/EditConfigOp';
import ReloadStatusOp from '../components/Homepage/Operations/ReloadStatusOp';
import RebalanceServerTableOp from '../components/Homepage/Operations/RebalanceServerTableOp';
import Confirm from '../components/Confirm';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';
import InfoOutlinedIcon from '@material-ui/icons/InfoOutlined';
import { get, isEmpty } from "lodash";
import { SegmentStatusRenderer } from '../components/SegmentStatusRenderer';
import Skeleton from '@material-ui/lab/Skeleton';
import NotFound from '../components/NotFound';

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
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  },
  copyIdButton: {
    paddingBlock: 0,
    marginLeft: 10
  }
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true
};

type Props = {
  tenantName: string;
  tableName: string;
  instanceName: string;
};

type Summary = {
  tableName: string;
  reportedSize: number;
  estimatedSize: number;
};

const TenantPageDetails = ({ match }: RouteComponentProps<Props>) => {
  const { tenantName, tableName, instanceName } = match.params;
  const classes = useStyles();
  const history = useHistory();
  const location = useLocation();
  const [fetching, setFetching] = useState(true);
  const [tableNotFound, setTableNotFound] = useState(false);

  const initialTableSummary: Summary = {
    tableName: match.params.tableName,
    reportedSize: null,
    estimatedSize: null,
  };
  const [tableSummary, setTableSummary] = useState<Summary>(initialTableSummary);

  const [tableState, setTableState] = React.useState({
    enabled: true,
  });

  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogDetails, setDialogDetails] = React.useState(null);
  const {dispatch} = React.useContext(NotificationContext);

  const [showEditConfig, setShowEditConfig] = useState(false);
  const [config, setConfig] = useState('{}');

  const instanceColumns = ["Instance Name", "# of segments"];
  const loadingInstanceData = Utils.getLoadingTableData(instanceColumns);
  const [instanceCountData, setInstanceCountData] = useState<TableData>(loadingInstanceData);

  const segmentListColumns = ['Segment Name', 'Status'];
  const loadingSegmentList = Utils.getLoadingTableData(segmentListColumns);
  const [segmentList, setSegmentList] = useState<TableData>(loadingSegmentList);

  const [tableSchema, setTableSchema] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [tableType, setTableType] = useState('');
  const [tableConfig, setTableConfig] = useState('');
  const [schemaJSON, setSchemaJSON] = useState(null);
  const [actionType, setActionType] = useState(null);
  const [showReloadStatusModal, setShowReloadStatusModal] = useState(false);
  const [reloadStatusData, setReloadStatusData] = useState(null);
  const [tableJobsData, setTableJobsData] = useState<TableSegmentJobs | null>(null);
  const [showRebalanceServerModal, setShowRebalanceServerModal] = useState(false);
  const [schemaJSONFormat, setSchemaJSONFormat] = useState(false);

  const fetchTableData = async () => {
    // We keep all the fetching inside this component since we need to be able
    // to handle users making changes to the table and then reloading the json.
    setFetching(true);
    fetchSyncTableData().then(()=> {
      setFetching(false);
      if (!tableNotFound) {
        fetchAsyncTableData();
      }
    });
  };

  const fetchSyncTableData = async () => {
    return Promise.all([
      fetchTableSchema(),
      fetchTableJSON(),
    ]);
  }

  const fetchAsyncTableData = async () => {
    // set async data back to loading
    setTableSummary(initialTableSummary);
    setInstanceCountData(loadingInstanceData);
    setSegmentList(loadingSegmentList);

    // load async data
    PinotMethodUtils.getTableSummaryData(tableName).then((result) => {
      setTableSummary(result);
    });
    fetchSegmentData()
  }

  const fetchSegmentData = async () => {
    const result = await PinotMethodUtils.getSegmentList(tableName);
    const {columns, records, externalViewObj} = result;
    const instanceObj = {};
    externalViewObj && Object.keys(externalViewObj).map((segmentName)=>{
      const instanceKeys = Object.keys(externalViewObj[segmentName]);
      instanceKeys.map((instanceName)=>{
        if(!instanceObj[instanceName]){
          instanceObj[instanceName] = 0;
        }
        instanceObj[instanceName] += 1;
      })
    });
    const instanceRecords = [];
    Object.keys(instanceObj).map((instanceName)=>{
      instanceRecords.push([instanceName, instanceObj[instanceName]]);
    })
    setInstanceCountData({
      columns: instanceColumns,
      records: instanceRecords
    });

    const segmentTableRows = [];
    records.forEach(([name, status]) =>
      segmentTableRows.push([
        name,
        {
          customRenderer: (
            <SegmentStatusRenderer
              segmentName={name}
              tableName={tableName}
              status={status as DISPLAY_SEGMENT_STATUS}
            />
          ),
        },
      ])
    );

    setSegmentList({columns, records: segmentTableRows});
  };

  const fetchTableSchema = async () => {
    const result = await PinotMethodUtils.getTableSchemaData(tableName);
    if(result.error){
      setSchemaJSON(null);
      setTableSchema({
        columns: ['Column', 'Type', 'Field Type', 'Multi Value'],
        records: []
      });
    } else {
      setSchemaJSON(JSON.parse(JSON.stringify(result)));
      const tableSchema = Utils.syncTableSchemaData(result, true);
      setTableSchema(tableSchema);
    }
  };

  const fetchTableJSON = async () => {
    return PinotMethodUtils.getTableDetails(tableName).then((result) => {
      if(result.error){
        dispatch({type: 'error', message: result.error, show: true});
      } else {
        if (isEmpty(result)) {
          setTableNotFound(true);
          return;
        }
        const tableObj:any = result.OFFLINE || result.REALTIME;
        setTableType(tableObj.tableType);
        setTableConfig(JSON.stringify(result, null, 2));
        return fetchTableState(tableObj.tableType);
      }
    });
  };

  const fetchTableState = async (type) => {
    return PinotMethodUtils.getTableState(tableName, type)
      .then((stateResponse) => {
        return setTableState({enabled: stateResponse.state === 'enabled'});
      });
  };

  useEffect(() => {
    fetchTableData();
  }, []);

  const handleSwitchChange = (event) => {
    setDialogDetails({
      title: tableState.enabled ? 'Disable Table' : 'Enable Table',
      content: `Are you sure want to ${tableState.enabled ? 'disable' : 'enable'} this table?`,
      successCb: () => toggleTableState()
    });
    setConfirmDialog(true);
  };

  const toggleTableState = async () => {
    const result = await PinotMethodUtils.toggleTableState(tableName, tableState.enabled ? InstanceState.DISABLE : InstanceState.ENABLE, tableType.toLowerCase() as TableType);
    syncResponse(result);
  };

  const handleConfigChange = (value: string) => {
    setConfig(value);
  };

  const saveConfigAction = async () => {
    let configObj = JSON.parse(config);
    if(actionType === 'editTable'){
      if(configObj.OFFLINE || configObj.REALTIME){
        configObj = configObj.OFFLINE || configObj.REALTIME;
      }
      const result = await PinotMethodUtils.updateTable(tableName, configObj);
      syncResponse(result);
    } else if(actionType === 'editSchema'){
      const result = await PinotMethodUtils.updateSchema(schemaJSON.schemaName, configObj);
      syncResponse(result);
    }
  };

  const syncResponse = (result, customMessage?: React.ReactNode) => {
    if(result.status){
      dispatch({type: 'success', message: customMessage || result.status, show: true});
      fetchTableData();
      setShowEditConfig(false);
    } else {
      dispatch({type: 'error', message: result.error, show: true});
    }
    closeDialog();
  };

  const handleDeleteTableAction = () => {
    setDialogDetails({
      title: 'Delete Table',
      content: 'Are you sure want to delete this table? All data and configs will be deleted.',
      successCb: () => deleteTable()
    });
    setConfirmDialog(true);
  };

  const deleteTable = async () => {
    const result = await PinotMethodUtils.deleteTableOp(tableName);
    if(result.status){
      dispatch({type: 'success', message: result.status, show: true});
    } else {
      dispatch({type: 'error', message: result.error, show: true});
    }
    closeDialog();
    if(result.status){
      setTimeout(()=>{
        if(tenantName){
          history.push(Utils.navigateToPreviousPage(location, true));  
        } else {
          history.push('/tables');
        }
      }, 1000);
    }
  };

  const handleDeleteSchemaAction = () => {
    setDialogDetails({
      title: 'Delete Schema',
      content: 'Are you sure want to delete this schema? Any tables using this schema might not function correctly.',
      successCb: () => deleteSchema()
    });
    setConfirmDialog(true);
  };

  const deleteSchema = async () => {
    const result = await PinotMethodUtils.deleteSchemaOp(schemaJSON.schemaName);
    syncResponse(result);
  };

  const handleReloadSegments = () => {
    setDialogDetails({
      title: 'Reload all segments',
      content: 'Are you sure want to reload all the segments?',
      successCb: () => reloadSegments()
    });
    setConfirmDialog(true);
  };

  const reloadSegments = async () => {
    const result = await PinotMethodUtils.reloadAllSegmentsOp(tableName, tableType);

    let reloadJobId = null;

    try {
      // extract reloadJobId from response
      const statusResponseObj = JSON.parse(result.status)
      reloadJobId = get(statusResponseObj, `${tableName}.reloadJobId`, null)
    } catch {
      reloadJobId = null;
    }

    const handleCopyReloadJobId = () => {
      if(!reloadJobId) {
        return;
      }
      navigator.clipboard.writeText(reloadJobId);
    }

    const customMessage = (
      <Box>
        <Typography variant='inherit'>{result.status}</Typography>
        <Button 
          className={classes.copyIdButton} 
          variant="outlined" 
          color="inherit" 
          size="small" 
          onClick={handleCopyReloadJobId}
        >
          Copy Id
        </Button>
      </Box>
    )
    
    syncResponse(result, reloadJobId && customMessage);
  };

  const handleReloadStatus = async () => {
    try{
      setShowReloadStatusModal(true);
      const [reloadStatusData, tableJobsData] = await Promise.all([
        PinotMethodUtils.reloadStatusOp(tableName, tableType),
        PinotMethodUtils.fetchTableJobs(tableName, "RELOAD_SEGMENT"),
      ]);

      if(reloadStatusData.error || tableJobsData.error) {
        dispatch({type: 'error', message: reloadStatusData.error || tableJobsData.error, show: true});
        setShowReloadStatusModal(false);
        return;
      }
      
      setReloadStatusData(reloadStatusData);
      setTableJobsData(tableJobsData);
    } catch(error) {
      dispatch({type: 'error', message: error, show: true});
      setShowReloadStatusModal(false);
    }
  };

  const handleRebalanceBrokers = () => {
    setDialogDetails({
      title: (<>Rebalance brokers <Tooltip interactive title={(<a className={"tooltip-link"} target="_blank" href="https://docs.pinot.apache.org/operators/operating-pinot/rebalance/rebalance-brokers">Click here for more details</a>)} arrow placement="top"><InfoOutlinedIcon/></Tooltip></>),
      content: 'Are you sure want to rebalance the brokers?',
      successCb: () => rebalanceBrokers()
    });
    setConfirmDialog(true);
  };
  
  const rebalanceBrokers = async () => {
    const result = await PinotMethodUtils.rebalanceBrokersForTableOp(tableName);
    syncResponse(result);
  };

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogDetails(null);
  };

  if (fetching) {
    return <AppLoader />;
  } else if (tableNotFound) {
    return <NotFound message={`Table ${tableName} not found`} />;
  } else {
    return (
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
        <div className={classes.operationDiv}>
          <SimpleAccordion
            headerTitle="Operations"
            showSearchBox={false}
          >
            <div>
              <CustomButton
                onClick={()=>{
                  setActionType('editTable');
                  setConfig(tableConfig);
                  setShowEditConfig(true);
                }}
                tooltipTitle="Edit Table"
                enableTooltip={true}
              >
                Edit Table
              </CustomButton>
              <CustomButton
                onClick={handleDeleteTableAction}
                tooltipTitle="Delete Table"
                enableTooltip={true}
              >
                Delete Table
              </CustomButton>
              <CustomButton
                onClick={()=>{
                  setActionType('editSchema');
                  setConfig(JSON.stringify(schemaJSON, null, 2));
                  setShowEditConfig(true);
                }}
                tooltipTitle="Edit Schema"
                enableTooltip={true}
              >
                Edit Schema
              </CustomButton>
              <CustomButton
                isDisabled={!schemaJSON} onClick={handleDeleteSchemaAction}
                tooltipTitle="Delete Schema"
                enableTooltip={true}
              >
                Delete Schema
              </CustomButton>
              <CustomButton
                isDisabled={true} onClick={()=>{console.log('truncate table');}}
                // tooltipTitle="Truncate Table"
                // enableTooltip={true}
              >
                Truncate Table
              </CustomButton>
              <CustomButton
                onClick={handleReloadSegments}
                tooltipTitle="Reloads all segments of the table to apply changes such as indexing, column default values, etc"
                enableTooltip={true}
              >
                Reload All Segments
              </CustomButton>
              <CustomButton
                onClick={handleReloadStatus}
                tooltipTitle="The status of all indexes for each column"
                enableTooltip={true}
              >
                Reload Status
              </CustomButton>
              <CustomButton
                onClick={()=>{setShowRebalanceServerModal(true);}}
                tooltipTitle="Recalculates the segment to server mapping for this table"
                enableTooltip={true}
              >
                Rebalance Servers
              </CustomButton>
              <CustomButton
                onClick={handleRebalanceBrokers}
                tooltipTitle="Rebuilds brokerResource mapping for this table"
                enableTooltip={true}
              >
                Rebalance Brokers
              </CustomButton>
              <Tooltip title="Disabling will disable the table for queries, consumption and data push" arrow placement="top">
              <FormControlLabel
                control={
                  <Switch
                    checked={tableState.enabled}
                    onChange={handleSwitchChange}
                    name="enabled"
                    color="primary"
                  />
                }
                label="Enable"
              />
              </Tooltip>
            </div>
          </SimpleAccordion>
        </div>
        <div className={classes.highlightBackground}>
          <TableToolbar name="Summary" showSearchBox={false} />
          <Grid container spacing={2} alignItems="center" className={classes.body}>
            <Grid item xs={4}>
              <strong>Table Name:</strong> {tableSummary.tableName}
            </Grid>
            <Grid item container xs={4} wrap="nowrap" spacing={1}>
              <Grid item>
                <Tooltip title="Uncompressed size of all data segments with replication" arrow placement="top">
                  <strong>Reported Size:</strong>
                </Tooltip>
              </Grid>
              <Grid item>
                {/* Now Skeleton can be a block element because it's the only thing inside this grid item */}
                {tableSummary.reportedSize ?
                  Utils.formatBytes(tableSummary.reportedSize) :
                  <Skeleton variant="text" animation="wave" width={100} />
                }
              </Grid>
            </Grid>
            <Grid item container xs={4} wrap="nowrap" spacing={1}>
              <Grid item>
                <Tooltip title="Estimated size of all data segments with replication, in case any servers are not reachable for actual size" arrow placement="top-start">
                  <strong>Estimated Size: </strong>
                </Tooltip>
              </Grid>
              <Grid item>
                {/* Now Skeleton can be a block element because it's the only thing inside this grid item */}
                {tableSummary.estimatedSize ?
                  Utils.formatBytes(tableSummary.estimatedSize) :
                  <Skeleton variant="text" animation="wave" width={100} />
                }
              </Grid>
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
                  value={tableConfig}
                  className={classes.queryOutput}
                  autoCursor={false}
                />
              </SimpleAccordion>
            </div>
            <CustomizedTables
              title={"Segments - " + segmentList.records.length}
              data={segmentList}
              baseURL={
                tenantName && `/tenants/${tenantName}/table/${tableName}/` ||
                instanceName && `/instance/${instanceName}/table/${tableName}/` ||
                `/tenants/table/${tableName}/`
              }
              addLinks
              showSearchBox={true}
              inAccordionFormat={true}
            />
          </Grid>
          <Grid item xs={6}>
            {!schemaJSONFormat ?
              <CustomizedTables
                title="Table Schema"
                data={tableSchema}
                showSearchBox={true}
                inAccordionFormat={true}
                accordionToggleObject={{
                  toggleName: "JSON Format",
                  toggleValue: schemaJSONFormat,
                  toggleChangeHandler: ()=>{setSchemaJSONFormat(!schemaJSONFormat);}
                }}
              />
            :
            <div className={classes.sqlDiv}>
              <SimpleAccordion
                headerTitle="Table Schema"
                showSearchBox={false}
                accordionToggleObject={{
                  toggleName: "JSON Format",
                  toggleValue: schemaJSONFormat,
                  toggleChangeHandler: ()=>{setSchemaJSONFormat(!schemaJSONFormat);}
                }}
              >
                <CodeMirror
                  options={jsonoptions}
                  value={JSON.stringify(schemaJSON, null, 2)}
                  className={classes.queryOutput}
                  autoCursor={false}
                />
              </SimpleAccordion>
            </div>
            }
            <CustomizedTables
              title={"Instance Count - " + instanceCountData.records.length}
              data={instanceCountData}
              addLinks
              baseURL="/instance/"
              showSearchBox={true}
              inAccordionFormat={true}
            />
          </Grid>
        </Grid>
        <EditConfigOp
          showModal={showEditConfig}
          hideModal={()=>{setShowEditConfig(false);}}
          saveConfig={saveConfigAction}
          config={config}
          handleConfigChange={handleConfigChange}
        />
        {
          showReloadStatusModal &&
          <ReloadStatusOp
            hideModal={()=>{setShowReloadStatusModal(false); setReloadStatusData(null)}}
            reloadStatusData={reloadStatusData}
            tableJobsData={tableJobsData}
          />
        }
        {showRebalanceServerModal &&
          <RebalanceServerTableOp
            hideModal={()=>{setShowRebalanceServerModal(false)}}
            tableType={tableType.toUpperCase()}
            tableName={tableName}
          />
        }
        {confirmDialog && dialogDetails && <Confirm
          openDialog={confirmDialog}
          dialogTitle={dialogDetails.title}
          dialogContent={dialogDetails.content}
          successCallback={dialogDetails.successCb}
          closeDialog={closeDialog}
          dialogYesLabel='Yes'
          dialogNoLabel='No'
        />}
      </Grid>
    );
  }
};

export default TenantPageDetails;
