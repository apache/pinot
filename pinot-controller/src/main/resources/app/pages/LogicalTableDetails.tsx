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
import { RouteComponentProps, useHistory } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { Link } from 'react-router-dom';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import TableToolbar from '../components/TableToolbar';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomButton from '../components/CustomButton';
import EditConfigOp from '../components/Homepage/Operations/EditConfigOp';
import Confirm from '../components/Confirm';
import { NotificationContext } from '../components/Notification/NotificationContext';
import NotFound from '../components/NotFound';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';

const useStyles = makeStyles((theme) => ({
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
    marginBottom: 20,
  },
  link: {
    color: '#4285f4',
    textDecoration: 'none',
    '&:hover': {
      textDecoration: 'underline',
    },
  },
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true,
};

type Props = {
  logicalTableName: string;
};

const ConfigSection = ({ title, data, classes }: { title: string; data: any; classes: any }) => {
  if (!data) return null;
  return (
    <div className={classes.sqlDiv}>
      <SimpleAccordion headerTitle={title} showSearchBox={false}>
        <CodeMirror
          options={jsonoptions}
          value={JSON.stringify(data, null, 2)}
          className={classes.queryOutput}
          autoCursor={false}
        />
      </SimpleAccordion>
    </div>
  );
};

const LogicalTableDetails = ({ match }: RouteComponentProps<Props>) => {
  const { logicalTableName } = match.params;
  const classes = useStyles();
  const history = useHistory();
  const [fetching, setFetching] = useState(true);
  const [notFound, setNotFound] = useState(false);
  const [logicalTableConfig, setLogicalTableConfig] = useState<any>(null);
  const [configJSON, setConfigJSON] = useState('');

  const [showEditConfig, setShowEditConfig] = useState(false);
  const [config, setConfig] = useState('{}');
  const [confirmDialog, setConfirmDialog] = useState(false);
  const [dialogDetails, setDialogDetails] = useState(null);
  const { dispatch } = React.useContext(NotificationContext);

  const fetchData = async () => {
    setFetching(true);
    setNotFound(false);
    try {
      const result = await PinotMethodUtils.getLogicalTableConfig(logicalTableName);
      if (result.error || !result) {
        setNotFound(true);
      } else {
        const parsed = typeof result === 'string' ? JSON.parse(result) : result;
        setLogicalTableConfig(parsed);
        setConfigJSON(JSON.stringify(parsed, null, 2));
      }
    } catch (e) {
      setNotFound(true);
    }
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, [logicalTableName]);

  const handleConfigChange = (value: string) => {
    setConfig(value);
  };

  const saveConfigAction = async () => {
    try {
      const configObj = JSON.parse(config);
      const result = await PinotMethodUtils.updateLogicalTableConfig(logicalTableName, configObj);
      if (result.status) {
        dispatch({ type: 'success', message: result.status, show: true });
        fetchData();
        setShowEditConfig(false);
      } else {
        dispatch({ type: 'error', message: result.error || 'Failed to update', show: true });
      }
    } catch (e) {
      dispatch({ type: 'error', message: e.toString(), show: true });
    }
  };

  const handleDeleteAction = () => {
    setDialogDetails({
      title: 'Delete Logical Table',
      content: `Are you sure you want to delete the logical table "${logicalTableName}"?`,
      successCb: () => deleteLogicalTable(),
    });
    setConfirmDialog(true);
  };

  const deleteLogicalTable = async () => {
    try {
      const result = await PinotMethodUtils.deleteLogicalTableOp(logicalTableName);
      if (result.status) {
        dispatch({ type: 'success', message: result.status, show: true });
        closeDialog();
        setTimeout(() => {
          history.push('/tables');
        }, 1000);
      } else {
        dispatch({ type: 'error', message: result.error || 'Failed to delete', show: true });
        closeDialog();
      }
    } catch (e) {
      dispatch({ type: 'error', message: e.toString(), show: true });
      closeDialog();
    }
  };

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogDetails(null);
  };

  const buildPhysicalTablesData = () => {
    if (!logicalTableConfig?.physicalTableConfigMap) {
      return { columns: ['Physical Table Name', 'Multi-Cluster'], records: [] };
    }
    const records = Object.entries(logicalTableConfig.physicalTableConfigMap).map(
      ([tableName, config]: [string, any]) => {
        const isMultiCluster = config?.multiCluster === true;
        const nameCell = isMultiCluster
          ? tableName
          : {
              customRenderer: (
                <Link to={`/tenants/table/${tableName}`} className={classes.link}>
                  {tableName}
                </Link>
              ),
            };
        return [nameCell, isMultiCluster ? 'Yes' : 'No'];
      }
    );
    return {
      columns: ['Physical Table Name', 'Multi-Cluster'],
      records,
    };
  };

  if (fetching) {
    return <AppLoader />;
  }

  if (notFound) {
    return <NotFound message={`Logical table "${logicalTableName}" not found`} />;
  }

  const physicalTablesData = buildPhysicalTablesData();

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
        <SimpleAccordion headerTitle="Operations" showSearchBox={false}>
          <div>
            <CustomButton
              onClick={() => {
                setConfig(configJSON);
                setShowEditConfig(true);
              }}
              tooltipTitle="Edit the logical table configuration"
              enableTooltip={true}
            >
              Edit Config
            </CustomButton>
            <CustomButton
              onClick={handleDeleteAction}
              tooltipTitle="Delete this logical table"
              enableTooltip={true}
            >
              Delete Logical Table
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>

      <div className={classes.highlightBackground}>
        <TableToolbar name="Summary" showSearchBox={false} />
        <Grid container spacing={2} className={classes.body}>
          <Grid item xs={4}>
            <strong>Table Name:</strong> {logicalTableConfig.tableName}
          </Grid>
          <Grid item xs={4}>
            <strong>Broker Tenant:</strong> {logicalTableConfig.brokerTenant || 'N/A'}
          </Grid>
          <Grid item xs={4}>
            <strong>Physical Tables:</strong>{' '}
            {logicalTableConfig.physicalTableConfigMap
              ? Object.keys(logicalTableConfig.physicalTableConfigMap).length
              : 0}
          </Grid>
          {logicalTableConfig.refOfflineTableName && (
            <Grid item xs={4}>
              <strong>Ref Offline Table:</strong>{' '}
              <Link to={`/tenants/table/${logicalTableConfig.refOfflineTableName}`} className={classes.link}>
                {logicalTableConfig.refOfflineTableName}
              </Link>
            </Grid>
          )}
          {logicalTableConfig.refRealtimeTableName && (
            <Grid item xs={4}>
              <strong>Ref Realtime Table:</strong>{' '}
              <Link to={`/tenants/table/${logicalTableConfig.refRealtimeTableName}`} className={classes.link}>
                {logicalTableConfig.refRealtimeTableName}
              </Link>
            </Grid>
          )}
        </Grid>
      </div>

      <Grid container spacing={2}>
        <Grid item xs={6}>
          <CustomizedTables
            title="Physical Tables"
            data={physicalTablesData}
            showSearchBox={true}
            inAccordionFormat={true}
          />
          <ConfigSection title="Query Config" data={logicalTableConfig.query} classes={classes} />
          <ConfigSection title="Quota Config" data={logicalTableConfig.quota} classes={classes} />
          <ConfigSection title="Time Boundary Config" data={logicalTableConfig.timeBoundaryConfig} classes={classes} />
        </Grid>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion headerTitle="Logical Table Config" showSearchBox={false}>
              <CodeMirror
                options={jsonoptions}
                value={configJSON}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>
      </Grid>

      <EditConfigOp
        showModal={showEditConfig}
        hideModal={() => setShowEditConfig(false)}
        saveConfig={saveConfigAction}
        config={config}
        handleConfigChange={handleConfigChange}
      />
      {confirmDialog && dialogDetails && (
        <Confirm
          openDialog={confirmDialog}
          dialogTitle={dialogDetails.title}
          dialogContent={dialogDetails.content}
          successCallback={dialogDetails.successCb}
          closeDialog={closeDialog}
          dialogYesLabel="Yes"
          dialogNoLabel="No"
        />
      )}
    </Grid>
  );
};

export default LogicalTableDetails;
