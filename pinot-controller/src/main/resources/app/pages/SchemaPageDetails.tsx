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
import { Checkbox, DialogContent, FormControlLabel, Grid, IconButton, Tooltip } from '@material-ui/core';
import { RouteComponentProps, useHistory } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { TableData } from 'Models';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';
import CustomButton from '../components/CustomButton';
import Confirm from '../components/Confirm';
import CustomCodemirror from '../components/CustomCodemirror';
import CustomDialog from '../components/CustomDialog';
import { HelpOutlineOutlined } from '@material-ui/icons';

const useStyles = makeStyles(() => ({
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
  schemaName: string;
  instanceName: string;
};

type Summary = {
    schemaName: string;
  reportedSize: string | number;
  estimatedSize: string | number;
};

const SchemaPageDetails = ({ match }: RouteComponentProps<Props>) => {
  const { schemaName } = match.params;
  const classes = useStyles();
  const history = useHistory();
  const [fetching, setFetching] = useState(true);
  const [] = useState<Summary>({
    schemaName: match.params.schemaName,
    reportedSize: '',
    estimatedSize: '',
  });

  const [] = React.useState({
    enabled: true,
  });

  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogDetails, setDialogDetails] = React.useState(null);
  const {dispatch} = React.useContext(NotificationContext);

  const [showEditConfig, setShowEditConfig] = useState(false);
  const [config, setConfig] = useState('{}');

  const [tableSchema, setTableSchema] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [tableConfig, setTableConfig] = useState('');
  const [schemaJSON, setSchemaJSON] = useState(null);
  const [actionType,setActionType] = useState(null);
  const [schemaJSONFormat, setSchemaJSONFormat] = useState(false);
  const [reloadSegmentsOnUpdate, setReloadSegmentsOnUpdate] = useState(false);

  const fetchTableSchema = async () => {
    const result = await PinotMethodUtils.getTableSchemaData(schemaName);
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
    fetchTableJSON();
  };

  const fetchTableJSON = async () => {
    const result = await PinotMethodUtils.getSchemaData(schemaName);
    setTableConfig(JSON.stringify(result, null, 2));
    setFetching(false);
  };

  useEffect(()=>{
    fetchTableSchema();
  },[])

  const handleConfigChange = (value: string) => {
    setConfig(value);
  };

  const saveConfigAction = async () => {
    let configObj = JSON.parse(config);
    if(actionType === 'editTable'){
      if(configObj.OFFLINE || configObj.REALTIME){
        configObj = configObj.OFFLINE || configObj.REALTIME;
      }
      const result = await PinotMethodUtils.updateTable(schemaName, configObj);
      syncResponse(result);
    } else if(actionType === 'editSchema'){
      const result = await PinotMethodUtils.updateSchema(schemaJSON.schemaName, configObj, reloadSegmentsOnUpdate);
      syncResponse(result);
    }
  };

  const syncResponse = (result) => {
    if(result.status){
      dispatch({type: 'success', message: result.status, show: true});
      setShowEditConfig(false);
      fetchTableJSON();
      setReloadSegmentsOnUpdate(false);
    } else {
      dispatch({type: 'error', message: result.error, show: true});
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
    history.push('/tables');
  };

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogDetails(null);
  };

  const handleSegmentDialogHide = () => {
    setShowEditConfig(false);
    setReloadSegmentsOnUpdate(false);
  }

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
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
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
            </div>
        </SimpleAccordion>
      </div>
      <Grid container spacing={2}>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Schema Json"
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
        </Grid>
        <Grid item xs={6}>
          {!schemaJSONFormat ?
            <CustomizedTables
              title="Table Schema"
              data={tableSchema}
              showSearchBox={true}
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
        </Grid>
      </Grid>
      {/* Segment config edit dialog */}
      <CustomDialog
        open={showEditConfig}
        handleClose={handleSegmentDialogHide}
        title="Edit Schema"
        handleSave={saveConfigAction}
      >
        <DialogContent>
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                color="primary"
                checked={reloadSegmentsOnUpdate}
                onChange={(e) => setReloadSegmentsOnUpdate(e.target.checked)}
                name="reload"
              />
            }
            label="Reload all segments"
          />
          <IconButton size="small">
            <Tooltip
              title="Reload all segments to make updated schema effective for already ingested data."
              arrow
              placement="top"
            >
              <HelpOutlineOutlined fontSize="small" />
            </Tooltip>
          </IconButton>
          <CustomCodemirror
            data={config}
            isEditable={true}
            returnCodemirrorValue={(newValue) => {
              handleConfigChange(newValue);
            }}
          />
        </DialogContent>
      </CustomDialog>

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
};

export default SchemaPageDetails;
