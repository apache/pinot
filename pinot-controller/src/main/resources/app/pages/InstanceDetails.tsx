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

import React, { useEffect, useState } from 'react';
import {
  FormControlLabel,
  Grid,
  makeStyles,
  Switch,
  Tooltip,
} from '@material-ui/core';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import { InstanceState, InstanceType } from 'Models';
import { RouteComponentProps } from 'react-router-dom';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import AppLoader from '../components/AppLoader';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
import EditTagsOp from '../components/Homepage/Operations/EditTagsOp';
import EditConfigOp from '../components/Homepage/Operations/EditConfigOp';
import { NotificationContext } from '../components/Notification/NotificationContext';
import { startCase } from 'lodash';
import Confirm from '../components/Confirm';
import { getInstanceTypeFromInstanceName } from '../utils/Utils';
import AsyncPinotTables from '../components/AsyncPinotTables';
import NotFound from '../components/NotFound';

const useStyles = makeStyles((theme) => ({
  codeMirrorDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  codeMirror: {
    '& .CodeMirror': { maxHeight: 430, border: '1px solid #BDCCD9' },
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
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
  instanceName: string;
};

const InstanceDetails = ({ match }: RouteComponentProps<Props>) => {
  const classes = useStyles();
  const { instanceName } = match.params;
  const instanceType = getInstanceTypeFromInstanceName(instanceName);
  const clusterName = localStorage.getItem('pinot_ui:clusterName');
  const [fetching, setFetching] = useState(true);
  const [instanceNotFound, setInstanceNotFound] = useState(false);
  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogDetails, setDialogDetails] = React.useState(null);

  const [instanceConfig, setInstanceConfig] = useState(null);
  const [liveConfig, setLiveConfig] = useState(null);
  const [instanceDetails, setInstanceDetails] = useState(null);
  const [tagsList, setTagsList] = useState([]);
  const [tagsErrorObj, setTagsErrorObj] = useState({
    isError: false,
    errorMessage: null,
  });
  const [config, setConfig] = useState('{}');

  const [state, setState] = React.useState({
    enabled: true,
  });

  const [showEditTag, setShowEditTag] = useState(false);
  const [showEditConfig, setShowEditConfig] = useState(false);
  const { dispatch } = React.useContext(NotificationContext);

  const fetchData = async () => {
    const configResponse = await PinotMethodUtils.getInstanceConfig(
      clusterName,
      instanceName
    );

    if (configResponse?.code === 404) {
      setInstanceNotFound(true);
    } else {
      const liveConfigResponse = await PinotMethodUtils.getLiveInstanceConfig(
        clusterName,
        instanceName
      );
      const instanceDetails = await PinotMethodUtils.getInstanceDetails(
        instanceName
      );
      setInstanceConfig(JSON.stringify(configResponse, null, 2));
      const instanceHost = instanceDetails.hostName.replace(
        `${startCase(instanceType.toLowerCase())}_`,
        ''
      );
      const instancePutObj = {
        host: instanceHost,
        port: instanceDetails.port,
        type: instanceType,
        tags: instanceDetails.tags,
        pools: instanceDetails.pools,
        grpcPort: instanceDetails.grpcPort,
        adminPort: instanceDetails.adminPort,
        queryServicePort: instanceDetails.queryServicePort,
        queryMailboxPort: instanceDetails.queryMailboxPort,
        queriesDisabled: instanceDetails.queriesDisabled,
      };
      setState({ enabled: instanceDetails.enabled });
      setInstanceDetails(JSON.stringify(instancePutObj, null, 2));
      setLiveConfig(JSON.stringify(liveConfigResponse, null, 2));
    }
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleTagsChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    tags: Array<string> | null
  ) => {
    isTagsValid(tags);
    setTagsList(tags);
  };

  const isTagsValid = (_tagsList) => {
    let isValid = true;
    setTagsErrorObj({ isError: false, errorMessage: null });
    _tagsList.map((tag) => {
      if (!isValid) {
        return;
      }
      if (instanceType === InstanceType.BROKER) {
        if (!tag.endsWith('_BROKER')) {
          isValid = false;
          setTagsErrorObj({
            isError: true,
            errorMessage: 'Tags should end with _BROKER.',
          });
        }
      } else if (instanceType === InstanceType.SERVER) {
        if (!tag.endsWith('_REALTIME') && !tag.endsWith('_OFFLINE')) {
          isValid = false;
          setTagsErrorObj({
            isError: true,
            errorMessage: 'Tags should end with _OFFLINE or _REALTIME.',
          });
        }
      }
    });
    return isValid;
  };

  const saveTagsAction = async (event, typedTag) => {
    let newTagsList = [...tagsList];
    if (typedTag.length > 0) {
      newTagsList.push(typedTag);
    }
    if (!isTagsValid(newTagsList)) {
      return;
    }
    const result = await PinotMethodUtils.updateTags(instanceName, newTagsList);
    if (result.status) {
      dispatch({ type: 'success', message: result.status, show: true });
      fetchData();
    } else {
      dispatch({ type: 'error', message: result.error, show: true });
    }
    setShowEditTag(false);
  };

  const handleDropAction = () => {
    setDialogDetails({
      title: 'Drop Instance',
      content: 'Are you sure want to drop this instance?',
      successCb: () => dropInstance(),
    });
    setConfirmDialog(true);
  };

  const dropInstance = async () => {
    const result = await PinotMethodUtils.deleteInstance(instanceName);
    if (result.status) {
      dispatch({ type: 'success', message: result.status, show: true });
      fetchData();
    } else {
      dispatch({ type: 'error', message: result.error, show: true });
    }
    closeDialog();
  };

  const handleSwitchChange = (event) => {
    setDialogDetails({
      title: state.enabled ? 'Disable Instance' : 'Enable Instance',
      content: `Are you sure want to ${
        state.enabled ? 'disable' : 'enable'
      } this instance?`,
      successCb: () => toggleInstanceState(),
    });
    setConfirmDialog(true);
  };

  const toggleInstanceState = async () => {
    const result = await PinotMethodUtils.toggleInstanceState(
      instanceName,
      state.enabled ? InstanceState.DISABLE : InstanceState.ENABLE
    );
    if (result.status) {
      dispatch({ type: 'success', message: result.status, show: true });
      fetchData();
    } else {
      dispatch({ type: 'error', message: result.error, show: true });
    }
    setState({ enabled: !state.enabled });
    closeDialog();
  };

  const handleConfigChange = (value: string) => {
    setConfig(value);
  };

  const saveConfigAction = async () => {
    if (JSON.parse(config)) {
      const result = await PinotMethodUtils.updateInstanceDetails(
        instanceName,
        config
      );
      if (result.status) {
        dispatch({ type: 'success', message: result.status, show: true });
        fetchData();
      } else {
        dispatch({ type: 'error', message: result.error, show: true });
      }
      setShowEditConfig(false);
    }
  };

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogDetails(null);
  };

  if (fetching) {
    return <AppLoader />;
  } else if (instanceNotFound) {
    return <NotFound message={`Instance ${instanceName} not found`} />;
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
        {!instanceName.toLowerCase().startsWith('controller') && (
          <div className={classes.operationDiv}>
            <SimpleAccordion headerTitle="Operations" showSearchBox={false}>
              <div>
                <CustomButton
                  onClick={() => {
                    setTagsList(
                      JSON.parse(instanceConfig)?.listFields?.TAG_LIST || []
                    );
                    setShowEditTag(true);
                  }}
                  tooltipTitle="Add/remove tags from this node"
                  enableTooltip={true}
                >
                  Edit Tags
                </CustomButton>
                <CustomButton
                  onClick={() => {
                    setConfig(instanceDetails);
                    setShowEditConfig(true);
                  }}
                  enableTooltip={true}
                >
                  Edit Config
                </CustomButton>
                <CustomButton
                  onClick={handleDropAction}
                  tooltipTitle={
                    instanceType !== InstanceType.MINION
                      ? 'Removes the node from the cluster. Untag and rebalance (to ensure the node is not being used by any table) and shutdown the instance before dropping.'
                      : ''
                  }
                  enableTooltip={true}
                >
                  Drop
                </CustomButton>
                <Tooltip
                  title="Disabling will disable the node for queries."
                  arrow
                  placement="top-start"
                >
                  <FormControlLabel
                    control={
                      <Switch
                        checked={state.enabled}
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
        )}
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
          {liveConfig ? (
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
          ) : null}
        </Grid>
        {instanceType == InstanceType.BROKER ||
        instanceType == InstanceType.SERVER ? (
          <AsyncPinotTables
            title="Tables"
            baseUrl={`/instance/${instanceName}/table/`}
            instance={instanceName}
          />
        ) : null}
        <EditTagsOp
          showModal={showEditTag}
          hideModal={() => {
            setShowEditTag(false);
          }}
          saveTags={saveTagsAction}
          tags={tagsList}
          handleTagsChange={handleTagsChange}
          error={tagsErrorObj}
        />
        <EditConfigOp
          showModal={showEditConfig}
          hideModal={() => {
            setShowEditConfig(false);
          }}
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
  }
};

export default InstanceDetails;
