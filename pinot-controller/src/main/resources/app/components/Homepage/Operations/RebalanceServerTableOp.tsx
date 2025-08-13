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

import React from 'react';
import {
  Grid, Box, Typography, Divider, Button, CircularProgress
} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import {RebalanceServerDialogHeader} from "./RebalanceServer/RebalanceServerDialogHeader";
import {
  RebalanceServerSection
} from "./RebalanceServer/RebalanceServerSection";
import Alert from "@material-ui/lab/Alert";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";
import {rebalanceServerOptions} from "./RebalanceServer/RebalanceServerOptions";
import {RebalanceServerConfigurationOption} from "./RebalanceServer/RebalanceServerConfigurationOption";
import {RebalanceResponse} from "./RebalanceServer/RebalanceResponse";
import {RebalanceServerStatusOp} from "./RebalanceServerStatusOp";

type Props = {
  tableType: string,
  tableName: string,
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void
};

const DryRunAction = ({ handleOnRun, disabled }: { handleOnRun: () => void, disabled?: boolean }) => {
  return (
      <Button disabled={disabled} onClick={handleOnRun} variant="contained" style={{ textTransform: 'none' }} color="primary">
        Dry Run
      </Button>
  );
}

const RebalanceAction = ({ handleOnRun, disabled }: { handleOnRun: () => void, disabled?: boolean }) => {
  return (
      <Button disabled={disabled} onClick={handleOnRun} variant="contained" style={{ textTransform: 'none' }} color="primary">
        Rebalance
      </Button>
  );
}

const BackAction = ({ onClick }: { onClick: () => void }) => {
  return (
      <Button onClick={onClick} variant="outlined" color="primary">
        Back
      </Button>
  );
}



export default function RebalanceServerTableOp({
  hideModal,
  tableName,
  tableType
}: Props) {
  const [pending, setPending] = React.useState(false);
  const [rebalanceResponse, setRebalanceResponse] = React.useState(null)
  const [rebalanceConfig, setRebalanceConfig] = React.useState(
      rebalanceServerOptions.reduce((config, option) => ({ ...config, [option.name]: option.defaultValue }), {})
  );
  const [isDryRun, setIsDryRun] = React.useState(false);
  const [dryRunCompleted, setDryRunCompleted] = React.useState(false);
  const [dryRunResponse, setDryRunResponse] = React.useState(null);
  const [showingDryRunResults, setShowingDryRunResults] = React.useState(false);
  const [showJobStatusDialog, setShowJobStatusDialog] = React.useState(false);
  const [selectedJobId, setSelectedJobId] = React.useState<string | null>(null);

  const getData = () => {
    return {
      type: tableType,
      ...rebalanceConfig,
    }
  };

  const handleSave = async () => {
    const data = getData();
    setPending(true);
    const response = await PinotMethodUtils.rebalanceServersForTableOp(tableName, data);

    if (response.error) {
      setRebalanceResponse({
        description: response.error,
        jobId: "NA",
        status: response.code
      })
    } else {
      setRebalanceResponse(response);
    }

    setShowingDryRunResults(false);
    setIsDryRun(false);
    setPending(false);
  };

    const handleDryRun = async () => {
    setIsDryRun(true);
    const data = getData();
    setPending(true);
    const response = await PinotMethodUtils.rebalanceServersForTableOp(tableName, {
      ...data,
      dryRun: true,
      preChecks: true
    });
    if (response.error) {
      setRebalanceResponse({
        description: response.error,
        jobId: "NA",
        status: response.code
      })
    } else {
      setRebalanceResponse(response);
      setDryRunResponse(response);
      setDryRunCompleted(true);
    }
    setShowingDryRunResults(true);
    setPending(false);
  };

  const handleConfigChange = (config: { [key: string]: string | number | boolean }) => {
    setRebalanceConfig({
      ...rebalanceConfig,
      ...config
    });
  }

  if (pending) {
    return (
        <Dialog
            showTitleDivider
            showFooterDivider
            size='md'
            open={true}
            handleClose={hideModal}
            title={<RebalanceServerDialogHeader />}
            showOkBtn={false}
        >
          <Box alignItems='center' display='flex' justifyContent='center'>
            <CircularProgress />
          </Box>
        </Dialog>
    )
  }

  const handleBackBtnOnClick = () => {
    setRebalanceResponse(null);
    setShowingDryRunResults(false);
    setIsDryRun(false);
  }

  const handleJobIdClick = (jobId: string) => {
    setSelectedJobId(jobId);
    setShowJobStatusDialog(true);
  }

  const handleCloseJobStatusDialog = () => {
    setShowJobStatusDialog(false);
    setSelectedJobId(null);
  }

  const getDialogActions = () => {
    if (!showingDryRunResults && !rebalanceResponse) {
      return <DryRunAction disabled={pending} handleOnRun={handleDryRun} />;
    }
    
    if (showingDryRunResults) {
      return (
        <Box display="flex">
          <BackAction onClick={handleBackBtnOnClick} />
          <Box ml={1}>
            <RebalanceAction disabled={pending} handleOnRun={handleSave} />
          </Box>
        </Box>
      );
    }
    
    return <BackAction onClick={handleBackBtnOnClick} />;
  };

  return (
    <React.Fragment>
      <Dialog
        showTitleDivider
        showFooterDivider
        size='md'
        okBtnDisabled={pending}
        open={true}
        handleClose={hideModal}
        title={<RebalanceServerDialogHeader />}
        showOkBtn={false}
        moreActions={getDialogActions()}
      >
          {!showingDryRunResults && !rebalanceResponse && (
            <Box flexDirection="column">
              <RebalanceServerSection sectionTitle='Basic Options'>
                <Grid container spacing={2}>
                  {rebalanceServerOptions.filter(option => !option.isAdvancedConfig && !option.isStatsGatheringConfig).map((option) => (
                      <Grid item xs={12} key={`basic-options-${option.name}`}>
                        <RebalanceServerConfigurationOption rebalanceConfig={rebalanceConfig} option={option} handleConfigChange={handleConfigChange} />
                      </Grid>
                  ))}
                </Grid>
              </RebalanceServerSection>
              <Divider style={{ marginBottom: 20 }}/>
              <RebalanceServerSection sectionTitle='Advanced Options' canHideSection showSectionByDefault={false}>
                <Grid container spacing={2}>
                  {rebalanceServerOptions.filter(option => option.isAdvancedConfig).map((option) => (
                      <Grid item xs={12} key={`advanced-options-${option.name}`}>
                        <RebalanceServerConfigurationOption rebalanceConfig={rebalanceConfig} option={option} handleConfigChange={handleConfigChange} />
                      </Grid>
                  ))}
                </Grid>
              </RebalanceServerSection>
            </Box>
          )}

          {showingDryRunResults && dryRunResponse && (
            <React.Fragment>
              <RebalanceResponse response={dryRunResponse} onJobIdClick={handleJobIdClick} />
            </React.Fragment>
          )}

          {rebalanceResponse && !showingDryRunResults && (
            <React.Fragment>
              <RebalanceResponse response={rebalanceResponse} onJobIdClick={handleJobIdClick} />
            </React.Fragment>
          )}
      </Dialog>
      {showJobStatusDialog && (
        <RebalanceServerStatusOp
          tableName={tableName}
          hideModal={handleCloseJobStatusDialog}
          initialJobId={selectedJobId}
        />
      )}
    </React.Fragment>
  );
}
