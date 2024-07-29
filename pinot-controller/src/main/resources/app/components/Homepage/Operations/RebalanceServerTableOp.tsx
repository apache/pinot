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
import { DialogContent, DialogContentText, FormControl, FormControlLabel, Grid, Input, InputLabel, Switch, Tooltip} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import CustomCodemirror from '../../CustomCodemirror';
import InfoOutlinedIcon from '@material-ui/icons/InfoOutlined';

type Props = {
  tableType: string,
  tableName: string,
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void
};

export default function RebalanceServerTableOp({
  hideModal,
  tableName,
  tableType
}: Props) {
  const [rebalanceResponse, setRebalanceResponse] = React.useState(null)
  const [dryRun, setDryRun] = React.useState(false);
  const [reassignInstances, setReassignInstances] = React.useState(false);
  const [includeConsuming, setIncludeConsuming] = React.useState(false);
  const [bootstrap, setBootstrap] = React.useState(false);
  const [downtime, setDowntime] = React.useState(false);
  const [minAvailableReplicas, setMinAvailableReplicas] = React.useState("1");
  const [bestEfforts, setBestEfforts] = React.useState(false);
  const [lowDiskMode, setLowDiskMode] = React.useState(false);

  const getData = () => {
    return {
      type: tableType,
      dryRun, reassignInstances, includeConsuming, bootstrap, downtime, bestEfforts, lowDiskMode,
      minAvailableReplicas: parseInt(minAvailableReplicas, 10)
    }
  };

  const handleSave = async (event) => {
    const data = getData();
    const response = await PinotMethodUtils.rebalanceServersForTableOp(tableName, data);
    setRebalanceResponse(response);
  };

  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      title={(<>Rebalance Server <Tooltip interactive title={(<a className={"tooltip-link"} target="_blank" href="https://docs.pinot.apache.org/operators/operating-pinot/rebalance/rebalance-servers">Click here for more details</a>)} arrow placement="top"><InfoOutlinedIcon/></Tooltip></>)}
      handleSave={handleSave}
      showOkBtn={!rebalanceResponse}
    >
      <DialogContent>
        {!rebalanceResponse ?
          <Grid container spacing={2}>
            <Grid item xs={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={dryRun}
                    onChange={() => setDryRun(!dryRun)}
                    name="dryRun"
                    color="primary"
                  />
                }
                label="Dry Run"
              />
              <br/>
              <FormControlLabel
                control={
                  <Switch
                    checked={includeConsuming}
                    onChange={() => setIncludeConsuming(!includeConsuming)} 
                    name="includeConsuming"
                    color="primary"
                  />
                }
                label="Include Consuming"
              />
              <br/>
              <FormControlLabel
                control={
                  <Switch
                    checked={downtime}
                    onChange={() => setDowntime(!downtime)} 
                    name="downtime"
                    color="primary"
                  />
                }
                label="Downtime"
              />
              <br />
              <FormControlLabel
                control={
                  <Switch
                    checked={lowDiskMode}
                    onChange={() => setLowDiskMode(!lowDiskMode)} 
                    name="lowDiskMode"
                    color="primary"
                  />
                }
                label="Low Disk Mode"
              />
            </Grid>
            <Grid item xs={6}>
              <FormControlLabel
                control={
                  <Switch
                    checked={reassignInstances}
                    onChange={() => setReassignInstances(!reassignInstances)} 
                    name="reassignInstances"
                    color="primary"
                  />
                }
                label="Reassign Instances"
              />
              <br/>
              <FormControlLabel
                control={
                  <Switch
                    checked={bootstrap}
                    onChange={() => setBootstrap(!bootstrap)}
                    name="bootstrap"
                    color="primary"
                  />
                }
                label="Bootstrap"
              />
              <br/>
              <FormControlLabel
                control={
                  <Switch
                    checked={bestEfforts}
                    onChange={() => setBestEfforts(!bestEfforts)} 
                    name="bestEfforts"
                    color="primary"
                  />
                }
                label="Best Efforts"
              />
            </Grid>
            <Grid item xs={6}>
              <FormControl fullWidth={true}>
                <InputLabel htmlFor="my-input">Minimum Available Replicas</InputLabel>
                <Input id="my-input" type="number" value={minAvailableReplicas} onChange={(e)=> setMinAvailableReplicas(e.target.value)}/>
              </FormControl>
            </Grid>
          </Grid>
        : 
          <React.Fragment>
            <DialogContentText>
              Operation Status:
            </DialogContentText>
            <CustomCodemirror
              data={rebalanceResponse}
              isEditable={false}
            />
          </React.Fragment>
        }
      </DialogContent>
    </Dialog>
  );
}