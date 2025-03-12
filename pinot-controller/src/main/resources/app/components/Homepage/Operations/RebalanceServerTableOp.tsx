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
  DialogContentText,
  FormControl,
  FormControlLabel,
  Grid,
  Input,
  InputLabel,
  Switch,
  Box,
  Typography,
  List,
  ListItem,
  ListItemText,
  ListItemIcon
} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import CustomCodemirror from '../../CustomCodemirror';
import {RebalanceServerDialogHeader} from "./RebalanceServer/RebalanceServerDialogHeader";
import {RebalanceServerConfigurationSection} from "./RebalanceServer/RebalanceServerConfigurationSection";
import Alert from "@material-ui/lab/Alert";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";
import {rebalanceServerOptions} from "./RebalanceServer/RebalanceServerOptions";
import FiberManualRecordIcon from '@material-ui/icons/FiberManualRecord';
import {RebalanceServerConfigurationOption} from "./RebalanceServer/RebalanceServerConfigurationOption";

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
  const [rebalanceConfig, setRebalanceConfig] = React.useState(
      rebalanceServerOptions.reduce((config, option) => ({ ...config, [option.name]: option.defaultValue }), {})
  );

  const getData = () => {
    return {
      type: tableType,
      ...rebalanceConfig,
    }
  };

  const handleSave = async (event) => {
    const data = getData();
    const response = await PinotMethodUtils.rebalanceServersForTableOp(tableName, data);
    setRebalanceResponse(response);
  };

  const handleConfigChange = (config: { [key: string]: string | number | boolean }) => {
    setRebalanceConfig({
      ...rebalanceConfig,
      ...config
    });
  }

  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      title={<RebalanceServerDialogHeader />}
      handleSave={handleSave}
      showOkBtn={!rebalanceResponse}
    >
        {!rebalanceResponse ?
          <Box flexDirection="column">
            <RebalanceServerConfigurationSection sectionTitle='Preparation'>
              <Alert color='info' icon={<InfoOutlinedIcon fontSize='small' />}>
                <Typography variant='body2'>
                  It is strongly recommended to run rebalance with these options enabled prior to running the actual rebalance.
                  This is needed to verify that rebalance will do what's expected.
                </Typography>
                <List>
                  {rebalanceServerOptions
                      .filter(option => option.isStatsGatheringConfig)
                      .map(option => (
                          <ListItem key={option.name} style={{ padding: 0 }}>
                            <ListItemIcon style={{ minWidth: 30 }}>
                              <FiberManualRecordIcon color='primary' fontSize='small'/>
                            </ListItemIcon>
                            <ListItemText primaryTypographyProps={{ variant: 'body2' }} primary={option.label} />
                          </ListItem>
                      ))
                  }
                </List>
              </Alert>
            </RebalanceServerConfigurationSection>
            <RebalanceServerConfigurationSection sectionTitle='Basic Options'>
              <Grid container spacing={2}>
                {rebalanceServerOptions.filter(option => !option.isAdvancedConfig).map((option) => (
                    <Grid item>
                      <RebalanceServerConfigurationOption option={option} handleConfigChange={handleConfigChange} />
                    </Grid>
                ))}
              </Grid>
            </RebalanceServerConfigurationSection>
            <RebalanceServerConfigurationSection sectionTitle='Advanced Options' canHideSection showSectionByDefault={false}>
              <Grid container spacing={2}>
                {rebalanceServerOptions.filter(option => option.isAdvancedConfig).map((option) => (
                    <Grid item>
                      <RebalanceServerConfigurationOption option={option} handleConfigChange={handleConfigChange} />
                    </Grid>
                ))}
              </Grid>
            </RebalanceServerConfigurationSection>
          </Box>
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
    </Dialog>
  );
}