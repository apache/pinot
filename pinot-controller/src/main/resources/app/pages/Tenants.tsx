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
import { Grid, makeStyles } from '@material-ui/core';
import { InstanceType } from 'Models';
import { RouteComponentProps } from 'react-router-dom';
import SimpleAccordion from '../components/SimpleAccordion';
import AsyncPinotTables from '../components/AsyncPinotTables';
import CustomButton from '../components/CustomButton';
import { AsyncInstanceTable } from '../components/AsyncInstanceTable';

const useStyles = makeStyles((theme) => ({
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
}));

type Props = {
  tenantName: string;
};

const TenantPage = ({ match }: RouteComponentProps<Props>) => {
  const { tenantName } = match.params;
  const classes = useStyles();

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
                console.log('rebalance');
              }}
              tooltipTitle="Recalculates the segment to server mapping for all tables in this tenant"
              enableTooltip={true}
              isDisabled={true}
            >
              Rebalance Server Tenant
            </CustomButton>
            <CustomButton
              onClick={() => {
                console.log('rebuild');
              }}
              tooltipTitle="Rebuilds brokerResource mappings for all tables in this tenant"
              enableTooltip={true}
              isDisabled={true}
            >
              Rebuild Broker Resource
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>
      <AsyncPinotTables
        title={tenantName}
        tenants={[tenantName]}
        baseUrl={`/tenants/${tenantName}/table/`}
      />
      <Grid container spacing={2}>
        <Grid item xs={6}>
          <AsyncInstanceTable
            instanceType={InstanceType.BROKER}
            tenant={tenantName}
          />
        </Grid>
        <Grid item xs={6}>
          <AsyncInstanceTable
            instanceType={InstanceType.SERVER}
            tenant={tenantName}
          />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default TenantPage;
