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
import { get, lowerCase, mapKeys, startCase } from 'lodash';
import { DataTable, InstanceType, TableData } from 'Models';
import CustomizedTables from './Table';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Utils from '../utils/Utils';
import Loading from './Loading';

type BaseProps = {
  instanceType: InstanceType;
  showInstanceDetails?: boolean;
  instanceNames: string[] | null;
  liveInstanceNames?: string[];
};

type ClusterProps = BaseProps & {
  cluster: string;
  tenant?: never;
};

type TenantProps = BaseProps & {
  tenant: string;
  cluster?: never;
};

type Props = ClusterProps | TenantProps;

export const AsyncInstanceTable = ({
  instanceType,
  cluster,
  instanceNames,
  liveInstanceNames,
  showInstanceDetails = false,
}: Props) => {
  const instanceColumns = showInstanceDetails
    ? ['Instance Name', 'Enabled', 'Hostname', 'Port', 'Status']
    : ['Instance Name'];
  const [instanceData, setInstanceData] = useState<TableData>(
    Utils.getLoadingTableData(instanceColumns)
  );

  useEffect(() => {
    if(instanceNames) {
      const loadingColumns = Array(instanceColumns.length - 1).fill(Loading);
      setInstanceData({
        columns: instanceColumns,
        records: instanceNames.map((name) => [name, ...loadingColumns]) || [],
      });
    }
  }, [instanceNames]);

  useEffect(() => {
    // async load all the other details
    if(showInstanceDetails && cluster && instanceNames && liveInstanceNames) {
      fetchAdditionalInstanceDetails();
    }
  }, [showInstanceDetails, cluster, instanceNames, liveInstanceNames]);

  const fetchAdditionalInstanceDetails = async () => {
    const additionalData = await PinotMethodUtils.getInstanceData(
      instanceNames,
      liveInstanceNames
    );
    setInstanceData(additionalData);
  }

  return (
    <CustomizedTables
      title={startCase(instanceType)}
      data={instanceData}
      addLinks
      baseURL="/instance/"
      showSearchBox={true}
      inAccordionFormat={true}
    />
  );
};
