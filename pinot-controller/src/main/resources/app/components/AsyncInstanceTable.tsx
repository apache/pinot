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
import { InstanceType, TableData } from 'Models';
import CustomizedTables from './Table';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Utils from '../utils/Utils';

type BaseProps = {
  instanceType: InstanceType;
  showInstanceDetails?: boolean;
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
  tenant,
  showInstanceDetails = false,
}: Props) => {
  const instanceColumns = showInstanceDetails
    ? ['Instance Name', 'Enabled', 'Hostname', 'Port', 'Status']
    : ['Instance Name'];
  const [instanceData, setInstanceData] = useState<TableData>(
    Utils.getLoadingTableData(instanceColumns)
  );

  const fetchInstances = async (
    instanceType: InstanceType,
    tenant?: string
  ): Promise<string[]> => {
    if (tenant) {
      if (instanceType === InstanceType.BROKER) {
        return PinotMethodUtils.getBrokerOfTenant(tenant).then(
          (brokersData) => {
            return Array.isArray(brokersData) ? brokersData : [];
          }
        );
      } else if (instanceType === InstanceType.SERVER) {
        return PinotMethodUtils.getServerOfTenant(tenant).then(
          (serversData) => {
            return Array.isArray(serversData) ? serversData : [];
          }
        );
      }
    } else {
      return fetchInstancesOfType(instanceType);
    }
  };

  const fetchInstancesOfType = async (instanceType: InstanceType) => {
    return PinotMethodUtils.getAllInstances().then((instancesData) => {
      const lowercaseInstanceData = mapKeys(instancesData, (value, key) =>
        lowerCase(key)
      );
      return get(lowercaseInstanceData, lowerCase(instanceType));
    });
  };

  useEffect(() => {
    const instances = fetchInstances(instanceType, tenant);
    if (showInstanceDetails) {
      const instanceDetails = instances.then(async (instancesData) => {
        const liveInstanceArr = await PinotMethodUtils.getLiveInstance(cluster);
        return PinotMethodUtils.getInstanceData(
          instancesData,
          liveInstanceArr.data
        );
      });
      instanceDetails.then((instanceDetailsData) => {
        setInstanceData(instanceDetailsData);
      });
    } else {
      instances.then((instancesData) => {
        setInstanceData({
          columns: instanceColumns,
          records: instancesData.map((instance) => [instance]),
        });
      });
    }
  }, [instanceType, cluster, tenant, showInstanceDetails]);

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
