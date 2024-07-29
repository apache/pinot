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
import { startCase } from 'lodash';
import { AsyncInstanceTable } from '../AsyncInstanceTable';
import { InstanceType } from 'Models';

type Props = {
  clusterName: string;
  instanceType?: InstanceType;
};


const Instances = ({ clusterName, instanceType }: Props) => {
  const order = [
    InstanceType.CONTROLLER,
    InstanceType.BROKER,
    InstanceType.SERVER,
    InstanceType.MINION,
  ];
  return (
    <>
      {order
        .filter((key) => !instanceType || instanceType === key)
        .map((key) => {
          return (
            <AsyncInstanceTable
              key={startCase(key)}
              cluster={clusterName}
              instanceType={key}
              showInstanceDetails
            />
          );
        })}
    </>
  );
};

export default Instances;
