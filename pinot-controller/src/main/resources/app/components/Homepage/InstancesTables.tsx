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
import map from 'lodash/map';
import { getInstances } from '../../requests';
import AppLoader from '../AppLoader';
import InstaceTable from './InstanceTable';

type DataTable = {
  [name: string]: string[]
};

const Instances = () => {
  const [fetching, setFetching] = useState(true);
  const [instances, setInstances] = useState<DataTable>();

  useEffect(() => {
    getInstances().then(({ data }) => {
      const initialVal: DataTable = {};
      // It will create instances list array like
      // {Controller: ['Controller1', 'Controller2'], Broker: ['Broker1', 'Broker2']}
      const groupedData = data.instances.reduce((r, a) => {
        const y = a.split('_');
        const key = y[0].trim();
        r[key] = [...r[key] || [], a];
        return r;
      }, initialVal);

      setInstances(groupedData);
      setFetching(false);
    });
  }, []);

  return fetching ? (
    <AppLoader />
  ) : (
    <>
      {
        map(instances, (value, key) => {
          return <InstaceTable key={key} name={key} instances={value} />;
        })
      }
    </>
  );
};

export default Instances;