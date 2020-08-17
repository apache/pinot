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
import AppLoader from '../AppLoader';
import InstanceTable from './InstanceTable';
import PinotMethodUtils from '../../utils/PinotMethodUtils';

type DataTable = {
  [name: string]: string[]
};

const Instances = () => {
  const [fetching, setFetching] = useState(true);
  const [instances, setInstances] = useState<DataTable>();
  const [clusterName, setClusterName] = useState('');

  const fetchData = async () => {
    const result = await PinotMethodUtils.getAllInstances();
    let clusterNameRes = localStorage.getItem('pinot_ui:clusterName');
    if(!clusterNameRes){
      clusterNameRes = await PinotMethodUtils.getClusterName();
    }
    setInstances(result);
    setClusterName(clusterNameRes);
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
  }, []);

  return fetching ? (
    <AppLoader />
  ) : (
    <>
      {
        map(instances, (value, key) => {
          return <InstanceTable key={key} name={key} instances={value} clusterName={clusterName} />;
        })
      }
    </>
  );
};

export default Instances;