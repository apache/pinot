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
import { TableData } from 'Models';
import { getInstance } from '../../requests';
import CustomizedTables from '../Table';
import AppLoader from '../AppLoader';

type Props = {
  name: string,
  instances: string[]
};

const InstaceTable = ({ name, instances }: Props) => {

  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });

  useEffect(() => {

    const promiseArr = [
      ...instances.map(inst => getInstance(inst))
    ];

    Promise.all(promiseArr).then(result => {
      setTableData({
        columns: ['Name', 'Enabled', 'Hostname', 'Port', 'URI'],
        records: [
          ...result.map(({ data }) => (
            [data.instanceName, data.enabled, data.hostName, data.port, `${data.hostName}:${data.port}`]
          ))
        ]
      });
      setFetching(false);
    });
  }, [instances]);

  return (
    fetching ? <AppLoader /> :
    <CustomizedTables title={name} data={tableData} />
  );
};

export default InstaceTable;