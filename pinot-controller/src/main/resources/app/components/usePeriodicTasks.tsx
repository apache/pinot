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
import { DataTable } from 'Models';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from './Table';

const PeriodicTaskTable = ({ tableData }) => {
  return !!tableData?.records?.length && (
    <CustomizedTables
      title="Periodic Tasks"
      data={tableData}
      showSearchBox={true}
      inAccordionFormat={true}
      isPagination={false}
    />
  );
};

export default function usePeriodicTasks(props) {
  const { shouldFetchData } = props;
  const [periodicTaskNames, setPeriodicTaskNames] = useState<DataTable>({ records: [], columns: [] });

  const fetchData = async () => {
    if (!shouldFetchData) return;
    const periodicTaskNames = await PinotMethodUtils.getAllPeriodicTaskNames();
    setPeriodicTaskNames(periodicTaskNames);
  };

  useEffect(() => {
    fetchData();
  }, [shouldFetchData]);

  return {
    periodicTaskNames,
    setPeriodicTaskNames,
    content: (
      <PeriodicTaskTable tableData={periodicTaskNames} />
    )
  };
}
