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

import React, {useState, useEffect} from 'react';
import { Grid, makeStyles } from '@material-ui/core';
import { TableData } from 'Models';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomizedTables from '../components/Table';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto'
  },

}));

const TablesListingPage = () => {
  const classes = useStyles();

  const [fetching, setFetching] = useState(true);
  const [tableData, setTableData] = useState<TableData>({
    columns: [],
    records: []
  });

  const fetchData = async () => {
    const tablesResponse = await PinotMethodUtils.getQueryTablesList({bothType: true});
    const tablesList = [];
    tablesResponse.records.map((record)=>{
      tablesList.push(...record);
    });
    const tableDetails = await PinotMethodUtils.getAllTableDetails(tablesList);
    setTableData(tableDetails);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return fetching ? (
    <AppLoader />
  ) : (
    <Grid item xs className={classes.gridContainer}>
      <CustomizedTables
        title="Tables"
        data={tableData}
        isPagination
        addLinks
        baseURL="/tenants/:Tenant Name:/table/" // TODO
        regexReplace={true}
        showSearchBox={true}
        inAccordionFormat={true}
      />
    </Grid>
  );
};

export default TablesListingPage;