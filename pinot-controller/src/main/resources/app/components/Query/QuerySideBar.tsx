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

import * as React from 'react';

import { createStyles, Theme, makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import CssBaseline from '@material-ui/core/CssBaseline';
import { Grid } from '@material-ui/core';
import { TableData } from 'Models';
import CustomizedTables from '../Table';

const drawerWidth = 300;

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      display: 'flex',
    },
    appBar: {
      zIndex: theme.zIndex.drawer + 1,
    },
    drawer: {
      width: drawerWidth,
      height: 'calc(100vh - 70px)',
      flexShrink: 0,
      backgroundColor: '#333333',
    },
    drawerPaper: {
      position: 'unset',
      width: drawerWidth,
    },
    drawerContainer: {
      overflow: 'auto',
      paddingTop: '20px'
    },
    content: {
      flexGrow: 1,
      padding: theme.spacing(3),
    },
    itemContainer: {
      color: '#3B454E',
      borderRadius: '4px'
    },
    selectedItem: {
      background: '#D8E1E8!important'
    },
    link: {
      textDecoration: 'none'
    },
    leftPanel: {
      width: 300,
      padding: '0 20px',
      wordBreak: 'break-all',
    },
  }),
);

type Props = {
  tableList: TableData;
  fetchSQLData: Function;
  tableSchema: TableData;
  selectedTable: string;
  queryLoader: boolean;
};

const Sidebar = ({ tableList, fetchSQLData, tableSchema, selectedTable, queryLoader }: Props) => {
  const classes = useStyles();

  return (
    <>
      <CssBaseline />
      <Drawer
        open={false}
        className={classes.drawer}
        variant="permanent"
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        <div className={classes.drawerContainer}>
          <Grid item xs className={classes.leftPanel}>
            <CustomizedTables
              title="Tables"
              data={tableList}
              cellClickCallback={fetchSQLData}
              isCellClickable
              showSearchBox={true}
              inAccordionFormat
            />

            {!queryLoader && tableSchema.records.length ? (
              <CustomizedTables
                title={`${selectedTable} schema`}
                data={tableSchema}
                highlightBackground
                showSearchBox={true}
                inAccordionFormat
              />
            ) : null}
          </Grid>
        </div>
      </Drawer>
    </>
  );
};

export default Sidebar;
