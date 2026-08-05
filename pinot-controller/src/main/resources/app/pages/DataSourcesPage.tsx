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
import { Grid, makeStyles, Paper } from '@material-ui/core';
import { Link } from 'react-router-dom';
import Skeleton from '@material-ui/lab/Skeleton';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { NotificationContext } from '../components/Notification/NotificationContext';
import { getMaterializedViewList } from '../requests';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto',
  },
  paper: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    padding: '24px 0',
    height: '100%',
    color: '#4285f4',
    borderRadius: 4,
    marginBottom: 15,
    textAlign: 'center',
    backgroundColor: 'rgba(66, 133, 244, 0.1)',
    borderColor: 'rgba(66, 133, 244, 0.5)',
    borderStyle: 'solid',
    borderWidth: '1px',
    '& h2, h4, p': { margin: 0 },
    '& h4': {
      textTransform: 'uppercase',
      letterSpacing: 1,
      fontWeight: 600,
    },
    '& p': {
      marginTop: 10,
      color: '#5f6b7a',
      fontSize: 13,
      maxWidth: 320,
    },
    '&:hover': { borderColor: '#4285f4' },
  },
  paperLinks: {
    textDecoration: 'none',
    height: '100%',
  },
}));

/**
 * Hub page that groups together the queryable surfaces ("data sources") exposed by the
 * cluster. Currently lists physical tables and materialized views; new card entries can
 * be added here as additional data-source types are introduced.
 */
const DataSourcesPage = () => {
  const classes = useStyles();
  const { dispatch } = React.useContext(NotificationContext);

  const [tablesCount, setTablesCount] = useState<number | null>(null);
  const [mvCount, setMvCount] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;

    PinotMethodUtils.getQueryTablesList({ bothType: true })
      .then((res: any) => {
        if (cancelled) {
          return;
        }
        setTablesCount(res?.records?.length ?? 0);
      })
      .catch((e: any) => {
        if (cancelled) {
          return;
        }
        setTablesCount(0);
        dispatch({
          type: 'error',
          message: `Failed to load tables count: ${e?.message || e}`,
          show: true,
        });
      });

    getMaterializedViewList()
      .then((res) => {
        if (cancelled) {
          return;
        }
        const mvs = (res.data && res.data.materializedViews) || [];
        setMvCount(mvs.length);
      })
      .catch((e: any) => {
        if (cancelled) {
          return;
        }
        setMvCount(0);
        dispatch({
          type: 'error',
          message: `Failed to load materialized views count: ${e?.response?.data?.error || e?.message || e}`,
          show: true,
        });
      });

    return () => {
      cancelled = true;
    };
  }, [dispatch]);

  const loading = <Skeleton animation="wave" width={50} />;

  return (
    <Grid item xs className={classes.gridContainer}>
      <Grid container spacing={3}>
        <Grid item xs={12} sm={6}>
          <Link to="/tables" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Tables</h4>
              <h2>{tablesCount === null ? loading : tablesCount}</h2>
              <p>Physical real-time and offline tables that store the underlying segments and serve queries directly.</p>
            </Paper>
          </Link>
        </Grid>
        <Grid item xs={12} sm={6}>
          <Link to="/materialized-views" className={classes.paperLinks}>
            <Paper className={classes.paper}>
              <h4>Materialized Views</h4>
              <h2>{mvCount === null ? loading : mvCount}</h2>
              <p>Derived, pre-aggregated tables refreshed by minion tasks to accelerate recurring queries.</p>
            </Paper>
          </Link>
        </Grid>
      </Grid>
    </Grid>
  );
};

export default DataSourcesPage;
