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
import { Grid, makeStyles } from '@material-ui/core';
import { TableData } from 'Models';
import CustomizedTables from '../components/Table';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';
import { getMaterializedViewList } from '../requests';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto',
  },
}));

const COLUMNS = [
  'Materialized View',
  'Base Tables',
  'Watermark',
  'VALID',
  'STALE',
  'Total',
  'Last Refresh',
  'Staleness SLO (ms)',
  'Status',
];

const MaterializedViewListingPage = () => {
  const classes = useStyles();
  const { dispatch } = React.useContext(NotificationContext);
  const [data, setData] = useState<TableData>({ columns: COLUMNS, records: [] });

  useEffect(() => {
    let mounted = true;
    getMaterializedViewList()
      .then((res) => {
        if (!mounted) {
          return;
        }
        const mvs = (res.data && res.data.materializedViews) || [];
        const records = mvs.map((m: any) => {
          // The server emits placeholder entries `{materializedViewTableName, error}` for
          // znodes it could not summarize (e.g. malformed metadata). Carry the `error` message
          // into a Status column so operators can spot the problem without having to look at
          // controller logs.
          if (m.error) {
            return [
              m.materializedViewTableName || '',
              '—',
              '—',
              '—',
              '—',
              '—',
              '—',
              '—',
              `ERROR: ${m.error}`,
            ];
          }
          return [
            m.materializedViewTableName || '',
            Array.isArray(m.baseTables) ? m.baseTables.join(', ') : '',
            Utils.formatEpochMillis(m.watermarkMs),
            m.validPartitions ?? 0,
            m.stalePartitions ?? 0,
            m.totalPartitions ?? 0,
            Utils.formatEpochMillis(m.lastRefreshTime),
            m.stalenessThresholdMs ?? 0,
            'OK',
          ];
        });
        setData({ columns: COLUMNS, records });
      })
      .catch((err) => {
        if (!mounted) {
          return;
        }
        // Surface the failure rather than rendering an empty table (which would look identical
        // to "no MVs configured"). Operators using the UI to diagnose a problem need to be able
        // to tell those two states apart.
        const detail = err?.response?.data?.error || err?.message || String(err);
        dispatch({ type: 'error', message: `Failed to load materialized views: ${detail}`, show: true });
        setData({ columns: COLUMNS, records: [] });
      });
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <Grid item xs className={classes.gridContainer}>
      <CustomizedTables
        title="Materialized Views"
        data={data}
        addLinks
        baseURL="/materialized-views/"
        showSearchBox={true}
        inAccordionFormat={true}
      />
    </Grid>
  );
};

export default MaterializedViewListingPage;
