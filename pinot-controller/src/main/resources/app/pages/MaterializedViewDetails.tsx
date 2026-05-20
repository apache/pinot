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
import { RouteComponentProps, useHistory } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import { TableData } from 'Models';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomButton from '../components/CustomButton';
import Confirm from '../components/Confirm';
import NotFound from '../components/NotFound';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';
import { getMaterializedView, deleteMaterializedView } from '../requests';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/sql/sql';
import 'codemirror/mode/javascript/javascript';

const useStyles = makeStyles(() => ({
  gridContainer: {
    padding: 20,
    backgroundColor: 'white',
    maxHeight: 'calc(100vh - 70px)',
    overflowY: 'auto',
  },
  block: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20,
  },
  sqlOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 220 },
  },
  jsonOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 320 },
  },
  summary: {
    padding: 12,
  },
  kv: {
    display: 'grid',
    gridTemplateColumns: '220px 1fr',
    rowGap: 6,
    columnGap: 16,
  },
  label: {
    color: '#7f8a96',
    fontWeight: 500,
  },
  value: {
    fontFamily: 'monospace',
  },
  controls: {
    marginBottom: 12,
    display: 'flex',
    gap: 12,
  },
}));

const sqlOptions = {
  lineNumbers: true,
  mode: 'text/x-sql',
  styleActiveLine: true,
  theme: 'default',
  readOnly: true,
};

const jsonOptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  theme: 'default',
  readOnly: true,
};

const PARTITION_COLUMNS = ['Bucket Start', 'State', 'Segment Count', 'CRC', 'Last Refresh'];

interface MaterializedViewDefinition {
  materializedViewTableName: string;
  definedSQL: string;
  baseTables: string[];
  partitionExprMaps: Record<string, string>;
  stalenessThresholdMs: number;
  validPartitions: number;
  stalePartitions: number;
  totalPartitions: number;
  splitSpec: {
    sourceTimeColumn: string;
    sourceTimeFormat: string;
    materializedViewTimeColumn: string;
    bucketMs: number;
  } | null;
}

interface MaterializedViewRuntimePartition {
  bucketStartMs: number;
  state: string;
  segmentCount: number;
  crc: number;
  lastRefreshTime: number;
}

interface MaterializedViewRuntime {
  watermarkMs?: number;
  partitions?: MaterializedViewRuntimePartition[];
  absent?: boolean;
}

type Props = {
  materializedViewTableName: string;
};

const MaterializedViewDetails = ({ match }: RouteComponentProps<Props>) => {
  const { materializedViewTableName } = match.params;
  const classes = useStyles();
  const history = useHistory();
  const { dispatch } = React.useContext(NotificationContext);

  const [fetching, setFetching] = useState(true);
  const [notFound, setNotFound] = useState(false);
  const [definition, setDefinition] = useState<MaterializedViewDefinition | null>(null);
  const [runtime, setRuntime] = useState<MaterializedViewRuntime | null>(null);
  const [confirmDialog, setConfirmDialog] = useState(false);
  // Bumped by the Reload button to re-run the load effect; using a nonce instead of a hand-
  // mirrored fetch keeps the cancellation contract (the effect's cleanup flips `cancelled`
  // on unmount or dep-change) in one place.
  const [reloadNonce, setReloadNonce] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setFetching(true);
    setNotFound(false);
    getMaterializedView(materializedViewTableName)
      .then((res) => {
        // Guard: if the user navigated to a different MV or triggered another reload while
        // this request was in flight, discard the response so we never paint stale data
        // under a new URL / older snapshot.
        if (cancelled) {
          return;
        }
        setDefinition(res.data?.definition || null);
        setRuntime(res.data?.runtime || null);
        setFetching(false);
      })
      .catch((e: any) => {
        if (cancelled) {
          return;
        }
        if (e?.response?.status === 404) {
          setNotFound(true);
        } else {
          dispatch({
            type: 'error',
            message: e?.response?.data?.error || e?.message || 'Failed to load',
            show: true,
          });
        }
        setFetching(false);
      });
    return () => {
      cancelled = true;
    };
  }, [materializedViewTableName, reloadNonce, dispatch]);

  const reload = () => setReloadNonce((n) => n + 1);

  const buildPartitionRows = (): TableData => {
    const partitions = runtime?.partitions ?? [];
    const records = partitions.map((p) => [
      Utils.formatEpochMillis(p.bucketStartMs),
      p.state,
      p.segmentCount,
      p.crc,
      Utils.formatEpochMillis(p.lastRefreshTime),
    ]);
    return { columns: PARTITION_COLUMNS, records };
  };

  const drop = async () => {
    try {
      await deleteMaterializedView(materializedViewTableName);
      setConfirmDialog(false);
      dispatch({ type: 'success', message: `Dropped ${materializedViewTableName}`, show: true });
      // Navigate immediately rather than via setTimeout — the success toast is rendered by
      // the listing page just as well, and a delayed push leaks the timer if the user
      // navigates manually in the meantime.
      history.push('/materialized-views');
    } catch (e: any) {
      const detail = e?.response?.data?.error || e?.message || 'Failed to drop';
      dispatch({ type: 'error', message: detail, show: true });
      setConfirmDialog(false);
    }
  };

  if (fetching && !definition) {
    return <AppLoader />;
  }
  if (notFound) {
    return <NotFound message={`Materialized view "${materializedViewTableName}" was not found.`} />;
  }

  return (
    <Grid item xs className={classes.gridContainer}>
      <div className={classes.controls}>
        <CustomButton onClick={reload}>Reload</CustomButton>
        <CustomButton
          isDisabled={false}
          onClick={() => setConfirmDialog(true)}
          tooltipTitle="Drop this materialized view and its segments"
          enableTooltip={true}
        >
          Drop MV
        </CustomButton>
      </div>

      <div className={classes.block}>
        <SimpleAccordion headerTitle="Summary" showSearchBox={false}>
          <div className={classes.summary}>
            <div className={classes.kv}>
              <span className={classes.label}>Name</span>
              <span className={classes.value}>{definition?.materializedViewTableName || materializedViewTableName}</span>
              <span className={classes.label}>Base tables</span>
              <span className={classes.value}>
                {Array.isArray(definition?.baseTables) ? definition!.baseTables.join(', ') : '—'}
              </span>
              <span className={classes.label}>Watermark</span>
              <span className={classes.value}>{Utils.formatEpochMillis(runtime?.watermarkMs)}</span>
              <span className={classes.label}>Staleness SLO (ms)</span>
              <span className={classes.value}>{definition?.stalenessThresholdMs ?? 0}</span>
              <span className={classes.label}>Partitions (total)</span>
              <span className={classes.value}>{definition?.totalPartitions ?? 0}</span>
              <span className={classes.label}>Partitions (VALID)</span>
              <span className={classes.value}>{definition?.validPartitions ?? 0}</span>
              <span className={classes.label}>Partitions (STALE)</span>
              <span className={classes.value}>{definition?.stalePartitions ?? 0}</span>
            </div>
          </div>
        </SimpleAccordion>
      </div>

      <div className={classes.block}>
        <SimpleAccordion headerTitle="Defined SQL" showSearchBox={false}>
          <CodeMirror
            options={sqlOptions}
            value={definition?.definedSQL || ''}
            className={classes.sqlOutput}
            autoCursor={false}
          />
        </SimpleAccordion>
      </div>

      <div className={classes.block}>
        <SimpleAccordion headerTitle="Definition + Partition Summary (JSON)" showSearchBox={false}>
          <CodeMirror
            options={jsonOptions}
            value={JSON.stringify(definition || {}, null, 2)}
            className={classes.jsonOutput}
            autoCursor={false}
          />
        </SimpleAccordion>
      </div>

      <div className={classes.block}>
        <CustomizedTables
          title="Partitions"
          data={buildPartitionRows()}
          showSearchBox={true}
          inAccordionFormat={true}
        />
      </div>

      <Confirm
        openDialog={confirmDialog}
        dialogTitle="Drop materialized view?"
        dialogContent={`This will delete ${materializedViewTableName} and all its segments. This action cannot be undone.`}
        successCallback={drop}
        closeDialog={() => setConfirmDialog(false)}
        dialogYesLabel="Drop"
        dialogNoLabel="Cancel"
      />
    </Grid>
  );
};

export default MaterializedViewDetails;
