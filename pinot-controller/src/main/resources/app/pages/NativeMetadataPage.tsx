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
import {
  Box, Button, CircularProgress, Grid, Paper, Tab, Table, TableBody, TableCell, TableHead, TableRow,
  Tabs, Typography, makeStyles
} from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import {
  NativeMetadataResponse, NativeParticipants, NativeWriteCapabilities, NativeWriteMarker,
  nativeEditorJson, pendingNativeWrite, readNativeMetadata, readNativeWriteCapabilities
} from '../requests/nativeMetadata';
import NativeMetadataEditor, { NativeEditDraft } from '../components/NativeMetadataEditor';
import NativeTableSize from '../components/NativeTableSize';

const useStyles = makeStyles({
  page: { flex: 1, minWidth: 0, padding: 24, maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' },
  json: { whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', margin: 0, fontSize: 13 },
  revision: { overflowWrap: 'anywhere', marginTop: 12 },
});

const SECTIONS = ['Tables', 'Schemas', 'Cluster configuration', 'Membership'];

// This page deliberately mounts no Helix table/instance widgets. Published config and lease membership are
// metadata facts; neither establishes segment ONLINE state, serving authority, or query readiness.
const NativeMetadataPage = () => {
  const classes = useStyles();
  const [section, setSection] = useState(0);
  const [refresh, setRefresh] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<NativeMetadataResponse<any> | null>(null);
  const [capabilities, setCapabilities] = useState<NativeWriteCapabilities | null>(null);
  const [capabilityError, setCapabilityError] = useState<string | null>(null);
  const [draft, setDraft] = useState<NativeEditDraft | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [pending, setPending] = useState<NativeWriteMarker | null>(() => pendingNativeWrite());
  const [inspectedPending, setInspectedPending] = useState(false);
  const [acknowledgedPending, setAcknowledgedPending] = useState(false);
  const [writeStatus, setWriteStatus] = useState<string | null>(null);
  const writesBlocked = loading || !!error || !!draft || (!!pending && !acknowledgedPending);

  useEffect(() => {
    let cancelled = false;
    setCapabilities(null);
    setCapabilityError(null);
    readNativeWriteCapabilities().then((value) => {
      if (!cancelled) {
        setCapabilities(value);
      }
    }).catch((failure) => {
      if (!cancelled) {
        setCapabilityError(failure instanceof Error ? failure.message : 'Metadata editing is unavailable.');
      }
    });
    return () => { cancelled = true; };
  }, [refresh]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setResult(null);
    const path = section === 0
      ? (selected ? `/tables/${encodeURIComponent(selected)}` : '/tables?type=OFFLINE')
      : section === 1
        ? (selected ? `/schemas/${encodeURIComponent(selected)}` : '/schemas')
        : section === 2 ? '/cluster/configs' : '/metadata/participants';
    readNativeMetadata(path).then((response) => {
      if (!cancelled) {
        setResult(response);
        if (pending && !editorOpen) {
          setInspectedPending(true);
        }
      }
    }).catch((failure) => {
      if (!cancelled) {
        setError(failure instanceof Error ? failure.message : 'Unable to read committed metadata.');
      }
    }).finally(() => {
      if (!cancelled) {
        setLoading(false);
      }
    });
    return () => { cancelled = true; };
  }, [section, selected, refresh]);

  const changeSection = (_event: React.ChangeEvent<{}>, next: number) => {
    setSelected(null);
    setResult(null);
    setSection(next);
  };

  const openDraft = (operation: NativeEditDraft) => {
    setWriteStatus(null);
    setDraft(operation);
    setEditorOpen(true);
  };

  const editLoaded = () => {
    if (!result?.etag) {
      return;
    }
    try {
      const cluster = section === 2;
      openDraft({ title: cluster ? 'Merge cluster configuration' : `Edit ${selected}`,
        method: cluster ? 'POST' : 'PUT',
        path: cluster ? '/cluster/configs' : `/${section === 0 ? 'tables' : 'schemas'}/${encodeURIComponent(selected)}`,
        etag: result.etag, create: cluster && result.etag.endsWith(':absent"'),
        json: nativeEditorJson(result, section === 0) });
    } catch (_) {
      setWriteStatus('This JSON cannot be edited without changing its numeric values. Use an API client that preserves them.');
    }
  };

  const inspectPending = () => {
    setEditorOpen(false);
    setInspectedPending(false);
    setRefresh((value) => value + 1);
  };

  const renderActions = () => {
    if (!capabilities || !result) {
      return null;
    }
    return <Box display="flex" mb={2}>
      {section < 2 && !selected && (section === 0 ? capabilities.tableCreate : capabilities.schemaCreate) === true
        && <Button color="primary" variant="outlined" disabled={writesBlocked} onClick={() => openDraft({
          title: section === 0 ? 'Create OFFLINE table' : 'Create schema', method: 'POST',
          path: section === 0 ? '/tables' : '/schemas?override=false', create: true,
          json: section === 0 ? '{\n  "tableName": "",\n  "tableType": "OFFLINE",\n  "segmentsConfig": {"replication": "1"}\n}'
            : '{\n  "schemaName": "",\n  "dimensionFieldSpecs": [],\n  "metricFieldSpecs": [],\n  "dateTimeFieldSpecs": []\n}',
        })}>{section === 0 ? 'Create OFFLINE table' : 'Create schema'}</Button>}
      {result.etag && ((section === 0 && selected && capabilities.tableUpdate === true)
        || (section === 1 && selected && capabilities.schemaUpdate === true)
        || (section === 2 && capabilities.clusterConfigUpdate === true))
        && <Button color="primary" variant="outlined" disabled={writesBlocked} onClick={editLoaded}>
          {section === 2 ? 'Merge configuration' : 'Edit JSON'}
        </Button>}
      {section === 1 && selected && result.etag && capabilities.schemaDelete === true
        && <Button disabled={writesBlocked} onClick={() => openDraft({ title: `Delete schema ${selected}`,
          method: 'DELETE', path: `/schemas/${encodeURIComponent(selected)}`, create: false, etag: result.etag })}>
          Delete schema
        </Button>}
      {section === 2 && result.etag && capabilities.clusterConfigDelete === true && Object.keys(result.value).length > 0
        && <Button disabled={writesBlocked} onClick={() => openDraft({ title: 'Delete cluster configuration key',
          method: 'DELETE', path: '/cluster/configs', create: false, etag: result.etag,
          clusterKeys: Object.keys(result.value) })}>Delete key</Button>}
    </Box>;
  };

  const renderMembership = (membership: NativeParticipants) => (
    <>
      <Typography variant="body2">
        Lease state at committed round {membership.control?.round ?? 'uninitialized'}. Refresh to read a new snapshot.
        A live lease or a ready flag does not establish query readiness.
      </Typography>
      <Table size="small" aria-label="Native membership">
        <TableHead><TableRow>
          <TableCell>Participant</TableCell><TableCell>Role</TableCell><TableCell>Lease at snapshot</TableCell>
          <TableCell>Expiry round</TableCell><TableCell>Ready</TableCell><TableCell>Draining</TableCell>
          <TableCell>Registration epoch</TableCell>
        </TableRow></TableHead>
        <TableBody>{membership.participants.map((member) => (
          <TableRow key={member.participant.participantId}>
            <TableCell>{member.participant.participantId}
              <Typography variant="caption" component="div">Incarnation: {member.participant.incarnation}</Typography>
            </TableCell>
            <TableCell>{member.participant.role}</TableCell>
            <TableCell>{member.retired ? 'Retired' : member.live ? 'Live' : 'Expired'}</TableCell>
            <TableCell>{member.expiryRound}</TableCell><TableCell>{member.ready ? 'Yes' : 'No'}</TableCell>
            <TableCell>{member.draining ? 'Yes' : 'No'}</TableCell>
            <TableCell>{member.participant.registrationEpoch}</TableCell>
          </TableRow>
        ))}</TableBody>
      </Table>
      {membership.participants.length === 0 && <Typography>No registered participants.</Typography>}
    </>
  );

  const renderResult = () => {
    if (!result) {
      return null;
    }
    if (section === 3) {
      return renderMembership(result.value as NativeParticipants);
    }
    if (section < 2 && !selected) {
      const names: string[] = section === 0 ? result.value.tables : result.value;
      return names.length === 0 ? <Typography>No {SECTIONS[section].toLowerCase()} published.</Typography> : (
        <Table size="small" aria-label={SECTIONS[section]}>
          <TableHead><TableRow><TableCell>Name</TableCell></TableRow></TableHead>
          <TableBody>{names.map((name) => (
            <TableRow key={name}><TableCell>
              <Button color="primary" onClick={() => { setResult(null); setSelected(name); }}>{name}</Button>
            </TableCell></TableRow>
          ))}</TableBody>
        </Table>
      );
    }
    return <pre className={classes.json}>{result.json}</pre>;
  };

  return (
    <Grid item className={classes.page}>
      <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
        <Typography variant="h5">Ratis metadata</Typography>
        <Button variant="outlined" disabled={loading} onClick={() => setRefresh((value) => value + 1)}>Refresh</Button>
      </Box>
      <Alert severity="info">
        Browse and update validated metadata. Query serving and segment lifecycle actions are disabled.
        Updates may be rejected while native segment content or operations depend on the configuration.
      </Alert>
      {capabilityError && <Box mt={2}><Alert severity="warning">{capabilityError}</Alert></Box>}
      {writeStatus && <Box mt={2}><Alert severity="info">{writeStatus}</Alert></Box>}
      {pending && <Box mt={2}><Alert severity="warning">
        Outcome unknown for {pending.method} {pending.path} (request {pending.id}). The write may have committed.
        Reading current metadata does not establish its outcome. There is no public write-outcome lookup.
        {acknowledgedPending && ' You acknowledged this uncertainty; any further write is a new operation.'}
        <Box mt={1}>
          <Button disabled={loading} onClick={inspectPending}>Inspect current metadata</Button>
          {draft && !acknowledgedPending && <Button onClick={() => setEditorOpen(true)}>Review pending request</Button>}
          {!acknowledgedPending && <Button disabled={!inspectedPending || loading} onClick={() => {
            setAcknowledgedPending(true);
            setDraft(null);
            setEditorOpen(false);
          }}>Acknowledge uncertainty</Button>}
        </Box>
      </Alert></Box>}
      <Box mt={2}>
        <Paper variant="outlined">
          <Tabs value={section} onChange={changeSection} indicatorColor="primary" variant="scrollable">
            {SECTIONS.map((label) => <Tab key={label} label={label} />)}
          </Tabs>
          <Box p={2}>
            {selected && <Box mb={2}>
              <Button onClick={() => { setResult(null); setSelected(null); }}>Back to {SECTIONS[section].toLowerCase()}</Button>
              <Typography variant="h6">{selected}</Typography>
            </Box>}
            {loading && <CircularProgress size={24} aria-label="Loading committed metadata" />}
            {error && <Alert severity="error">{error}</Alert>}
            {!loading && !error && renderActions()}
            {!loading && !error && section === 0 && selected && capabilities?.tableSize &&
              <NativeTableSize key={selected} tableName={selected} refresh={refresh} />}
            {!loading && !error && renderResult()}
            {result && <Typography variant="caption" component="p" className={classes.revision}>
              Committed revision: {result.revision}
            </Typography>}
          </Box>
        </Paper>
      </Box>
      {draft && <NativeMetadataEditor draft={draft} open={editorOpen} onClose={() => {
        setDraft(null);
        setEditorOpen(false);
      }} onInspect={inspectPending} onPending={(marker) => {
        setPending(marker);
        setInspectedPending(false);
        setAcknowledgedPending(false);
      }} onCommitted={() => {
        // React 16 does not batch these asynchronous updates. Clear the detail image before selecting its list.
        setResult(null);
        if (draft.method === 'DELETE' && section === 1) {
          setSelected(null);
        }
        setDraft(null);
        setEditorOpen(false);
        setWriteStatus('Metadata change committed.');
        setRefresh((value) => value + 1);
      }} />}
    </Grid>
  );
};

export default NativeMetadataPage;
