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

import React, { useEffect, useRef, useState } from 'react';
import { Box, Button, MenuItem, TextField, Typography } from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import CustomDialog from './CustomDialog';
import {
  NativeWriteMarker, NativeWriteRequest, newNativeWriteId, rememberNativeWrite, validateNativeJson, writeNativeMetadata
} from '../requests/nativeMetadata';

export type NativeEditDraft = {
  title: string;
  method: 'POST' | 'PUT' | 'DELETE';
  path: string;
  json?: string;
  etag?: string;
  create: boolean;
  clusterKeys?: string[];
};

type Props = {
  draft: NativeEditDraft;
  open: boolean;
  onClose: () => void;
  onInspect: () => void;
  onCommitted: () => void;
  onPending: (marker: NativeWriteMarker | null) => void;
};

// An uncertain request remains immutable. Retrying sends the same bytes, version precondition and create key.
const NativeMetadataEditor = ({ draft, open, onClose, onInspect, onCommitted, onPending }: Props) => {
  const [json, setJson] = useState(draft.json || '');
  const [key, setKey] = useState(draft.clusterKeys?.[0] || '');
  const [pending, setPending] = useState<NativeWriteRequest | null>(null);
  const [sending, setSending] = useState(false);
  const [unknown, setUnknown] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const sendingRef = useRef(false);

  useEffect(() => {
    if (!pending) {
      return;
    }
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [pending]);

  const submit = async () => {
    if (sendingRef.current) {
      return;
    }
    let request = pending;
    try {
      if (!request) {
        if (draft.method !== 'DELETE') {
          validateNativeJson(json);
          if (new TextEncoder().encode(json).length > 2 * 1024 * 1024) {
            throw new Error('Configuration exceeds the 2 MiB editor limit.');
          }
        }
        if (draft.clusterKeys && !key) {
          throw new Error('Select a configuration key.');
        }
        if (!draft.create && !draft.etag) {
          throw new Error('Load the current metadata version before editing.');
        }
        request = Object.freeze({ id: newNativeWriteId(), method: draft.method,
          path: draft.clusterKeys ? `${draft.path}/${encodeURIComponent(key)}` : draft.path,
          body: draft.method === 'DELETE' ? undefined : json, etag: draft.etag, create: draft.create });
        // Persist identity before sending. No payload or credentials are stored in the browser.
        rememberNativeWrite(request);
        setPending(request);
        onPending(request);
      }
    } catch (failure) {
      setMessage(failure instanceof Error ? failure.message : 'Unable to prepare the request.');
      return;
    }
    sendingRef.current = true;
    setSending(true);
    setMessage(null);
    const outcome = await writeNativeMetadata(request);
    sendingRef.current = false;
    setSending(false);
    if (outcome.committed) {
      rememberNativeWrite(null);
      onPending(null);
      onCommitted();
    } else if (unknown || outcome.unknown) {
      // A conflict on retry does not prove whether the original attempt committed.
      setUnknown(true);
      setMessage(`${outcome.message} The original write outcome remains unknown.`);
    } else {
      rememberNativeWrite(null);
      setPending(null);
      onPending(null);
      setMessage(`Request rejected: ${outcome.message}`);
    }
  };

  return (
    <CustomDialog open={open} title={draft.title} size="md" handleClose={onClose} handleSave={submit}
      btnOkText={sending ? 'Sending…' : pending ? 'Retry same request' : draft.method === 'DELETE' ? 'Delete' : 'Save'}
      okBtnDisabled={sending} showCancelBtn={!pending} disableBackdropClick disableEscapeKeyDown
      moreActions={unknown && !sending ? <Button onClick={onInspect}>Inspect current metadata</Button> : undefined}>
      {unknown && <Box mb={2}><Alert severity="warning">
        This write may have committed. Retrying preserves the original request. Reading current metadata does not
        establish its outcome; there is no public write-outcome lookup.
      </Alert></Box>}
      {message && <Box mb={2}><Alert severity={unknown ? 'warning' : 'error'}>{message}</Alert></Box>}
      {draft.clusterKeys && <TextField select fullWidth label="Configuration key" value={key}
        disabled={!!pending} onChange={(event) => setKey(event.target.value)}>
        {draft.clusterKeys.map((name) => <MenuItem key={name} value={name}>{name}</MenuItem>)}
      </TextField>}
      {draft.method === 'DELETE' ? <Typography>Delete this metadata only if its loaded version is still current.</Typography>
        : <TextField label="JSON configuration" fullWidth multiline rows={16} variant="outlined" value={json}
          disabled={!!pending} onChange={(event) => setJson(event.target.value)}
          inputProps={{ style: { fontFamily: 'monospace' } }} />}
      {draft.path === '/tables' && draft.create && <Box mt={1}><Typography variant="body2">
        Create the schema first, using the table name without the _OFFLINE suffix.
      </Typography></Box>}
      {draft.path === '/cluster/configs' && draft.method === 'POST' && <Box mt={1}><Typography variant="body2">
        Keys omitted from this JSON remain unchanged. Use Delete key to remove a key.
      </Typography></Box>}
      {draft.etag && <Box mt={1}><Typography variant="caption">Loaded version: {draft.etag}</Typography></Box>}
      {pending && <Box mt={1}><Typography variant="caption">Request ID: {pending.id}</Typography></Box>}
    </CustomDialog>
  );
};

export default NativeMetadataEditor;
