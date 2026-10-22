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
import { Box, CircularProgress, Typography } from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import JSONbigBase from 'json-bigint';
import { readNativeMetadata } from '../requests/nativeMetadata';

const JSONbig = JSONbigBase({ strict: true });

// Disk sizes are observations of installed replicas, distinct from metadata/configuration and query readiness.
const NativeTableSize = ({ tableName, refresh }: { tableName: string; refresh: number }) => {
  const [result, setResult] = useState<{ reported: string; estimated: string; missingSegments: string; revision: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    setResult(null);
    setError(null);
    readNativeMetadata(`/tables/${encodeURIComponent(tableName)}/size?verbose=false`).then((response) => {
      const sizes = JSONbig.parse(response.json);
      const format = (value: any) => {
        const text = value?.toString();
        if (!text || !/^(?:-1|[0-9]+)$/.test(text)) {
          throw new Error('The controller returned an invalid table-size observation.');
        }
        return text === '-1' ? 'Unavailable' : `${text} bytes`;
      };
      if (!cancelled) {
        setResult({ reported: format(sizes.reportedSizeInBytes), estimated: format(sizes.estimatedSizeInBytes),
          missingSegments: (sizes.offlineSegments?.missingSegments ?? 0).toString(), revision: response.revision });
      }
    }).catch((failure) => {
      if (!cancelled) {
        setError(failure instanceof Error ? failure.message : 'Table size is unavailable.');
      }
    });
    return () => { cancelled = true; };
  }, [tableName, refresh]);
  return <Box my={2}>
    <Typography variant="subtitle1">Table size (including replicas)</Typography>
    {!result && !error && <CircularProgress size={20} aria-label="Loading table size" />}
    {error && <Alert severity="warning">Table size unavailable. {error}</Alert>}
    {result && <>
      <Typography>Reported size: {result.reported}</Typography>
      <Typography>Estimated size: {result.estimated}</Typography>
      {result.missingSegments !== '0' && <Alert severity="warning">Segments without size reports: {result.missingSegments}. These totals are incomplete.</Alert>}
      <Typography variant="caption">Metadata revision: {result.revision}</Typography>
    </>}
  </Box>;
};

export default NativeTableSize;
