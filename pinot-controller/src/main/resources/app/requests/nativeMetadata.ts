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

import JSONbigBase from 'json-bigint';
import { baseApi } from '../utils/axios-config';

const JSONbig = JSONbigBase({ strict: true });
const PENDING_WRITE_KEY = 'pinot_ui:nativePendingWrite';

export type NativeMetadataResponse<T> = { value: T; revision: string; json: string; etag?: string };
export type NativeWriteCapabilities = {
  schemaCreate: boolean; schemaUpdate: boolean; schemaDelete: boolean;
  tableCreate: boolean; tableUpdate: boolean; tableDelete: boolean;
  clusterConfigUpdate: boolean; clusterConfigDelete: boolean;
  tableSize?: boolean; columnCompressionStats?: boolean;
};
export type NativeWriteMarker = { id: string; method: string; path: string };
export type NativeWriteRequest = NativeWriteMarker & { method: 'POST' | 'PUT' | 'DELETE'; body?: string; etag?: string; create: boolean };
export type NativeWriteOutcome = { committed: boolean; unknown: boolean; message: string };

// Keep only the request identity across navigation. Config JSON and credentials must never be saved here.
export const rememberNativeWrite = (request: NativeWriteMarker | null) => {
  if (request) {
    sessionStorage.setItem(PENDING_WRITE_KEY, JSON.stringify({ id: request.id, method: request.method, path: request.path }));
  } else {
    try { sessionStorage.removeItem(PENDING_WRITE_KEY); } catch (_) { /* A stale warning is safe. */ }
  }
};

export const pendingNativeWrite = (): NativeWriteMarker | null => {
  try {
    const stored = sessionStorage.getItem(PENDING_WRITE_KEY);
    if (!stored) {
      return null;
    }
    const marker = JSON.parse(stored);
    if (typeof marker.id === 'string' && typeof marker.method === 'string' && typeof marker.path === 'string') {
      return marker;
    }
  } catch (_) { /* An unreadable marker still represents an unresolved write. */ }
  return { id: 'unavailable', method: 'Unknown request', path: '' };
};

export const newNativeWriteId = (): string => {
  const bytes = window.crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};

// Unwrap the existing table response without rounding long/default numeric values through JavaScript Number.
export const nativeEditorJson = (response: NativeMetadataResponse<any>, table = false): string => {
  const value = JSONbig.parse(response.json);
  return JSONbig.stringify(table ? value.OFFLINE : value, null, 2);
};

export const validateNativeJson = (json: string) => {
  const value = JSONbig.parse(json);
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Enter a JSON object.');
  }
};

export const readNativeWriteCapabilities = async (): Promise<NativeWriteCapabilities> => {
  const response = await baseApi.get('/metadata/capabilities', { timeout: 10000 });
  if (response.status !== 200 || !response.data || typeof response.data !== 'object') {
    throw new Error('Metadata editing is unavailable because supported operations could not be loaded.');
  }
  return response.data;
};

export const writeNativeMetadata = async (request: NativeWriteRequest): Promise<NativeWriteOutcome> => {
  try {
    const response = await baseApi.request({
      method: request.method, url: request.path, data: request.body, timeout: 10000,
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': request.id,
        ...(request.create ? { 'If-None-Match': '*' } : { 'If-Match': request.etag }) },
    });
    const revision = response.headers?.['x-pinot-metadata-revision'];
    if (response.status === 200 && response.data?.status === 'OK' && typeof response.data?.message === 'string'
        && response.data?.error === undefined && response.data?.outcomeUnknown !== true && typeof revision === 'string'
        && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+$/i.test(revision)) {
      return { committed: true, unknown: false, message: 'Metadata change committed.' };
    }
    const knownRejection = [400, 401, 403, 404, 409, 412, 413, 428, 501].includes(response.status)
      && response.data?.outcomeUnknown === false && typeof response.data?.error === 'string';
    return { committed: false, unknown: !knownRejection,
      message: typeof response.data?.error === 'string' ? response.data.error : 'No confirmed write result was received.' };
  } catch (_) {
    return { committed: false, unknown: true, message: 'No confirmed write result was received.' };
  }
};

export type NativeParticipants = {
  revision: { historyId: string; index: string };
  control: { round: string } | null;
  participants: Array<{
    participant: { participantId: string; role: string; incarnation: string; registrationEpoch: string };
    expiryRound: string;
    ready: boolean;
    draining: boolean;
    retired: boolean;
    live: boolean;
  }>;
};

// The shared axios client attaches the normal BASIC/OIDC credentials but resolves HTTP errors. Check status
// explicitly so a denied or unavailable read can never be rendered as an empty successful metadata image.
export const readNativeMetadata = async <T,>(path: string): Promise<NativeMetadataResponse<T>> => {
  const response = await baseApi.get<string>(path, { timeout: 10000, transformResponse: [(data) => data] });
  if (response.status !== 200 || response.data == null) {
    throw new Error(response.status === 401 || response.status === 403
      ? 'You are not authorized to browse this metadata.'
      : response.status === 404 ? 'This metadata no longer exists. Reload its list.'
      : 'Committed metadata is unavailable. Retry after the controller can reach a quorum.');
  }
  const revision = response.headers?.['x-pinot-metadata-revision'];
  if (typeof revision !== 'string' || !revision) {
    throw new Error('The controller did not provide a metadata revision.');
  }
  return { value: JSON.parse(response.data), json: response.data, revision, etag: response.headers?.etag };
};
