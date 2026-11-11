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
package org.apache.pinot.materializedview.executor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;


/// Abstraction for executing SQL queries against Pinot and streaming typed result rows.
///
/// Implementations may use different transport protocols (e.g. gRPC, Arrow Flight)
/// and are responsible for broker discovery, connection management, load balancing,
/// and response deserialization.
///
/// **Streaming contract:** [#executeQuery] returns a [QueryHandle] after the schema frame has
/// been consumed but BEFORE the data rows are buffered. The caller pulls rows from
/// [QueryHandle#rows] on demand; the implementation backs the iterator with the underlying
/// transport stream and decodes one chunk at a time. Heap residency is therefore bounded by
/// the caller's chunk size, not by the total row count of the query — large MV windows that
/// would have OOM'd a buffer-the-whole-result API stream through fine.
///
/// **Lifecycle:** the caller MUST close the [QueryHandle] (try-with-resources). Close cancels
/// the stream if the iterator hasn't been fully drained, releasing the underlying transport
/// resources and any buffered frame.
///
/// Instances are expected to be long-lived and thread-safe so they can be shared across
/// multiple task executions. A single [QueryHandle], however, is single-threaded.
public interface MaterializedViewQueryExecutor extends Closeable {

  /// Issues the query and returns a streaming handle. Blocks until the metadata + schema frames
  /// have been received from the broker; data frames are pulled lazily as the caller iterates.
  ///
  /// @param sql         the SQL query to execute
  /// @param authHeaders authentication headers to include in the request
  /// @return a handle exposing the schema and a row iterator; caller MUST close
  /// @throws IOException if communication with the broker fails or the schema frame is missing
  QueryHandle executeQuery(String sql, Map<String, String> authHeaders)
      throws IOException;

  /// Streaming handle for a single executing query: exposes the schema plus a row iterator
  /// backed by the underlying transport.
  ///
  /// Contract:
  ///
  ///   - [#getDataSchema] is safe to call any time; the schema is available as soon as
  ///     [#executeQuery] returns and never changes for the lifetime of the handle.
  ///   - [#rows] returns the same iterator instance on every call; the underlying stream is
  ///     single-pass, so calling [#rows] more than once produces the same iterator and is
  ///     therefore equivalent to caching the first return value.
  ///   - The iterator is single-pass and single-threaded.
  ///     [Iterator#next] throws [java.util.NoSuchElementException] when the stream is
  ///     exhausted (caller must drive iteration with [Iterator#hasNext]).
  ///   - [#close] drains any remaining stream frames so the underlying transport (e.g. gRPC
  ///     server-streaming RPC) is properly terminated.  Implementations MAY also issue an
  ///     explicit cancel to free server resources sooner; the contract guarantees only that
  ///     the underlying call ends.  It is idempotent: a second invocation no-ops.  Using
  ///     [#rows] after close is undefined behavior.
  ///   - The handle is NOT safe to share across threads.  A single task executor owns the
  ///     handle for the duration of one query.
  ///   - Overrides [Closeable#close] to drop the `IOException` declaration: the gRPC stream
  ///     drain on close cannot fail in a way the caller can recover from, so any underlying
  ///     transport error is rethrown as an unchecked exception rather than forcing
  ///     try-with-resources callers to catch a checked exception.
  interface QueryHandle extends Closeable {
    DataSchema getDataSchema();

    Iterator<Object[]> rows();

    @Override
    void close();
  }
}
