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
package org.apache.pinot.spi.ingest;

import org.apache.pinot.spi.annotations.InterfaceStability;

/// Executes INSERT INTO statements and tracks their lifecycle.
///
/// Implementations are responsible for ingesting data (rows or files), managing the statement
/// state machine, and producing segments. Different backends (e.g., Ratis-based row ingestion,
/// minion-based file ingestion) provide their own implementations.
///
/// Implementations must be thread-safe; a single instance may be invoked concurrently from
/// multiple broker request threads.
///
/// **v1 limitation:** the controller's coordinator hardcodes {@code instanceof
/// FileInsertExecutor} checks to call FILE-specific lifecycle hooks (task-name caching, Minion
/// task polling, lineage finalization). A third-party plugin that implements this interface and
/// registers as {@link InsertType#FILE} would silently bypass those hooks — to prevent that, the
/// coordinator's `registerExecutor` fails fast with {@link UnsupportedOperationException} if
/// a non-`FileInsertExecutor` is registered for FILE. v1 only supports pluggable
/// {@link InsertType#ROW} executors. The FILE-specific lifecycle hooks will be lifted into this
/// SPI (as default no-op methods) in a future release.
@InterfaceStability.Evolving
public interface InsertExecutor {

  /// Executes the given insert request.
  ///
  /// @param request the insert request containing data and metadata
  /// @return the result reflecting the current state of the statement
  InsertResult execute(InsertRequest request);

  /// Best-effort cleanup hook called after a statement transitions to ABORTED. Implementations
  /// use this to release any in-memory caches or side effects (e.g., dropping a Minion task-name
  /// cache, reverting a lineage entry). The coordinator owns the durable state transition; this
  /// hook runs only after the CAS write to ABORTED has succeeded.
  ///
  /// **Implementations MUST be idempotent.** Although the coordinator's CAS
  /// guarantees only one logical winner per abort decision, multiple controller-side paths can
  /// still invoke this hook for the same statementId across cleanup-sweep retries, controller
  /// failover replays, or plugin-internal retries. An abort handler that is not idempotent risks
  /// corrupting downstream state (e.g., double-revert of segment lineage). Treat repeated calls
  /// for the same statementId as no-ops after the first successful invocation.
  ///
  /// @param statementId the unique identifier of the statement that was aborted
  void abort(String statementId);
}
