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

/**
 * Lifecycle states for an INSERT INTO statement.
 *
 * <p>The state transitions follow this order:
 * <pre>
 *   NEW -> ACCEPTED -> PREPARED -> COMMITTED -> VISIBLE
 *                \         \          \
 *                 +-----> ABORTED <---+
 *                                      \
 *                                       -> GC
 * </pre>
 *
 * <ul>
 *   <li>{@link #NEW} — statement has been created but not yet acknowledged by the coordinator.</li>
 *   <li>{@link #ACCEPTED} — coordinator has accepted the statement for processing.</li>
 *   <li>{@link #PREPARED} — data has been staged and is ready to commit.</li>
 *   <li>{@link #COMMITTED} — commit decision has been made; segments are being published.</li>
 *   <li>{@link #VISIBLE} — segments are live and queryable.</li>
 *   <li>{@link #ABORTED} — statement was cancelled or failed; resources may still need cleanup.</li>
 *   <li>{@link #GC} — all resources for the statement have been garbage-collected.</li>
 * </ul>
 *
 * <p>This enum is thread-safe (immutable).
 */
public enum InsertStatementState {
  NEW,
  ACCEPTED,
  PREPARED,
  COMMITTED,
  VISIBLE,
  ABORTED,
  GC
}
