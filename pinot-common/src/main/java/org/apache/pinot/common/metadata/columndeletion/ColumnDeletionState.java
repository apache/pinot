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
package org.apache.pinot.common.metadata.columndeletion;


/// Lifecycle of one explicit column deletion on a table.
///
/// {@link #PREPARED} exists so the ledger can be written before the schema znode. If the controller
/// dies in that window, the next scan either aborts the entry (column still present) or advances it
/// to {@link #RECLAIMING} (schema write already landed). {@link #PENDING} means the schema write
/// landed and reclamation has not started. {@link #FAILED} means a reclaim attempt failed; the
/// entry still blocks a same-name re-add until an operator or later reconciler moves it to
/// {@link #COMPLETE}. {@link #COMPLETE} is a bounded tombstone and must not block a legal
/// same-name re-add.
public enum ColumnDeletionState {
  PREPARED,
  PENDING,
  RECLAIMING,
  COMPLETE,
  FAILED;

  /// True when a same-name add must be rejected.
  public boolean blocksReAdd() {
    return this != COMPLETE;
  }
}
