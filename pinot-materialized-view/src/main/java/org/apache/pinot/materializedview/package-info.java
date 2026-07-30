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

/// Apache Pinot materialized view (MV) subsystem.  An MV is an OFFLINE Pinot table whose rows
/// are produced by a saved SQL query against one or more base Pinot tables.  The MV is
/// refreshed periodically by a minion task and the broker transparently rewrites user queries
/// to read from the MV when its definition subsumes the query.
///
/// The subsystem is divided into three concerns, each living in its own sub-package family.
/// New contributors should start with the role they're modifying:
///
/// **1. Definition** — how an MV is *declared* and stored.
///   - [analysis][org.apache.pinot.materializedview.analysis] — parses and validates the MV's
///     `definedSQL` at create time (legal aggregations, group-by/dimension columns,
///     supported time expressions).
///   - [metadata][org.apache.pinot.materializedview.metadata] — ZK znode schemas for the
///     MV definition record and per-partition runtime state.
///   - [consistency][org.apache.pinot.materializedview.consistency] — controller-side
///     reconciler that keeps base-table / MV-table state aligned across drops, renames, and
///     ingestion-config drift.
///
/// **2. View generation** — how the MV's *data* is produced.
///   - [scheduler][org.apache.pinot.materializedview.scheduler] — controller-side minion
///     task scheduler.  Decides which time windows of the MV need to be (re)materialized
///     and generates `PinotTaskConfig`s.
///   - [executor][org.apache.pinot.materializedview.executor] — minion-side task executor
///     that runs the MV's SQL, builds the resulting segments, and uploads them.
///
/// **3. Query rewrite** — how the broker *uses* an existing MV at query time.
///   - [context][org.apache.pinot.materializedview.context] — broker-side compile-time
///     carrier holding the rewrite plan and any per-mode (FULL_REWRITE / SPLIT_REWRITE)
///     state the broker request handler consumes.
///   - [handler][org.apache.pinot.materializedview.handler] — broker SPI
///     ([MaterializedViewHandler][org.apache.pinot.materializedview.handler.MaterializedViewHandler])
///     and default implementation.  The broker delegates compile-time match, FULL_REWRITE
///     swap, and SPLIT_REWRITE dual scatter-gather through this interface so the bulk of
///     MV-aware logic lives outside `pinot-broker`.
///   - [rewrite][org.apache.pinot.materializedview.rewrite] — the rewrite engine itself: a
///     metadata cache that mirrors definition/runtime znodes, plus subsumption strategies
///     (see [rewrite.strategy][org.apache.pinot.materializedview.rewrite.strategy]) and
///     aggregation equivalence rules
///     (see [rewrite.equivalence][org.apache.pinot.materializedview.rewrite.equivalence]).
///
/// Cross-package data flow at query time:
///
///   user query
///     -> broker `BaseSingleStageBrokerRequestHandler`
///     -> `handler.MaterializedViewHandler#compile`
///     -> `rewrite.MaterializedViewQueryRewriteEngine`
///     -> `rewrite.strategy.*` match
///     -> `context.MaterializedViewContext`
///     -> broker dispatches the rewritten query (FULL_REWRITE) or merges base + MV
///        scatter-gather (SPLIT_REWRITE)
package org.apache.pinot.materializedview;
