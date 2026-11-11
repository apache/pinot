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

/// **Query rewrite — engine and shared utilities.**
///
/// The rewrite engine takes a user `PinotQuery` and decides whether it can be answered by a
/// materialized view.  Top-level pieces in this package:
///
///   - `MaterializedViewQueryRewriteEngine` — entry point.  Looks up MV candidates by base-
///     table name (via the metadata cache), applies subsumption strategies in cost order,
///     and picks the lowest-cost match.
///   - `MaterializedViewMetadataCache` — mirrors the ZK definition + runtime znodes into an
///     in-memory cache.  Subscribes to child-change and data-change listeners; releases
///     watcher slots in `close()`.
///   - `MaterializedViewRewritePlan` / `ExecutionMode` / `MatchType` — value objects
///     describing the rewrite decision.
///   - `MaterializedViewQueryShape` — coarse classifier (SCAN / AGGREGATION) used by
///     strategies to short-circuit shape mismatches.
///   - `MaterializedViewMatchUtils` — stateless helpers shared across strategies: filter
///     conjunct flattening, residual extraction, projection-key column resolution.
///
/// Sub-packages:
///   - [rewrite.strategy][org.apache.pinot.materializedview.rewrite.strategy] — concrete
///     subsumption strategies (exact, scan, aggregation).
///   - [rewrite.equivalence][org.apache.pinot.materializedview.rewrite.equivalence] —
///     aggregation equivalence rules (passthrough, sketch-merge).
package org.apache.pinot.materializedview.rewrite;
