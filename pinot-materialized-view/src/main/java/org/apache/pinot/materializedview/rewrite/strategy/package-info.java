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

/// **Query rewrite — subsumption strategies.**
///
/// Each strategy tests whether a user query can be answered by an MV under a specific
/// match relation.  The engine tries them in cost order; the first non-null plan wins:
///
///   - `ExactSubsumptionStrategy` — user query is structurally identical to the MV (same
///     SELECT / WHERE / GROUP BY / HAVING), only the table name differs.  Cost 0.0.
///   - `ScanSubsumptionStrategy` — user query projects a subset of the MV's columns with a
///     (possibly stricter) residual filter; no aggregation.  Cost 2.0 (or 3.0 with residual).
///   - `AggregationSubsumptionStrategy` — user query's GROUP BY is a subset of the MV's,
///     aggregates are compatible per
///     [equivalence][org.apache.pinot.materializedview.rewrite.equivalence] rules, residual
///     filter is allowed on dimensions that survive the MV's group-by.
///
/// All concrete strategies extend `AbstractSubsumptionStrategy` which provides the shared
/// match skeleton (projection map build, residual extraction, remap-and-rewrite) and
/// exposes per-step hooks for each subclass to override.
package org.apache.pinot.materializedview.rewrite.strategy;
