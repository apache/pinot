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

/// **Query rewrite — aggregation equivalence rules.**
///
/// An `AggregationEquivalence` rule says "the user's `foo(x)` can be answered from the MV's
/// `bar(x)`" and, on match, rewrites the user aggregation against the MV column.  The
/// `AggregationSubsumptionStrategy` in
/// [strategy][org.apache.pinot.materializedview.rewrite.strategy] consults the
/// `AggregationEquivalenceRegistry` to resolve each user aggregation against an MV-projected
/// aggregation.
///
/// Built-in rules:
///   - `PassthroughEquivalence` — identity-rewrite for aggregates that are split-safe and
///     don't need merging (e.g. `MIN`, `MAX`, `SUM`).  Maps `f(x) → f(mv_col)` directly.
///   - `SketchMergeEquivalence` — covers sketch-based aggregations where the MV stores the
///     raw sketch and the user query merges via the corresponding `*_merge` UDF (e.g.
///     `DISTINCT_COUNT_HLL → DISTINCT_COUNT_HLL_MERGE`).
package org.apache.pinot.materializedview.rewrite.equivalence;
