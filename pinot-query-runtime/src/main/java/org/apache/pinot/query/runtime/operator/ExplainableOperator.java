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
package org.apache.pinot.query.runtime.operator;

import org.apache.pinot.query.planner.plannode.ExplainedNode;


/// A leaf-stage operator that can describe itself as an [ExplainedNode] for `EXPLAIN`.
///
/// `QueryRunner#explainQuery` compiles the leaf stage with `explain=true` and, for each operator built,
/// records the [ExplainedNode] returned here so it can be spliced back into the broker plan tree. The default
/// row leaf ([LeafOperator]) implements this, and alternative leaf-stage operators can implement it too so
/// their execution shows up in `EXPLAIN` instead of falling back to the pre-execution Calcite plan.
///
/// Implementations must also be [MultiStageOperator]s: the collector receives each operator as a
/// [MultiStageOperator], so an implementation outside that hierarchy is silently never consulted.
public interface ExplainableOperator {

  /// Produces the explain representation of this operator's stage subtree. May run the single-stage engine in
  /// explain mode to obtain segment-level plans; it does not execute the multi-stage query.
  ///
  /// The returned node must not be null, and its title must contain the substring `Combine` — the value of
  /// [org.apache.pinot.query.planner.explain.ExplainNodeSimplifier#COMBINE]. That substring is what switches
  /// `PlanNodeMerger` and `PlanNodeSorter` from positional child matching to per-segment grouping and
  /// de-duplication. Positional matching bails out unless every server reports the same number of children,
  /// which per-segment plans do not, so a title without `Combine` silently produces `EXPLAIN` output repeated
  /// per server and per segment instead of one merged subtree.
  ///
  /// For the same reason the title must be identical across servers — `PlanNodeMerger` compares titles with
  /// `equals`, so per-server detail in the title defeats merging — and attributes carrying per-server values
  /// should be declared `IDEMPOTENT` where equal everywhere, since a differing non-idempotent `long`
  /// attribute also leaves the nodes unmerged.
  ///
  /// Called at most once per instance, on the thread that compiles the leaf stage and before the op chain is
  /// scheduled. Implementations therefore need not be thread-safe here, and need not support explaining and
  /// executing the same instance. It may block until the passive deadline
  /// (`OpChainExecutionContext#getPassiveDeadlineMs()`) and may throw. An implementation must release
  /// whatever it started before throwing: the caller does not close an operator whose `explain()` failed.
  ExplainedNode explain();
}
