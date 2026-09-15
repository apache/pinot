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
package org.apache.pinot.calcite.sql2rel;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Pinot extension of [RelDecorrelator] that relaxes the post-decorrelation type assertion.
///
/// Calcite 1.41.0 added a strict type-equality check after decorrelation (via `Litmus.THROW`) that fails
/// when decorrelation changes the nullability of output fields. This is a known Calcite bug (CALCITE-7379) where
/// correlated variables in LEFT/INNER JOIN decorrelation produce output types whose nullability (or field names)
/// differ from the original correlated plan. Although CALCITE-7379 is listed as fixed in Calcite 1.42.0, the
/// assertion still fires for some Pinot query shapes (e.g. a scalar correlated sub-query feeding an aggregate),
/// where the "before" and "after" row types render identically yet are deemed unequal by
/// `equalsSansFieldNames` (a nullability difference the digest string does not surface).
///
/// This class extends [RelDecorrelator] and provides a [#decorrelateQuery(RelNode, RelBuilder)] method
/// that mirrors [RelDecorrelator#decorrelateQuery(RelNode, RelBuilder)] (Calcite 1.42.0) but replaces the
/// `Litmus.THROW` assertion with a warning log when the output type changes during decorrelation, allowing the
/// (correct) decorrelated plan to be used instead of aborting query compilation. All Calcite members it relies on
/// (`CorelMap`, `CorelMapBuilder`, the `cm` field, the protected constructor and
/// `decorrelate`/`removeCorrelationViaRule`) are public or protected on the superclass, so this class
/// lives in a Pinot-owned package rather than the framework's `org.apache.calcite.sql2rel` namespace.
///
/// Thread-safety: callers only ever touch the stateless static [#decorrelateQuery(RelNode, RelBuilder)]
/// entry point, which constructs a fresh decorrelator instance per invocation. All mutable decorrelation state is
/// per-instance (owned by the superclass), so concurrent calls from independent planning threads do not interfere.
public class PinotRelDecorrelator extends RelDecorrelator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRelDecorrelator.class);

  private PinotRelDecorrelator(CorelMap cm, Context context, RelBuilder relBuilder) {
    super(cm, context, relBuilder);
  }

  /// Decorrelates a query without the strict type assertion that fails on CALCITE-7379.
  ///
  /// This method mirrors the structure of Calcite 1.42.0's `RelDecorrelator.decorrelateQuery` but
  /// deliberately calls the no-argument `removeCorrelationViaRule(RelNode)` / `decorrelate(RelNode)`
  /// overloads (which apply the same default rule set as the public `RuleSet`-overload chain) and replaces the
  /// final `Litmus.THROW` type assertion with [#verifyRowTypePreserved] (fail-fast on a structural type
  /// change, warn-and-continue on a nullability-only change -- the CALCITE-7379 case). Re-verify against the upstream
  /// method when upgrading Calcite.
  public static RelNode decorrelateQuery(RelNode rootRel, RelBuilder relBuilder) {
    // SYNCED WITH Calcite 1.42.0 RelDecorrelator.decorrelateQuery -- re-diff this method body against upstream on
    // every calcite.version bump; the only intended deviation is the final verifyRowTypePreserved call (CALCITE-7379).
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final PinotRelDecorrelator decorrelator =
        new PinotRelDecorrelator(corelMap, cluster.getPlanner().getContext(), relBuilder);

    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (decorrelator.cm.hasCorrelation()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    verifyRowTypePreserved(rootRel.getRowType(), newRootRel.getRowType());

    newRootRel = RelOptUtil.propagateRelHints(newRootRel, true);
    return newRootRel;
  }

  /// Verifies that decorrelation did not change the row type in a way that affects the downstream plan, replacing the
  /// upstream `Litmus.THROW` assertion (see class Javadoc / CALCITE-7379). A nullability-only divergence is
  /// tolerated with a warning -- Pinot derives `ColumnDataType` purely from the `SqlTypeName` (see
  /// `RelToPlanNodeConverter#convertToColumnDataType`), so it cannot alter the plan -- while a genuine structural
  /// type change (different base type / precision / scale) still fails fast rather than silently producing a
  /// wrong-typed plan.
  ///
  /// Package-private and extracted from [#decorrelateQuery] so the structural-vs-nullability decision (the
  /// whole reason this class does not blanket-suppress the assertion) is unit-testable without a triggering query.
  static void verifyRowTypePreserved(RelDataType before, RelDataType after) {
    if (before.equalsSansFieldNames(after)) {
      return;
    }
    if (!before.equalsSansFieldNamesAndNullability(after)) {
      throw new AssertionError("Decorrelation produced a relation with a different (structural) type; before: "
          + before + " after: " + after);
    }
    LOGGER.warn("Decorrelation changed only the nullability of the row type from {} to {} (CALCITE-7379)",
        before, after);
  }
}
