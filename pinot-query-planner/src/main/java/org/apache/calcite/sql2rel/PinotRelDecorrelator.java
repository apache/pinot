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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pinot extension of {@link RelDecorrelator} that relaxes the post-decorrelation type assertion.
 *
 * <p>Calcite 1.41.0 added a strict type equality check after decorrelation (via {@code Litmus.THROW}) that fails
 * when decorrelation changes the nullability of output fields. This is a known Calcite bug (CALCITE-7379) where
 * correlated variables in LEFT JOIN decorrelation produce output types with different nullability than the original
 * correlated plan. The fix is in Calcite main but not included in 1.41.0.
 *
 * <p>This class provides a {@link #decorrelateQuery(RelNode, RelBuilder)} method that performs decorrelation without
 * the strict type assertion, logging a warning instead when the types differ.
 */
public class PinotRelDecorrelator extends RelDecorrelator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PinotRelDecorrelator.class);

  private PinotRelDecorrelator(CorelMap cm,
      org.apache.calcite.plan.Context context, RelBuilder relBuilder) {
    super(cm, context, relBuilder);
  }

  /**
   * Decorrelates a query without the strict type assertion that fails on
   * CALCITE-7379.
   *
   * <p>This method replicates the logic of
   * {@link RelDecorrelator#decorrelateQuery(RelNode, RelBuilder)} but replaces
   * the {@code Litmus.THROW} assertion with a warning log when the output type
   * changes during decorrelation.
   */
  public static RelNode decorrelateQuery(RelNode rootRel,
      RelBuilder relBuilder) {
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final PinotRelDecorrelator decorrelator =
        new PinotRelDecorrelator(
            corelMap, cluster.getPlanner().getContext(), relBuilder);

    RelNode newRootRel =
        decorrelator.removeCorrelationViaRule(rootRel);

    if (decorrelator.cm.hasCorrelation()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    if (!rootRel.getRowType()
        .equalsSansFieldNames(newRootRel.getRowType())) {
      LOGGER.warn(
          "Decorrelation changed the row type from {} to {}"
              + " (CALCITE-7379)",
          rootRel.getRowType(), newRootRel.getRowType());
    }

    newRootRel = RelOptUtil.propagateRelHints(newRootRel, true);
    return newRootRel;
  }
}
