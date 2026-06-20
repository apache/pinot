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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Native multi-stage-engine expansion for {@code GROUP BY GROUPING SETS / ROLLUP / CUBE}. For each input row it emits
 * one output row per grouping set, so that an ordinary downstream aggregate keyed on the union group-by columns plus
 * the synthetic discriminator computes every set in a single pass:
 *
 * <pre>
 *   input
 *    └─ GroupingSetsExpandNode                    (this node — emits one row per input row per grouping set)
 *        └─ Aggregate over [unionGroupKeys..., $groupingId]   (ordinary LEAF/FINAL split + hash exchange)
 *            └─ Project restoring the original row type and computing GROUPING() / GROUPING_ID()
 * </pre>
 *
 * <p>The output schema is the input schema with one extra trailing INT column named
 * {@link GroupingSets#GROUPING_ID_COLUMN} ({@code $groupingId}). For grouping set {@code s} the operator copies the
 * input row, sets every union group-by column that is <em>rolled up</em> in {@code s} to {@code NULL}, and writes
 * {@code groupingIds.get(s)} into the {@code $groupingId} column. {@code groupingIds.get(s)} is the per-set bitmask
 * where bit {@code p} is set iff {@code groupingColumns.get(p)} is rolled up in {@code s} — the same
 * "1 = aggregated away" convention as {@link GroupingSets#groupingValue}, so genuine NULL groups and rolled-up NULL
 * groups stay distinct (their {@code $groupingId} differs) and GROUPING()/GROUPING_ID() can be derived downstream.
 *
 * <p><b>Mixed-version / rolling-upgrade note:</b> this is a plan-node type introduced after the leaf-pushdown design.
 * A multi-stage worker that predates it cannot deserialize the {@code groupingSetsExpandNode} proto oneof case and
 * fails the query outright (it never mis-reads the node as something else), so the feature requires all multi-stage
 * workers to be upgraded before brokers begin emitting it. There is no silent-wrong-result path.
 */
public class GroupingSetsExpandNode extends BasePlanNode {
  private final List<Integer> _groupingColumns;
  private final List<Integer> _groupingIds;

  public GroupingSetsExpandNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<Integer> groupingColumns, List<Integer> groupingIds) {
    super(stageId, dataSchema, nodeHint, inputs);
    _groupingColumns = List.copyOf(groupingColumns);
    _groupingIds = List.copyOf(groupingIds);
  }

  /// The row indexes (in the input schema, in union order) of the group-by columns that may be rolled up. Bit {@code p}
  /// of each entry in {@link #getGroupingIds()} refers to {@code groupingColumns.get(p)}.
  public List<Integer> getGroupingColumns() {
    return _groupingColumns;
  }

  /// One bitmask per grouping set: bit {@code p} set iff {@code groupingColumns.get(p)} is rolled up in that set. The
  /// value is also emitted into the trailing {@code $groupingId} column. The list order defines the output row order
  /// produced per input row.
  public List<Integer> getGroupingIds() {
    return _groupingIds;
  }

  @Override
  public String explain() {
    return "GROUPING_SETS_EXPAND";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitGroupingSetsExpand(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new GroupingSetsExpandNode(_stageId, _dataSchema, _nodeHint, inputs, _groupingColumns, _groupingIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GroupingSetsExpandNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GroupingSetsExpandNode that = (GroupingSetsExpandNode) o;
    return Objects.equals(_groupingColumns, that._groupingColumns) && Objects.equals(_groupingIds, that._groupingIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _groupingColumns, _groupingIds);
  }
}
