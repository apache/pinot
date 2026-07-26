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
package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;


/**
 * {@code RuntimeFilterRel} represents an additive probe-side runtime filter for an INNER equi-join.
 *
 * <p>It is created by {@code PinotJoinToInnerRuntimeFilterRule} and sits at the top of the probe
 * (left) leaf subtree, below the join's left exchange. It is a pass-through for the probe rows (its
 * row type equals the probe's), so the inner join keeps running unchanged in its intermediate stage.
 *
 * <p>The two inputs mirror the SEMI dynamic-broadcast layout (where the pipeline-breaker receive is the
 * second input of the consuming node):
 * <ul>
 *   <li>{@code left} (input 0) is the probe pipeline.</li>
 *   <li>{@code right} (input 1) is a {@code PIPELINE_BREAKER} exchange carrying the build-side join
 *       keys. After fragmentation it becomes a separate stage feeding a pipeline-breaker receive.</li>
 * </ul>
 *
 * <p>{@code probeKeys} index this node's (probe) row type; {@code buildKeys} index the build-key
 * exchange's row type. They are positionally aligned. {@code filterType} is the resolved reducer
 * strategy. The reducer never introduces false negatives, so it can be omitted at any time without
 * affecting correctness — the real hash join remains the source of truth.
 */
public class RuntimeFilterRel extends BiRel {
  private final List<Integer> _probeKeys;
  private final List<Integer> _buildKeys;
  private final RuntimeFilterType _filterType;

  public RuntimeFilterRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode probe, RelNode buildKeyExchange,
      List<Integer> probeKeys, List<Integer> buildKeys, RuntimeFilterType filterType) {
    super(cluster, traitSet, probe, buildKeyExchange);
    _probeKeys = List.copyOf(probeKeys);
    _buildKeys = List.copyOf(buildKeys);
    _filterType = filterType;
  }

  public static RuntimeFilterRel create(RelNode probe, RelNode buildKeyExchange, List<Integer> probeKeys,
      List<Integer> buildKeys, RuntimeFilterType filterType) {
    RelOptCluster cluster = probe.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new RuntimeFilterRel(cluster, traitSet, probe, buildKeyExchange, probeKeys, buildKeys, filterType);
  }

  public List<Integer> getProbeKeys() {
    return _probeKeys;
  }

  public List<Integer> getBuildKeys() {
    return _buildKeys;
  }

  public RuntimeFilterType getFilterType() {
    return _filterType;
  }

  @Override
  protected RelDataType deriveRowType() {
    // Pass-through: the probe rows flow through unchanged.
    return left.getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return new RuntimeFilterRel(getCluster(), traitSet, inputs.get(0), inputs.get(1), _probeKeys, _buildKeys,
        _filterType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("probeKeys", _probeKeys)
        .item("buildKeys", _buildKeys)
        .item("filterType", _filterType);
  }

  /**
   * The tiered reducer strategy for the probe-side runtime filter.
   */
  public enum RuntimeFilterType {
    IN, BLOOM, AUTO
  }
}
