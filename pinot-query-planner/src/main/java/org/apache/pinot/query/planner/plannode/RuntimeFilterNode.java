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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;


/**
 * {@code RuntimeFilterNode} carries a probe-side runtime filter for an INNER equi-join.
 *
 * <p>It is the additive counterpart of the SEMI-join dynamic broadcast: instead of replacing the join
 * with a leaf filter, the inner join keeps running in its intermediate stage while this node, placed at
 * the top of the probe (left) leaf stage, builds a reducer from the build (right) side's join keys and
 * pushes it onto the probe leaf scan. Rows that cannot match are dropped before they are
 * shuffled into the join.
 *
 * <p>Structure (mirrors the SEMI dynamic-broadcast layout, where the pipeline-breaker receive is the
 * second input of the consuming node):
 * <ul>
 *   <li>{@code input[0]} is the probe pipeline (table scan, optionally projected/filtered). The node is
 *       a pass-through for these rows, so its {@link #getDataSchema() data schema} equals the probe
 *       pipeline's output schema.</li>
 *   <li>{@code input[1]} is a {@link MailboxReceiveNode} reading a {@code PIPELINE_BREAKER} exchange that
 *       carries the build-side join keys.</li>
 * </ul>
 *
 * <p>{@code probeKeys} index this node's (probe) row type; {@code buildKeys} index {@code input[1]}'s
 * (build-key) row type. {@code type} is the resolved reducer strategy.
 *
 * <p>The reducer never introduces false negatives, so the node can always be dropped (empty build,
 * over-threshold, unsupported leaf, mixed-version) without affecting correctness — the real hash join
 * remains the source of truth.
 *
 * <p>This class is immutable.
 */
public class RuntimeFilterNode extends BasePlanNode {
  private final List<Integer> _probeKeys;
  private final List<Integer> _buildKeys;
  private final Type _type;

  public RuntimeFilterNode(int stageId, DataSchema dataSchema, @Nullable NodeHint nodeHint, List<PlanNode> inputs,
      List<Integer> probeKeys, List<Integer> buildKeys, Type type) {
    super(stageId, dataSchema, nodeHint, inputs);
    _probeKeys = List.copyOf(probeKeys);
    _buildKeys = List.copyOf(buildKeys);
    _type = type;
  }

  /**
   * Indexes into this node's (probe) row type identifying the columns the reducer filters on.
   */
  public List<Integer> getProbeKeys() {
    return _probeKeys;
  }

  /**
   * Indexes into {@code input[1]}'s (build-key) row type identifying the columns the reducer is built
   * from. Aligned positionally with {@link #getProbeKeys()}.
   */
  public List<Integer> getBuildKeys() {
    return _buildKeys;
  }

  /**
   * The resolved reducer strategy.
   */
  public Type getType() {
    return _type;
  }

  @Override
  public String explain() {
    return "RUNTIME_FILTER(type=" + _type + ", probeKeys=" + _probeKeys + ", buildKeys=" + _buildKeys + ")";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitRuntimeFilter(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new RuntimeFilterNode(_stageId, _dataSchema, _nodeHint, inputs, _probeKeys, _buildKeys, _type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RuntimeFilterNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RuntimeFilterNode that = (RuntimeFilterNode) o;
    return _type == that._type && Objects.equals(_probeKeys, that._probeKeys) && Objects.equals(_buildKeys,
        that._buildKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _probeKeys, _buildKeys, _type);
  }

  /**
   * The tiered reducer strategy for the probe-side runtime filter.
   * <ul>
   *   <li>{@link #IN} — always emit an exact {@code IN} predicate.</li>
   *   <li>{@link #BLOOM} — always emit a bloom predicate.</li>
   *   <li>{@link #AUTO} — emit an exact {@code IN} at/below a build-key-row threshold, else a bloom.</li>
   * </ul>
   */
  public enum Type {
    IN, BLOOM, AUTO
  }
}
