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

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;


/**
 * PlanNode is a serializable version of the {@link RelNode}. See {@link PlanNodeSerializer} and
 * {@link PlanNodeDeserializer} for details.
 */
public interface PlanNode {

  int getStageId();

  // NOTE: Stage ID is not determined when the plan node is created, so we need a setter.
  void setStageId(int stageId);

  DataSchema getDataSchema();

  NodeHint getNodeHint();

  List<PlanNode> getInputs();

  /**
   * Explain this plan node as expected by EXPLAIN IMPLEMENTATION PLAN FOR.
   *
   * In this mode, each worker is represented as a sub-tree in the plan.
   */
  String explain();

  <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context);

  /**
   * Returns a new plan node that is equal to the receiver in all aspects but the inputs, which will be replaced by the
   * given inputs.
   */
  PlanNode withInputs(List<PlanNode> inputs);

  class NodeHint {
    public static final NodeHint EMPTY = new NodeHint(Map.of());

    private final Map<String, Map<String, String>> _hintOptions;

    public NodeHint(Map<String, Map<String, String>> hintOptions) {
      _hintOptions = hintOptions;
    }

    public static NodeHint fromRelHints(List<RelHint> relHints) {
      int numHints = relHints.size();
      Map<String, Map<String, String>> hintOptions;
      if (numHints == 0) {
        hintOptions = Map.of();
      } else if (numHints == 1) {
        RelHint relHint = relHints.get(0);
        hintOptions = Map.of(relHint.hintName, relHint.kvOptions);
      } else {
        hintOptions = Maps.newHashMapWithExpectedSize(numHints);
        for (RelHint relHint : relHints) {
          // Put the first matching hint to match the behavior of PinotHintStrategyTable
          hintOptions.putIfAbsent(relHint.hintName, relHint.kvOptions);
        }
      }
      return new NodeHint(hintOptions);
    }

    public Map<String, Map<String, String>> getHintOptions() {
      return _hintOptions;
    }

    /**
     * Creates a new instance that shares the hint options with the receiver, but with the given key-value pair added.
     */
    public NodeHint with(String key, Map<String, String> value) {
      Map<String, Map<String, String>> newHintOptions = Maps.newHashMap(_hintOptions);
      newHintOptions.put(key, value);
      return new NodeHint(newHintOptions);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NodeHint)) {
        return false;
      }
      NodeHint nodeHint = (NodeHint) o;
      return Objects.equals(_hintOptions, nodeHint._hintOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_hintOptions);
    }
  }
}
