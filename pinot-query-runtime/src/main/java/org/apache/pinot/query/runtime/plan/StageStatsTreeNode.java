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
package org.apache.pinot.query.runtime.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor;


/**
 * Broker-side, in-memory view of the structured per-stage stats tree decoded from
 * {@link org.apache.pinot.common.proto.Worker.StageStatsNode}. Mirrors the proto shape but stores a deserialized
 * {@link StatMap} so that merging across multiple opchain reports (different workers of the same stage) is just a
 * recursive walk + {@link StatMap#merge(StatMap)} per node.
 *
 * <p>Used by the stream-mode stats reporting path. Mutable: {@link #merge(StageStatsTreeNode)} sums the other tree's
 * stat maps into this one in place.
 */
public class StageStatsTreeNode {

  /**
   * Thrown by {@link StageStatsTreeNode#merge(StageStatsTreeNode)} when the two trees disagree on shape (operator type
   * at any position differs, or children-count differs). The broker catches this, logs, and marks the stage as
   * {@code mergeFailed} in the per-stage coverage structure.
   */
  public static class ShapeMismatchException extends Exception {
    public ShapeMismatchException(String message) {
      super(message);
    }
  }

  private final OperatorTypeDescriptor _type;
  private final List<Integer> _planNodeIds;
  private final StatMap<?> _statMap;
  private final List<StageStatsTreeNode> _children;

  public StageStatsTreeNode(OperatorTypeDescriptor type, List<Integer> planNodeIds, StatMap<?> statMap,
      List<StageStatsTreeNode> children) {
    _type = type;
    _planNodeIds = List.copyOf(planNodeIds);
    _statMap = statMap;
    _children = List.copyOf(children);
  }

  public OperatorTypeDescriptor getType() {
    return _type;
  }

  public List<Integer> getPlanNodeIds() {
    return _planNodeIds;
  }

  public StatMap<?> getStatMap() {
    return _statMap;
  }

  public List<StageStatsTreeNode> getChildren() {
    return _children;
  }

  /**
   * Sums the other tree's stats into this one. Both trees must have identical shape (same operator type at every
   * position, same arity). On mismatch this method throws {@link ShapeMismatchException} and leaves this tree in an
   * unspecified partially-merged state — callers should drop it.
   */
  // We can prove the StatMaps share a key class because their operator type matches, but Java's type system can't
  // express that, so we suppress and rely on the runtime check inside StatMap.merge.
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void merge(StageStatsTreeNode other)
      throws ShapeMismatchException {
    if (_type.getId() != other._type.getId()) {
      throw new ShapeMismatchException("Operator type mismatch: " + _type.name() + " vs " + other._type.name());
    }
    if (_children.size() != other._children.size()) {
      throw new ShapeMismatchException("Children count mismatch for " + _type + ": " + _children.size() + " vs "
          + other._children.size());
    }
    ((StatMap) _statMap).merge(other._statMap);
    for (int i = 0; i < _children.size(); i++) {
      _children.get(i).merge(other._children.get(i));
    }
  }

  /**
   * Flattens this tree into a {@link MultiStageQueryStats.StageStats.Closed} via inorder traversal (leftmost-leaf
   * first), mirroring the order in which operators emit their stats today
   * ({@link MultiStageQueryStats.StageStats.Open#addLastOperator} call order). The result is compatible with the
   * legacy {@link QueryDispatcher.QueryResult} contract that exposes per-stage stats as a flat
   * {@code List<StageStats.Closed>}.
   *
   * <p>The flattening is lossy in the same way the legacy format is: tree shape is not preserved. Callers that need
   * tree shape should use this {@link StageStatsTreeNode} directly.
   */
  public MultiStageQueryStats.StageStats.Closed flattenInorder() {
    List<OperatorTypeDescriptor> types = new ArrayList<>();
    List<StatMap<?>> stats = new ArrayList<>();
    flattenInto(types, stats);
    return new MultiStageQueryStats.StageStats.Closed(types, stats);
  }

  private void flattenInto(List<OperatorTypeDescriptor> types, List<StatMap<?>> stats) {
    for (StageStatsTreeNode child : _children) {
      child.flattenInto(types, stats);
    }
    types.add(_type);
    stats.add(_statMap);
  }

  /**
   * Returns a deep copy of this subtree: every node's {@link StatMap} is cloned via the {@link StatMap} copy
   * constructor and children are copied recursively. The {@code type} and (immutable) {@code planNodeIds} are shared.
   *
   * <p>Used to hand a caller a snapshot of the accumulator that is isolated from subsequent in-place
   * {@link #merge(StageStatsTreeNode)} mutations of the live tree — otherwise a late merge on one thread would race
   * with a reader (e.g. {@code StatMap.asJson()}) on another, since {@link StatMap} is not safe for concurrent
   * read/merge.
   */
  // The cloned StatMap keeps the same (erased) key class as the source; Java's type system can't express that, so we
  // suppress and rely on the StatMap copy constructor copying the key class along with the values.
  @SuppressWarnings({"unchecked", "rawtypes"})
  public StageStatsTreeNode deepCopy() {
    List<StageStatsTreeNode> childrenCopy = new ArrayList<>(_children.size());
    for (StageStatsTreeNode child : _children) {
      childrenCopy.add(child.deepCopy());
    }
    return new StageStatsTreeNode(_type, _planNodeIds, new StatMap((StatMap) _statMap), childrenCopy);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StageStatsTreeNode)) {
      return false;
    }
    StageStatsTreeNode that = (StageStatsTreeNode) o;
    return _type.getId() == that._type.getId()
        && Objects.equals(_planNodeIds, that._planNodeIds)
        && Objects.equals(_statMap, that._statMap)
        && Objects.equals(_children, that._children);
  }

  @Override
  public int hashCode() {
    // Hash the type by id, not by instance, to stay consistent with equals() (which compares _type.getId()).
    return Objects.hash(_type.getId(), _planNodeIds, _statMap, _children);
  }
}
