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

import com.google.common.base.Preconditions;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;


public class MailboxSendNode extends BasePlanNode {
  private final BitSet _receiverStages;
  private final PinotRelExchangeType _exchangeType;
  private RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final boolean _prePartitioned;
  private final List<RelFieldCollation> _collations;
  private final boolean _sort;
  private final String _hashFunction;
  /// The explicit specs, by receiver stage id. A receiver without an entry uses [#getDefaultReceiverSpec(int)].
  private final TreeMap<Integer, ReceiverSpec> _receiverSpecs;

  // NOTE: null List is converted to empty List because there is no way to differentiate them in proto during ser/de.
  private MailboxSendNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs,
      BitSet receiverStages, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys, boolean prePartitioned,
      @Nullable List<RelFieldCollation> collations, boolean sort, String hashFunction,
      Map<Integer, ReceiverSpec> receiverSpecs) {
    super(stageId, dataSchema, null, inputs);
    _receiverStages = receiverStages;
    _exchangeType = exchangeType;
    _distributionType = distributionType;
    _keys = keys != null ? keys : List.of();
    _prePartitioned = prePartitioned;
    _collations = collations != null ? collations : List.of();
    _sort = sort;
    _hashFunction = hashFunction;
    _receiverSpecs = new TreeMap<>();
    for (ReceiverSpec spec : receiverSpecs.values()) {
      setReceiverSpec(spec);
    }
  }

  public MailboxSendNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs,
      @Nullable List<Integer> receiverStages, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys, boolean prePartitioned,
      @Nullable List<RelFieldCollation> collations, boolean sort, String hashFunction) {
    this(stageId, dataSchema, inputs, toBitSet(receiverStages), exchangeType,
        distributionType, keys, prePartitioned, collations, sort, hashFunction, Map.of());
  }

  public MailboxSendNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs,
      int receiverStage, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys, boolean prePartitioned,
      @Nullable List<RelFieldCollation> collations, boolean sort, String hashFunction) {
    this(stageId, dataSchema, inputs, toBitSet(receiverStage), exchangeType, distributionType, keys, prePartitioned,
        collations, sort, hashFunction, Map.of());
  }

  public MailboxSendNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs,
      @Nullable List<Integer> receiverStages, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys, boolean prePartitioned,
      @Nullable List<RelFieldCollation> collations, boolean sort, String hashFunction,
      Collection<ReceiverSpec> receiverSpecs) {
    this(stageId, dataSchema, inputs, toBitSet(receiverStages), exchangeType, distributionType, keys, prePartitioned,
        collations, sort, hashFunction, toSpecMap(receiverSpecs));
  }

  private static Map<Integer, ReceiverSpec> toSpecMap(Collection<ReceiverSpec> receiverSpecs) {
    Map<Integer, ReceiverSpec> result = new TreeMap<>();
    for (ReceiverSpec spec : receiverSpecs) {
      result.put(spec.receiverStageId(), spec);
    }
    return result;
  }

  private static BitSet toBitSet(int receiverStage) {
    BitSet bitSet = new BitSet(receiverStage + 1);
    bitSet.set(receiverStage);
    return bitSet;
  }

  private static BitSet toBitSet(@Nullable List<Integer> receiverStages) {
    BitSet bitSet = new BitSet();
    if (receiverStages == null || receiverStages.isEmpty()) {
      return bitSet;
    }
    for (int receiverStage : receiverStages) {
      bitSet.set(receiverStage);
    }
    return bitSet;
  }

  public boolean sharesReceiverStages(MailboxSendNode other) {
    return _receiverStages.intersects(other._receiverStages);
  }

  /// Returns the receiver stage ids, sorted in ascending order.
  public Iterable<Integer> getReceiverStageIds() {
    return () -> new Iterator<>() {
      int _next = _receiverStages.nextSetBit(0);

      @Override
      public boolean hasNext() {
        return _next >= 0;
      }

      @Override
      public Integer next() {
        int current = _next;
        _next = _receiverStages.nextSetBit(_next + 1);
        return current;
      }
    };
  }

  /// returns true if this node sends to multiple receivers
  public boolean isMultiSend() {
    return _receiverStages.cardinality() > 1;
  }

  @Deprecated
  public int getReceiverStageId() {
    Preconditions.checkState(!_receiverStages.isEmpty(), "Receivers not set");
    return _receiverStages.nextSetBit(0);
  }

  public void addReceiver(MailboxReceiveNode node) {
    if (_receiverStages.get(node.getStageId())) {
      throw new IllegalStateException("Receiver already added: " + node.getStageId());
    }
    _receiverStages.set(node.getStageId());
  }

  /// Undoes [#addReceiver(MailboxReceiveNode)]. It also removes the spec of that receiver, if any.
  public void removeReceiver(MailboxReceiveNode node) {
    if (!_receiverStages.get(node.getStageId())) {
      throw new IllegalStateException("Receiver not present: " + node.getStageId());
    }
    _receiverStages.clear(node.getStageId());
    _receiverSpecs.remove(node.getStageId());
  }

  /// Returns how this node sends to the given receiver stage: its explicit spec, or [#getDefaultReceiverSpec(int)].
  public ReceiverSpec getReceiverSpec(int receiverStageId) {
    ReceiverSpec spec = _receiverSpecs.get(receiverStageId);
    return spec != null ? spec : getDefaultReceiverSpec(receiverStageId);
  }

  /// Returns the spec of a receiver that has no explicit one: the distribution and the keys of the node, and every
  /// column.
  public ReceiverSpec getDefaultReceiverSpec(int receiverStageId) {
    return new ReceiverSpec(receiverStageId, _distributionType, _keys, null);
  }

  /// Returns the explicit specs, sorted by receiver stage id. Receivers without an explicit spec are not included.
  public Collection<ReceiverSpec> getReceiverSpecs() {
    return Collections.unmodifiableCollection(_receiverSpecs.values());
  }

  /// Returns true if every receiver reads the same data in the same way, which is the behavior of a node without
  /// explicit specs.
  public boolean hasUniformReceivers() {
    for (ReceiverSpec spec : _receiverSpecs.values()) {
      if (!spec.equals(getDefaultReceiverSpec(spec.receiverStageId()))) {
        return false;
      }
    }
    return true;
  }

  /// Sets how this node sends to one of its receiver stages.
  ///
  /// The node must not be a local exchange (`SINGLETON`) nor pre-partitioned, because the planner wires those 1-to-1,
  /// and the spec must only reference columns of the data schema of the node.
  public void setReceiverSpec(ReceiverSpec spec) {
    int receiverStageId = spec.receiverStageId();
    Preconditions.checkArgument(ReceiverSpec.isSupported(spec.distributionType()),
        "Unsupported distribution for a receiver spec: %s", spec.distributionType());
    Preconditions.checkArgument(_receiverStages.get(receiverStageId), "Stage %s is not a receiver of stage %s",
        receiverStageId, _stageId);
    Preconditions.checkState(ReceiverSpec.isSupported(_distributionType) && !_prePartitioned,
        "Stage %s cannot have receiver specs: its distribution is %s and pre-partitioned is %s", _stageId,
        _distributionType, _prePartitioned);
    // Some planner tests build nodes without a data schema.
    int numColumns = _dataSchema != null ? _dataSchema.size() : Integer.MAX_VALUE;
    for (int key : spec.keys()) {
      Preconditions.checkArgument(key >= 0 && key < numColumns, "Key %s is not a column of stage %s", key, _stageId);
    }
    if (spec.columnMask() != null) {
      for (int column : spec.columnMask()) {
        Preconditions.checkArgument(column >= 0 && column < numColumns, "Column %s is not a column of stage %s",
            column, _stageId);
      }
    }
    _receiverSpecs.put(receiverStageId, spec);
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  public void setDistributionType(RelDistribution.Type distributionType) {
    _distributionType = distributionType;
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public boolean isPrePartitioned() {
    return _prePartitioned;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public boolean isSort() {
    return _sort;
  }

  public String getHashFunction() {
    return _hashFunction;
  }

  @Override
  public String explain() {
    StringBuilder sb = new StringBuilder();
    sb.append("MAIL_SEND(");
    sb.append(_distributionType);
    sb.append(')');
    if (isPrePartitioned()) {
      sb.append("[PARTITIONED]");
    }
    if (isSort()) {
      sb.append("[SORTED]");
    }
    if (!hasUniformReceivers()) {
      sb.append(_receiverSpecs.values());
    }
    return sb.toString();
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxSend(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new MailboxSendNode(_stageId, _dataSchema, inputs, _receiverStages, _exchangeType, _distributionType, _keys,
        _prePartitioned, _collations, _sort, _hashFunction, _receiverSpecs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MailboxSendNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MailboxSendNode that = (MailboxSendNode) o;
    return Objects.equals(_receiverStages, that._receiverStages) && _prePartitioned == that._prePartitioned
        && _sort == that._sort && _exchangeType == that._exchangeType && _distributionType == that._distributionType
        && Objects.equals(_keys, that._keys) && Objects.equals(_collations, that._collations)
        && Objects.equals(_hashFunction, that._hashFunction) && Objects.equals(_receiverSpecs, that._receiverSpecs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _receiverStages, _exchangeType, _distributionType, _keys, _prePartitioned,
        _collations, _sort, _hashFunction, _receiverSpecs);
  }

  @Override
  public String toString() {
    return "MailboxSendNode{"
        + "_stageId=" + _stageId
        + ", _receivers=" + _receiverStages
        + '}';
  }
}
