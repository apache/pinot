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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The stats of a given query.
 * <p>
 * For the same query, multiple instances of this class may exist. Each of them will have a partial view of the stats.
 * Specifically, while the query is being executed, each operator will return its own partial view of the stats when
 * EOS block is sent.
 * <p>
 * Simple operations with a single upstream, like filters or transforms, would just add their own information to the
 * stats. More complex operations, like joins or receiving mailboxes, will merge the stats from all their upstreams and
 * add their own stats.
 * <p>
 * The complete stats for the query are obtained in the execution root (usually the broker) by merging the partial
 * views.
 * <p>
 * In order to reduce allocation, this class is mutable. Some operators may create their own stats, but most of them
 * will receive a stats object from the upstream operator and modify it by adding their own stats and sometimes merging
 * them with other upstream stats.
 */
public class MultiStageQueryStats {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageQueryStats.class);
  private final int _currentStageId;
  private final StageStats.Open _currentStats;
  /**
   * Known stats for stages whose id is higher than the current one.
   * <p>
   * A stage may not know all the stats whose id is higher than the current one, so this list may contain null values.
   * It may also grow in size when different merge methods are called.
   * <p>
   * For example the stats of the left hand side of a join may know stats of stages 3 and 4 and the right side may know
   * stats of stages 5. When merging the stats of the join, the stats of stages 5 will be added to this list.
   *
   * @see #mergeUpstream(List)
   * @see #mergeUpstream(MultiStageQueryStats)
   */
  private final ArrayList<StageStats.Closed> _closedStats;
  private static final MultiStageOperator.Type[] ALL_TYPES = MultiStageOperator.Type.values();

  private MultiStageQueryStats(int stageId) {
    _currentStageId = stageId;
    _currentStats = new StageStats.Open();
    _closedStats = new ArrayList<>();
  }

  private MultiStageQueryStats(MultiStageQueryStats other) {
    _currentStageId = other._currentStageId;
    _currentStats = StageStats.Open.copy(other._currentStats);
    _closedStats = new ArrayList<>(other._closedStats.size());
    for (StageStats.Closed closed : other._closedStats) {
      if (closed == null) {
        _closedStats.add(null);
      } else {
        _closedStats.add(StageStats.Closed.copy(closed));
      }
    }
  }

  public static MultiStageQueryStats emptyStats(int stageId) {
    return new MultiStageQueryStats(stageId);
  }

  public static MultiStageQueryStats copy(MultiStageQueryStats stats) {
    return new MultiStageQueryStats(stats);
  }

  public int getCurrentStageId() {
    return _currentStageId;
  }

  /**
   * Serialize the current stats in a way it is compatible with {@link #mergeUpstream(List)}.
   * <p>
   * The serialized stats are returned in a list where the index is the stage id. Stages downstream or not related to
   * the current one will be null.
   */
  public List<DataBuffer> serialize()
      throws IOException {

    ArrayList<DataBuffer> serializedStats = new ArrayList<>(getMaxStageId());
    for (int i = 0; i < _currentStageId; i++) {
      serializedStats.add(null);
    }

    try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream.Builder().get();
        DataOutputStream output = new DataOutputStream(baos)) {

      _currentStats.serialize(output);
      ByteBuffer currentBuf = ByteBuffer.wrap(baos.toByteArray());

      serializedStats.add(PinotByteBuffer.wrap(currentBuf));

      for (StageStats.Closed closedStats : _closedStats) {
        if (closedStats == null) {
          serializedStats.add(null);
          continue;
        }
        baos.reset();
        closedStats.serialize(output);
        ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
        serializedStats.add(PinotByteBuffer.wrap(buf));
      }
    }
    Preconditions.checkState(serializedStats.size() == getMaxStageId() + 1,
        "Serialized stats size is different from expected size. Expected %s, got %s",
        getMaxStageId() + 1, serializedStats.size());
    return serializedStats;
  }

  public StageStats.Open getCurrentStats() {
    return _currentStats;
  }

  /**
   * Returns the higher stage id known by this object.
   */
  public int getMaxStageId() {
    return _currentStageId + _closedStats.size();
  }

  /**
   * Get the stats of a stage whose id is higher than the current one.
   * <p>
   * This method returns null in case the stage id is unknown by this stage or no stats are stored for it.
   */
  @Nullable
  public StageStats.Closed getUpstreamStageStats(int stageId) {
    if (stageId <= _currentStageId) {
      throw new IllegalArgumentException("Stage " + stageId + " cannot be upstream of current stage "
          + _currentStageId);
    }

    int index = stageId - _currentStageId - 1;
    if (index >= _closedStats.size()) {
      return null;
    }
    return _closedStats.get(index);
  }

  public void mergeInOrder(MultiStageQueryStats otherStats, MultiStageOperator.Type type,
      StatMap<?> statMap) {
    Preconditions.checkArgument(_currentStageId == otherStats._currentStageId,
        "Cannot merge stats from different stages (%s and %s)", _currentStageId, otherStats._currentStageId);
    mergeUpstream(otherStats);
    StageStats.Open currentStats = getCurrentStats();
    currentStats.concat(otherStats.getCurrentStats());
    currentStats.addLastOperator(type, statMap);
  }

  private void growUpToStage(int stageId) {
    _closedStats.ensureCapacity(stageId - _currentStageId);
    while (getMaxStageId() < stageId) {
      _closedStats.add(null);
    }
  }

  /**
   * Merge upstream stats from another MultiStageQueryStats object into this one.
   * <p>
   * Only the stages whose id is higher than the current one are merged. The reason to do so is that upstream stats
   * should be already closed while current stage may need some extra tuning.
   * <p>
   * For example set operations may need to merge the stats from all its upstreams before concatenating stats of the
   * current stage.
   */
  public void mergeUpstream(MultiStageQueryStats otherStats) {
    Preconditions.checkArgument(_currentStageId <= otherStats._currentStageId,
        "Cannot merge stats from early stage %s into stats of later stage %s",
        otherStats._currentStageId, _currentStageId);

    growUpToStage(otherStats.getMaxStageId());

    int currentDiff = otherStats._currentStageId - _currentStageId;
    if (currentDiff > 0) {
      StageStats.Closed close = otherStats._currentStats.close();
      int selfIdx = currentDiff - 1;
      StageStats.Closed myStats = _closedStats.get(selfIdx);
      if (myStats == null) {
        _closedStats.set(selfIdx, close);
      } else {
        myStats.merge(close);
      }
    }

    for (int i = 0; i < otherStats._closedStats.size(); i++) {
      StageStats.Closed otherStatsForStage = otherStats._closedStats.get(i);
      if (otherStatsForStage == null) {
        continue;
      }
      int selfIdx = i + currentDiff;
      StageStats.Closed myStats = _closedStats.get(selfIdx);
      try {
        if (myStats == null) {
          _closedStats.set(selfIdx, otherStatsForStage);
          assert getUpstreamStageStats(i + otherStats._currentStageId + 1) == otherStatsForStage;
        } else {
          myStats.merge(otherStatsForStage);
        }
      } catch (IllegalArgumentException | IllegalStateException ex) {
        LOGGER.warn("Error merging stats on stage {}. Ignoring the new stats", i, ex);
      }
    }
  }

  public void mergeUpstream(List<DataBuffer> otherStats) {
    for (int i = 0; i <= _currentStageId && i < otherStats.size(); i++) {
      if (otherStats.get(i) != null) {
        throw new IllegalArgumentException("Cannot merge stats from early stage " + i + " into stats of "
            + "later stage " + _currentStageId);
      }
    }
    growUpToStage(otherStats.size() - 1);

    for (int i = _currentStageId + 1; i < otherStats.size(); i++) {
      DataBuffer otherBuf = otherStats.get(i);
      if (otherBuf != null) {
        StageStats.Closed myStats = getUpstreamStageStats(i);
        try (PinotInputStream dis = otherBuf.openInputStream()) {
          if (myStats == null) {
            StageStats.Closed deserialized = StageStats.Closed.deserialize(dis);
            _closedStats.set(i - _currentStageId - 1, deserialized);
            assert getUpstreamStageStats(i) == deserialized;
          } else {
            myStats.merge(dis);
          }
        } catch (IOException ex) {
          LOGGER.warn("Error deserializing stats on stage " + i + ". Considering the new stats empty", ex);
        } catch (IllegalArgumentException | IllegalStateException ex) {
          LOGGER.warn("Error merging stats on stage " + i + ". Ignoring the new stats", ex);
        }
      }
    }
  }

  public List<StageStats.Closed> getClosedStats() {
    return Collections.unmodifiableList(_closedStats);
  }

  public JsonNode asJson() {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("stage", _currentStageId);
    node.set("open", _currentStats.asJson());

    ArrayNode closedStats = JsonUtils.newArrayNode();
    for (StageStats.Closed closed : _closedStats) {
      if (closed == null) {
        closedStats.addNull();
      } else {
        closedStats.add(closed.asJson());
      }
    }
    node.set("closed", closedStats);
    return node;
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiStageQueryStats that = (MultiStageQueryStats) o;
    return _currentStageId == that._currentStageId && Objects.equals(_currentStats, that._currentStats)
        && Objects.equals(_closedStats, that._closedStats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_currentStageId, _currentStats, _closedStats);
  }

  /**
   * {@code StageStats} tracks execution statistics for a single stage.
   * <p>
   * Instances of this class may not have the complete stat information for the stage. Specifically, while the query
   * is being executed, each OpChain will contain its own partial view of the stats.
   * The final stats for the stage are obtained in the execution root (usually the broker) by
   * {@link Closed#merge(StageStats) merging} the partial views from all OpChains.
   */
  public abstract static class StageStats {

    /**
     * The types of the operators in the stage.
     * <p>
     * The operator index used here is the index of the operator in the operator tree, in the order of the inorder
     * traversal. That means that the first value is the leftmost leaf, and the last value is the root.
     * <p>
     * This list contains no null values.
     */
    protected final List<MultiStageOperator.Type> _operatorTypes;
    /**
     * The stats associated with the given operator index.
     * <p>
     * The operator index used here is the index of the operator in the operator tree, in the order of the inorder
     * traversal. That means that the first value is the leftmost leaf, and the last value is the root.
     * <p>
     * This list contains no null values.
     */
    protected final List<StatMap<?>> _operatorStats;

    private StageStats() {
      this(new ArrayList<>(), new ArrayList<>());
    }

    private StageStats(List<MultiStageOperator.Type> operatorTypes, List<StatMap<?>> operatorStats) {
      Preconditions.checkArgument(operatorTypes.size() == operatorStats.size(),
          "Operator types and stats must have the same size (%s != %s)",
          operatorTypes.size(), operatorStats.size());
      for (int i = 0; i < operatorTypes.size(); i++) {
        if (operatorTypes.get(i) == null) {
          throw new IllegalArgumentException("Unexpected null operator type at index " + i);
        }
      }
      for (int i = 0; i < operatorStats.size(); i++) {
        if (operatorStats.get(i) == null) {
          throw new IllegalArgumentException("Unexpected null operator stats of type " + operatorTypes.get(i)
              + " at index " + i);
        }
      }
      _operatorTypes = operatorTypes;
      _operatorStats = operatorStats;
    }

    List<StatMap<?>> copyOperatorStats() {
      ArrayList<StatMap<?>> copy = new ArrayList<>(_operatorStats.size());
      for (StatMap<?> operatorStat : _operatorStats) {
        copy.add(new StatMap<>(operatorStat));
      }
      return copy;
    }

    /**
     * Return the stats associated with the given operator index.
     * <p>
     * The operator index used here is the index of the operator in the operator tree, in the order of the inorder
     * traversal.
     * That means that the first value is the leftmost leaf, and the last value is the root.
     * <p>
     * It is the operator responsibility to store here its own stats and that must be done just before the end of stream
     * block is sent. This means that calling this method before the stats is added will throw an index out of bounds.
     *
     * @param operatorIdx The operator index in inorder traversal of the operator tree.
     * @return The value of the stat or null if no stat is registered.
     * @throws IndexOutOfBoundsException if there is no stats for the given operator index.
     */
    public StatMap<?> getOperatorStats(int operatorIdx) {
      return _operatorStats.get(operatorIdx);
    }

    public MultiStageOperator.Type getOperatorType(int index) {
      return _operatorTypes.get(index);
    }

    public MultiStageOperator.Type getLastType() {
      return _operatorTypes.get(_operatorTypes.size() - 1);
    }

    public StatMap<?> getLastOperatorStats() {
      return _operatorStats.get(_operatorStats.size() - 1);
    }

    public int getLastOperatorIndex() {
      return _operatorStats.size() - 1;
    }

    public void forEach(BiConsumer<MultiStageOperator.Type, StatMap<?>> consumer) {
      Iterator<MultiStageOperator.Type> typeIterator = _operatorTypes.iterator();
      Iterator<StatMap<?>> statIterator = _operatorStats.iterator();
      while (typeIterator.hasNext()) {
        consumer.accept(typeIterator.next(), statIterator.next());
      }
    }

    public JsonNode asJson() {
      ArrayNode json = JsonUtils.newArrayNode();

      for (int i = 0; i < _operatorStats.size(); i++) {
        ObjectNode statNode = JsonUtils.newObjectNode();
        statNode.put("type", _operatorTypes.get(i).name());
        StatMap<?> stats = _operatorStats.get(i);
        if (!stats.isEmpty()) {
          statNode.set("stats", stats.asJson());
        }
        json.add(statNode);
      }

      return json;
    }

    /**
     * Serialize the stats to the given output.
     * <p>
     * Stats can be then deserialized as {@link Closed} with {@link Closed#deserialize(DataInput)}.
     */
    public void serialize(DataOutput output)
        throws IOException {
      // TODO: we can serialize with short or variable size
      output.writeInt(_operatorTypes.size());
      assert MultiStageOperator.Type.values().length < Byte.MAX_VALUE : "Too many operator types. "
          + "Need to increase the number of bytes size per operator type";
      for (int i = 0; i < _operatorTypes.size(); i++) {
        output.writeByte(_operatorTypes.get(i).ordinal());
        StatMap<?> statMap = _operatorStats.get(i);
        statMap.serialize(output);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StageStats that = (StageStats) o;
      return Objects.equals(_operatorTypes, that._operatorTypes) && Objects.equals(_operatorStats, that._operatorStats);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_operatorTypes, _operatorStats);
    }

    @Override
    public String toString() {
      return asJson().toString();
    }

    /**
     * Closed stats represent the stats of upstream stages.
     * <p>
     * These stats can be serialized or deserialized and merged with other stats of the same stage, but operators must
     * never add entries for itself in upstream stats.
     */
    public static class Closed extends StageStats {
      public Closed(List<MultiStageOperator.Type> operatorTypes, List<StatMap<?>> operatorStats) {
        super(operatorTypes, operatorStats);
      }

      public static Closed copy(Closed other) {
        return new Closed(new ArrayList<>(other._operatorTypes), other.copyOperatorStats());
      }

      /**
       * Merges the stats from another StageStats object into this one.
       * <p>
       * This object is modified in place while the other object is not modified.
       * Both stats must belong to the same stage.
       */
      // We need to deal with unchecked because Java type system is not expressive enough to handle this at static time
      // But we know we are merging the same stat types because they were created for the same operation.
      // There is also a dynamic check in the StatMap.merge() method.
      @SuppressWarnings("unchecked")
      public void merge(StageStats other) {
        Preconditions.checkState(_operatorTypes.equals(other._operatorTypes), "Cannot merge stats from "
            + "different stages. Found types %s and %s", _operatorTypes, other._operatorTypes);
        for (int i = 0; i < _operatorStats.size(); i++) {
          StatMap otherStats = other._operatorStats.get(i);
          StatMap myStats = _operatorStats.get(i);
          myStats.merge(otherStats);
        }
      }

      public void merge(PinotInputStream input)
          throws IOException {
        int numOperators = input.readInt();
        if (numOperators != _operatorTypes.size()) {
          Closed deserialized;
          try {
            deserialized = deserialize(input, numOperators);
          } catch (IOException e) {
            throw new IOException("Cannot merge stats from stages with different operators. Expected "
                + _operatorTypes + " operators, got " + numOperators, e);
          } catch (Exception e) {
            throw new RuntimeException("Cannot merge stats from stages with different operators. Expected "
                + _operatorTypes + " operators, got " + numOperators, e);
          }
          throw new RuntimeException("Cannot merge stats from stages with different operators. Expected "
              + _operatorTypes + " operators, got " + deserialized._operatorTypes
              + ". Deserialized stats: " + deserialized);
        }
        for (int i = 0; i < numOperators; i++) {
          byte ordinal = input.readByte();
          if (ordinal != _operatorTypes.get(i).ordinal()) {
            throw new IllegalStateException("Cannot merge stats from stages with different operators. Expected "
                + " operator " + _operatorTypes.get(i) + "at index " + i + ", got " + ordinal);
          }
          _operatorStats.get(i).merge(input);
        }
      }

      /**
       * Same as {@link #merge(StageStats)} but reads the stats from a DataInput, so it should be slightly faster given
       * it doesn't need to create new objects.
       */
      public static Closed deserialize(DataInput input)
          throws IOException {
        return deserialize(input, input.readInt());
      }
    }

    public static Closed deserialize(DataInput input, int numOperators)
        throws IOException {
      List<MultiStageOperator.Type> operatorTypes = new ArrayList<>(numOperators);
      List<StatMap<?>> operatorStats = new ArrayList<>(numOperators);

      MultiStageOperator.Type[] allTypes = ALL_TYPES;
      try {
        for (int i = 0; i < numOperators; i++) {
          byte ordinal = input.readByte();
          if (ordinal < 0 || ordinal >= allTypes.length) {
            throw new IllegalStateException(
                "Invalid operator type ordinal " + ordinal + " at index " + i + ". " + "Deserialized so far: "
                    + new Closed(operatorTypes, operatorStats));
          }
          MultiStageOperator.Type type = allTypes[ordinal];
          operatorTypes.add(type);

          @SuppressWarnings("unchecked")
          StatMap<?> opStatMap = StatMap.deserialize(input, type.getStatKeyClass());
          operatorStats.add(opStatMap);
        }
        return new Closed(operatorTypes, operatorStats);
      } catch (IOException e) {
        throw new IOException("Error deserializing stats. Deserialized so far: "
            + new Closed(operatorTypes.subList(0, operatorStats.size()), operatorStats), e);
      } catch (RuntimeException e) {
        throw new RuntimeException("Error deserializing stats. Deserialized so far: "
            + new Closed(operatorTypes.subList(0, operatorStats.size()), operatorStats), e);
      }
    }

    /**
     * Open stats represent the stats of the current stage.
     * <p>
     * These stats can be modified by the operator that is currently executing. Specifically they can add stats for
     * the current operator or merge with other open stats from the same stage.
     */
    public static class Open extends StageStats {
      private Open() {
        super();
      }

      /// Adds the given stats as the metrics for the last operator found in the stage so far.
      ///
      /// @param statMap The stats for the operator to add. The ownership of this map will be transferred to this
      ///  object, so the caller should not modify it after calling this method.
      public Open addLastOperator(MultiStageOperator.Type type, StatMap<?> statMap) {
        Preconditions.checkArgument(statMap.getKeyClass().equals(type.getStatKeyClass()),
            "Expected stats of class %s for type %s but found class %s",
            type.getStatKeyClass(), type, statMap.getKeyClass());
        if (!_operatorStats.isEmpty() && _operatorStats.get(_operatorStats.size() - 1) == statMap) {
          // This is mostly useful to detect errors in the code.
          // In the future we may choose to evaluate it only if asserts are enabled
          throw new IllegalArgumentException("Cannot add the same stat map twice.");
        }
        Preconditions.checkNotNull(type, "Cannot add null operator type");
        Preconditions.checkNotNull(statMap, "Cannot add null stats");
        _operatorTypes.add(type);
        _operatorStats.add(statMap);
        return this;
      }

      public static Open copy(Open other) {
        Open copy = new Open();
        copy._operatorTypes.addAll(other._operatorTypes);
        copy._operatorStats.addAll(other.copyOperatorStats());
        return copy;
      }

      /**
       * Adds the given stats at the end of this object.
       */
      public void concat(StageStats.Open other) {
        _operatorTypes.addAll(other._operatorTypes);
        _operatorStats.addAll(other._operatorStats);
      }

      public Closed close() {
        return new Closed(_operatorTypes, _operatorStats);
      }
    }
  }

  public static class Builder {
    private final MultiStageQueryStats _stats;

    public Builder(int stageId) {
      _stats = new MultiStageQueryStats(stageId);
    }

    public Builder customizeOpen(Consumer<StageStats.Open> customizer) {
      customizer.accept(_stats._currentStats);
      return this;
    }

    /**
     * Adds a new operator to the stats.
     * @param consumer a function that will be called with a new and empty open stats object and returns the closed stat
     *                 to be added. The received object can be freely modified and close.
     */
    public Builder addLast(Function<StageStats.Open, StageStats.Closed> consumer) {
      StageStats.Open open = new StageStats.Open();
      _stats._closedStats.add(consumer.apply(open));
      return this;
    }

    public MultiStageQueryStats build() {
      return _stats;
    }
  }
}
