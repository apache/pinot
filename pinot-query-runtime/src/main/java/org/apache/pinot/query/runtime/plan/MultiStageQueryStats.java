package org.apache.pinot.query.runtime.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
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
   * @see #mergeInOrder(MultiStageQueryStats, MultiStageOperator.Type, StatMap)
   */
  private final ArrayList<StageStats.Closed> _closedStats;

  private MultiStageQueryStats(int stageId) {
    _currentStageId = stageId;
    _currentStats = new StageStats.Open();
    _closedStats = new ArrayList<>();
  }

  private static MultiStageQueryStats create(int stageId, MultiStageOperator.Type type, @Nullable StatMap<?> opStats) {
    MultiStageQueryStats multiStageQueryStats = new MultiStageQueryStats(stageId);
    multiStageQueryStats.getCurrentStats().addLastOperator(type, opStats);
    return multiStageQueryStats;
  }

  public static MultiStageQueryStats createRoot() {
    return new MultiStageQueryStats(0);
  }

  public static MultiStageQueryStats createLeaf(int stageId, @Nullable StatMap<DataTable.MetadataKey> opStats) {
    return create(stageId, MultiStageOperator.Type.LEAF, opStats);
  }

  public static MultiStageQueryStats createLiteral(int stageId, StatMap<MultiStageOperator.BaseStatKeys> statMap) {
    return create(stageId, MultiStageOperator.Type.LITERAL, statMap);
  }

  public static MultiStageQueryStats createCancelledSend(int stageId,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    return create(stageId, MultiStageOperator.Type.MAILBOX_SEND, statMap);
  }

  public static MultiStageQueryStats createReceive(int stageId, StatMap<BaseMailboxReceiveOperator.StatKey> stats) {
    return create(stageId, MultiStageOperator.Type.MAILBOX_RECEIVE, stats);
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
  public List<ByteBuffer> serialize()
      throws IOException {

    ArrayList<ByteBuffer> serializedStats = new ArrayList<>(getMaxStageId());
    for (int i = 0; i < _currentStageId; i++) {
      serializedStats.add(null);
    }

    try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos)) {

      _currentStats.serialize(output);
      ByteBuffer currentBuf = ByteBuffer.wrap(baos.toByteArray());

      serializedStats.add(currentBuf);

      for (StageStats.Closed closedStats : _closedStats) {
        if (closedStats == null) {
          serializedStats.add(null);
          continue;
        }
        baos.reset();
        closedStats.serialize(output);
        ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
        serializedStats.add(buf);
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
        LOGGER.warn("Error merging stats on stage " + i + ". Ignoring the new stats", ex);
      }
    }
  }

  public void mergeUpstream(List<ByteBuffer> otherStats) throws IOException {
    for (int i = 0; i <= _currentStageId && i < otherStats.size(); i++) {
      if (otherStats.get(i) != null) {
        throw new IllegalArgumentException("Cannot merge stats from early stage " + i + " into stats of "
            + "later stage " + _currentStageId);
      }
    }
    growUpToStage(otherStats.size() - 1);

    for (int i = _currentStageId + 1; i < otherStats.size(); i++) {
      ByteBuffer otherBuf = otherStats.get(i);
      if (otherBuf != null) {
        StageStats.Closed myStats = getUpstreamStageStats(i);
        try (InputStream is = new ByteBufferInputStream(Collections.singletonList(otherBuf));
            DataInputStream dis = new DataInputStream(is)) {
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
          "Operator types and stats must have the same size");
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
      output.writeInt(_operatorTypes.size());
      assert MultiStageOperator.Type.values().length < Byte.MAX_VALUE : "Too many operator types. "
          + "Need to increase the number of bytes size per operator type";
      for (MultiStageOperator.Type operatorType : _operatorTypes) {
        output.writeByte(operatorType.ordinal());
      }
      for (StatMap<?> stats : _operatorStats) {
        if (stats.isEmpty()) {
          output.writeBoolean(false);
        } else {
          output.writeBoolean(true);
          stats.serialize(output);
        }
      }
    }

    public <A> void accept(StatsVisitor<A> visitor, A arg) {
      for (int i = 0; i < _operatorTypes.size(); i++) {
        MultiStageOperator.Type type = _operatorTypes.get(i);
        StatMap<?> statMap = _operatorStats.get(i);
        if (statMap == null) {
          continue;
        }
        switch (type) {
          case AGGREGATE:
            visitor.visitAggregate((StatMap<AggregateOperator.StatKey>) statMap, arg);
            break;
          case HASH_JOIN:
            visitor.visitHashJoin((StatMap<HashJoinOperator.StatKey>) statMap, arg);
            break;
          case LEAF:
            visitor.visitLeaf((StatMap<DataTable.MetadataKey>) statMap, arg);
            break;
          case MAILBOX_RECEIVE:
            visitor.visitMailboxReceive((StatMap<BaseMailboxReceiveOperator.StatKey>) statMap, arg);
            break;
          case MAILBOX_SEND:
            visitor.visitMailboxSend((StatMap<MailboxSendOperator.StatKey>) statMap, arg);
            break;
          case FILTER:
          case INTERSECT:
          case LITERAL:
          case MINUS:
          case PIPELINE_BREAKER:
          case SORT:
          case TRANSFORM:
          case UNION:
          case WINDOW:
            visitor.visitBase((StatMap<MultiStageOperator.BaseStatKeys>) statMap, arg);
            break;
          default:
            throw new IllegalStateException("Unknown operator type: " + type);
        }
      }
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
            + "different stages");
        for (int i = 0; i < _operatorStats.size(); i++) {
          StatMap otherStats = other._operatorStats.get(i);
          StatMap myStats = _operatorStats.get(i);
          myStats.merge(otherStats);
        }
      }

      public void merge(DataInputStream input)
          throws IOException {
        int numOperators = input.readInt();
        Preconditions.checkState(numOperators == _operatorTypes.size(),
            "Cannot merge stats from stages with different operators");
        for (int i = 0; i < numOperators; i++) {
          MultiStageOperator.Type expectedType = MultiStageOperator.Type.values()[input.readByte()];
          Preconditions.checkState(expectedType == _operatorTypes.get(i),
              "Cannot merge stats from stages with different operators");
        }
        for (int i = 0; i < numOperators; i++) {
          if (input.readBoolean()) {
            _operatorStats.get(i).merge(input);
          }
        }
      }

      /**
       * Same as {@link #merge(StageStats)} but reads the stats from a DataInput, so it should be slightly faster given
       * it doesn't need to create new objects.
       */
      public static Closed deserialize(DataInput input)
          throws IOException {
        int numOperators = input.readInt();
        List<MultiStageOperator.Type> operatorTypes = new ArrayList<>(numOperators);
        for (int i = 0; i < numOperators; i++) {
          // This assumes the number of operator types at serialized time is the same as at deserialization time.
          operatorTypes.add(MultiStageOperator.Type.values()[input.readByte()]);
        }
        List<StatMap<?>> operatorStats = new ArrayList<>(numOperators);
        for (int i = 0; i < numOperators; i++) {
          if (input.readBoolean()) {
            operatorStats.add(operatorTypes.get(i).deserializeStats(input));
          } else {
            operatorStats.add(operatorTypes.get(i).emptyStats());
          }
        }
        return new Closed(operatorTypes, operatorStats);
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

      public void addLastOperator(MultiStageOperator.Type type, StatMap<?> statMap) {
        Preconditions.checkNotNull(type, "Cannot add null operator type");
        Preconditions.checkNotNull(statMap, "Cannot add null stats");
        _operatorTypes.add(type);
        _operatorStats.add(statMap);
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

  public interface StatsVisitor<A> {
    void visitBase(StatMap<MultiStageOperator.BaseStatKeys> stats, A arg);
    void visitLeaf(StatMap<DataTable.MetadataKey> stats, A arg);
    void visitAggregate(StatMap<AggregateOperator.StatKey> stats, A arg);
    void visitHashJoin(StatMap<HashJoinOperator.StatKey> stats, A arg);
    void visitMailboxReceive(StatMap<BaseMailboxReceiveOperator.StatKey> statMap, A arg);
    void visitMailboxSend(StatMap<MailboxSendOperator.StatKey> statMap, A arg);
  }
}
