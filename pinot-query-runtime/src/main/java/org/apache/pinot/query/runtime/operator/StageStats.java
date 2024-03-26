package org.apache.pinot.query.runtime.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * {@code StageStats} tracks execution statistics for a stage.
 *
 * Instances of this class may not have the complete stat information for the stage. Specifically, while the query
 * is being executed, each OpChain will contain its own partial view of the stats.
 * The final stats for the stage are obtained in the execution root (usually the broker) by
 * {@link #merge(StageStats) merging} the partial views from all OpChains.
 */
public class StageStats {

  private final List<MultiStageOperator.Type> _operatorTypes;
  private final List<StatMap<?>> _operatorStats;

  public StageStats() {
    this(new ArrayList<>(), new ArrayList<>());
  }

  public StageStats(List<MultiStageOperator.Type> operatorTypes, List<StatMap<?>> operatorStats) {
    Preconditions.checkArgument(operatorTypes.size() == operatorStats.size(),
        "Operator types and stats must have the same size");
    _operatorTypes = operatorTypes;
    _operatorStats = operatorStats;
  }

  /**
   * Return the stats associated with the given operator index.
   *
   * The operator index used here is the index of the operator in the operator tree, in the order of the inorder
   * traversal.
   * That means that the first value is the leftmost leaf, and the last value is the root.
   *
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

  public MultiStageOperator.Type getLastType() {
    return _operatorTypes.get(_operatorTypes.size() - 1);
  }

  public StatMap<?> getLastOperatorStats() {
    return _operatorStats.get(_operatorStats.size() - 1);
  }

  public JsonNode asJson() {
    ObjectNode json = JsonUtils.newObjectNode();

    ArrayNode operationStats = JsonUtils.newArrayNode();
    for (StatMap<?> stats : _operatorStats) {
      if (stats == null) {
        operationStats.addNull();
      } else {
        operationStats.add(stats.asJson());
      }
    }
    json.set("operationStats", operationStats);

    return json;
  }

  public void serialize(DataOutput output)
      throws IOException {
    output.writeInt(_operatorTypes.size());
    assert MultiStageOperator.Type.values().length < 256 : "Too many operator types. Need to increase the number of "
        + "bytes size per operator type";
    for (MultiStageOperator.Type operatorType : _operatorTypes) {
      output.writeByte(operatorType.ordinal());
    }
    for (StatMap<?> stats : _operatorStats) {
      if (stats == null) {
        output.writeBoolean(false);
      } else {
        output.writeBoolean(true);
        stats.serialize(output);
      }
    }
  }

  /**
   * Merges the stats from another StageStats object into this one.
   *
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
      if (myStats == null) {
        _operatorStats.set(i, otherStats);
      } else if (otherStats != null) {
        myStats.merge(otherStats);
      }
    }
  }

  /**
   * Same as {@link #merge(StageStats)} but reads the stats from a DataInput, so it should be slightly faster given
   * it doesn't need to create new objects.
   */
  public static StageStats deserialize(DataInput input)
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
        operatorStats.add(null);
      }
    }
    return new StageStats(operatorTypes, operatorStats);
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  public void addLastOperator(MultiStageOperator.Type type, StatMap<?> statMap) {
    _operatorTypes.add(type);
    _operatorStats.add(statMap);
  }

  public void concat(StageStats other) {
    _operatorTypes.addAll(other._operatorTypes);
    _operatorStats.addAll(other._operatorStats);
  }
}
