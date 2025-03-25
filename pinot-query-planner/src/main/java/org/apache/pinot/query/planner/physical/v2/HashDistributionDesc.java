package org.apache.pinot.query.planner.physical.v2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Descriptor for hash distribution of data.
 * <p>
 *   <b>Example:</b> Say a given plan node will have n streams of data in the output. Then, if the
 *   {@link #_keyIndexes} is [3, 1], {@link #_hashFunction} is "Murmur" and {@link #_numPartitions} is 5,
 *   then the record to stream mapping can be determined using (Murmur(valueAtIndex3, valueAtIndex1) % 5) % n.
 * </p>
 */
public class HashDistributionDesc {
  private final int _cachedHashCode;
  private final List<Integer> _keyIndexes;
  private final String _hashFunction;
  private final int _numPartitions;

  public HashDistributionDesc(List<Integer> keyIndexes, String hashFunction, int numPartitions) {
    _cachedHashCode = Objects.hash(keyIndexes, hashFunction, numPartitions);
    _keyIndexes = keyIndexes;
    _hashFunction = hashFunction;
    _numPartitions = numPartitions;
  }

  public List<Integer> getKeyIndexes() {
    return _keyIndexes;
  }

  public String getHashFunction() {
    return _hashFunction;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  @Nullable
  public Set<HashDistributionDesc> apply(Map<Integer, List<Integer>> mapping) {
    for (Integer currentKeyIndex : _keyIndexes) {
      if (!mapping.containsKey(currentKeyIndex) || mapping.get(currentKeyIndex).isEmpty()) {
        return null;
      }
    }
    List<List<Integer>> allNewKeys = new ArrayList<>();
    computeAllMappings(0, _keyIndexes, mapping, new ArrayDeque<>(), allNewKeys);
    Set<HashDistributionDesc> result = new HashSet<>();
    for (List<Integer> newKey : allNewKeys) {
      result.add(new HashDistributionDesc(newKey, _hashFunction, _numPartitions));
    }
    return result;
  }

  /**
   * Consider a node that is partitioned on the key: [1]. If there's a project node on top of this, with project
   * expressions as: [RexInputRef#0, RexInputRef#1, RexInputRef#1], then the project node will have two hash
   * distribution descriptors: [1] and [2]. This method computes all such mappings for the given key indexes.
   */
  static void computeAllMappings(int index, List<Integer> currentKey, Map<Integer, List<Integer>> mapping,
      Deque<Integer> runningKey, List<List<Integer>> newKeysSink) {
    if (index == currentKey.size()) {
      newKeysSink.add(new ArrayList<>(runningKey));
      return;
    }
    List<Integer> possibilities = mapping.get(currentKey.get(index));
    for (int currentKeyPossibility : possibilities) {
      runningKey.addLast(currentKeyPossibility);
      computeAllMappings(index + 1, currentKey, mapping, runningKey, newKeysSink);
      runningKey.removeLast();
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
    HashDistributionDesc that = (HashDistributionDesc) o;
    return _numPartitions == that._numPartitions && Objects.equals(_hashFunction, that._hashFunction)
        && Objects.equals(_keyIndexes, that._keyIndexes);
  }

  @Override
  public int hashCode() {
    return _cachedHashCode;
  }
}
