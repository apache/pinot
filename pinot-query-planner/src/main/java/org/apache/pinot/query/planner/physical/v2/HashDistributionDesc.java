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
package org.apache.pinot.query.planner.physical.v2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;


/**
 * Descriptor for hash distribution of data.
 * <p>
 *   <b>Example:</b> Say a given plan node will have n streams of data in the output. Then, if the
 *   {@link #_keys} is [3, 1], {@link #_hashFunction} is "Murmur" and {@link #_numPartitions} is 5,
 *   then the record to stream mapping can be determined using (Murmur(valueAtIndex3, valueAtIndex1) % 5) % n.
 * </p>
 */
public class HashDistributionDesc {
  private final int _cachedHashCode;
  private final List<Integer> _keys;
  private final String _hashFunction;
  private final int _numPartitions;

  public HashDistributionDesc(List<Integer> keys, String hashFunction, int numPartitions) {
    _cachedHashCode = Objects.hash(keys, hashFunction, numPartitions);
    _keys = keys;
    _hashFunction = hashFunction;
    _numPartitions = numPartitions;
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public String getHashFunction() {
    return _hashFunction;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  /**
   * Returns the hash distribution descriptor for the given target mapping, or {@code null} if we can't preserve
   * partitioning info.
   */
  public Set<HashDistributionDesc> apply(PinotDistMapping mapping) {
    for (Integer currentKey : _keys) {
      if (currentKey >= mapping.getSourceCount() || CollectionUtils.isEmpty(mapping.getTargets(currentKey))) {
        return Set.of();
      }
    }
    List<List<Integer>> newKeys = new ArrayList<>();
    PinotDistMapping.computeAllMappings(0, _keys, mapping, new ArrayDeque<>(), newKeys);
    Set<HashDistributionDesc> newDescs = new HashSet<>();
    for (List<Integer> newKey : newKeys) {
      newDescs.add(new HashDistributionDesc(newKey, _hashFunction, _numPartitions));
    }
    return newDescs;
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
        && Objects.equals(_keys, that._keys);
  }

  @Override
  public int hashCode() {
    return _cachedHashCode;
  }
}
