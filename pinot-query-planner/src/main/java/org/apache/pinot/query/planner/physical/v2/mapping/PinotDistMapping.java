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
package org.apache.pinot.query.planner.physical.v2.mapping;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections4.CollectionUtils;


/**
 * Mapping specifically for Pinot Data Distribution and trait mapping. A mapping is defined for a source / destination
 * RelNode pair and is used to track how input fields are mapped to output fields.
 */
public class PinotDistMapping {
  private final int _sourceCount;
  private final Map<Integer, List<Integer>> _sourceToTargetMapping = new HashMap<>();

  public PinotDistMapping(int sourceCount) {
    _sourceCount = sourceCount;
    for (int i = 0; i < sourceCount; i++) {
      _sourceToTargetMapping.put(i, new ArrayList<>());
    }
  }

  public static PinotDistMapping identity(int sourceCount) {
    PinotDistMapping mapping = new PinotDistMapping(sourceCount);
    for (int i = 0; i < sourceCount; i++) {
      mapping.add(i, i);
    }
    return mapping;
  }

  public int getSourceCount() {
    return _sourceCount;
  }

  public List<Integer> getTargets(int source) {
    Preconditions.checkArgument(source >= 0 && source < _sourceCount, "Invalid source index: %s", source);
    List<Integer> target = _sourceToTargetMapping.get(source);
    return target == null ? List.of() : target;
  }

  public void add(int source, int target) {
    Preconditions.checkArgument(source >= 0 && source < _sourceCount, "Invalid source index: %s", source);
    _sourceToTargetMapping.computeIfAbsent(source, (x) -> new ArrayList<>()).add(target);
  }

  public List<List<Integer>> getMappedKeys(List<Integer> existingKeys) {
    List<List<Integer>> result = new ArrayList<>();
    computeAllMappings(0, existingKeys, this, new ArrayDeque<>(), result);
    return result;
  }

  public static RelCollation apply(RelCollation relCollation, PinotDistMapping mapping) {
    if (relCollation.getKeys().isEmpty()) {
      return relCollation;
    }
    List<RelFieldCollation> newFieldCollations = new ArrayList<>();
    for (RelFieldCollation fieldCollation : relCollation.getFieldCollations()) {
      List<Integer> newFieldIndices = mapping.getTargets(fieldCollation.getFieldIndex());
      if (CollectionUtils.isEmpty(newFieldIndices)) {
        break;
      }
      newFieldCollations.add(fieldCollation.withFieldIndex(newFieldIndices.get(0)));
    }
    return RelCollations.of(newFieldCollations);
  }

  /**
   * Consider a node that is partitioned on the key: [1]. If there's a project node on top of this, with project
   * expressions as: [RexInputRef#0, RexInputRef#1, RexInputRef#1], then the project node will have two hash
   * distribution descriptors: [1] and [2]. This method computes all such mappings for the given key indexes.
   * <p>
   *   This is a common occurrence in Calcite plans.
   * </p>
   */
  public static void computeAllMappings(int index, List<Integer> currentKey, PinotDistMapping mapping,
      Deque<Integer> runningKey, List<List<Integer>> newKeysSink) {
    if (index == currentKey.size()) {
      newKeysSink.add(new ArrayList<>(runningKey));
      return;
    }
    List<Integer> possibilities = mapping.getTargets(currentKey.get(index));
    for (int currentKeyPossibility : possibilities) {
      runningKey.addLast(currentKeyPossibility);
      computeAllMappings(index + 1, currentKey, mapping, runningKey, newKeysSink);
      runningKey.removeLast();
    }
  }
}
