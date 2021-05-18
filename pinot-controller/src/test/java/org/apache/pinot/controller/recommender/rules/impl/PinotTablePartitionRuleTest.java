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

package org.apache.pinot.controller.recommender.rules.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotTablePartitionRuleTest {

  private static final double TOP_CANDIDATE_RATIO = 0.8;
  private static final int NUM_PARTITIONS = 32;

  @Test
  public void testFindBestColumnForPartitioning() {

    // d has max weight, but c has more cardinality
    String[] columnNames = {"a", "b", "c", "d", "e"};
    double[] cardinalities = {100, 90, 80, 70, 60};
    double[] weights = {0.2, 0.3, 0.7, 0.8, 0.5};
    assertEquals(Optional.of("c"), findBestColumn(columnNames, cardinalities, weights));

    // d has max weight and max cardinality
    columnNames = new String[] {"a", "b", "c", "d", "e"};
    cardinalities = new double[] {100, 90, 80, 200, 60};
    weights = new double[] {0.2, 0.3, 0.7, 0.8, 0.5};
    assertEquals(Optional.of("d"), findBestColumn(columnNames, cardinalities, weights));

    // d has max weight, but its cardinality compared to numPartition is lower than threshold;
    columnNames = new String[] {"a", "b", "c", "d", "e"};
    cardinalities = new double[] {100, 90, 80, 10, 60};
    weights = new double[] {0.1, 0.1, 0.4, 0.8, 0.2};
    assertEquals(Optional.of("c"), findBestColumn(columnNames, cardinalities, weights));
  }

  private Optional<String> findBestColumn(String[] columnNames, double[] cardinalities, double[] weights) {
    List<Pair<String, Double>> colNameToWeightPairs = new ArrayList<>();
    Map<String, Double> colNameToCardinality = new HashMap<>();
    for (int i = 0; i < columnNames.length; i++) {
      colNameToCardinality.put(columnNames[i], cardinalities[i]);
      colNameToWeightPairs.add(ImmutablePair.of(columnNames[i], weights[i]));
    }
    return PinotTablePartitionRule.findBestColumnForPartitioning(
        colNameToWeightPairs,
        colNameToCardinality::get,
        TOP_CANDIDATE_RATIO,
        NUM_PARTITIONS
    );
  }
}