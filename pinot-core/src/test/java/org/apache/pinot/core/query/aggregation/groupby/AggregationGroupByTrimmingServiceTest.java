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
package org.apache.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationGroupByTrimmingServiceTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final QueryContext QUERY_CONTEXTS = QueryContextConverterUtils
      .getQueryContextFromPQL("SELECT SUM(m1), DISTINCTCOUNT(m2) FROM testTable GROUP BY d1, d2, d3 TOP 100");
  private static final int NUM_GROUP_KEYS = 3;
  private static final int GROUP_BY_TOP_N = 100;
  private static final int NUM_GROUPS = 50000;
  private static final int MAX_SIZE_OF_SET = 50;

  private List<String> _groups;
  private AggregationGroupByTrimmingService _trimmingService;

  @BeforeClass
  public void setUp() {
    // Generate a list of random groups.
    Set<String> groupSet = new HashSet<>(NUM_GROUPS);
    while (groupSet.size() < NUM_GROUPS) {
      List<String> group = new ArrayList<>(NUM_GROUP_KEYS);
      for (int i = 0; i < NUM_GROUP_KEYS; i++) {
        // Randomly generate group key without GROUP_KEY_DELIMITER
        group.add(RandomStringUtils.random(RANDOM.nextInt(10)).replace(GroupKeyGenerator.DELIMITER, ' '));
      }
      groupSet.add(buildGroupString(group));
    }
    _groups = new ArrayList<>(groupSet);

    // Explicitly set an empty group
    StringBuilder emptyGroupBuilder = new StringBuilder();
    for (int i = 1; i < NUM_GROUP_KEYS; i++) {
      emptyGroupBuilder.append(GroupKeyGenerator.DELIMITER);
    }
    _groups.set(NUM_GROUPS - 1, emptyGroupBuilder.toString());

    _trimmingService = new AggregationGroupByTrimmingService(QUERY_CONTEXTS);
  }

  @Test
  public void testTrimming() {
    // Test Server side trimming
    Map<String, Object[]> intermediateResultsMap = new HashMap<>(NUM_GROUPS);
    for (int i = 0; i < NUM_GROUPS; i++) {
      IntOpenHashSet set = new IntOpenHashSet();
      for (int j = 0; j <= i; j += NUM_GROUPS / MAX_SIZE_OF_SET) {
        set.add(j);
      }
      intermediateResultsMap.put(_groups.get(i), new Object[]{(double) i, set});
    }
    List<Map<String, Object>> trimmedIntermediateResultMaps =
        _trimmingService.trimIntermediateResultsMap(intermediateResultsMap);
    Map<String, Object> trimmedSumResultMap = trimmedIntermediateResultMaps.get(0);
    Map<String, Object> trimmedDistinctCountResultMap = trimmedIntermediateResultMaps.get(1);
    int trimSize = trimmedSumResultMap.size();
    Assert.assertEquals(trimmedDistinctCountResultMap.size(), trimSize, ERROR_MESSAGE);
    for (int i = NUM_GROUPS - trimSize; i < NUM_GROUPS; i++) {
      String group = _groups.get(i);
      Assert.assertEquals(((Double) trimmedSumResultMap.get(group)).intValue(), i, ERROR_MESSAGE);
      Assert.assertEquals(((IntOpenHashSet) trimmedDistinctCountResultMap.get(group)).size(),
          i / (NUM_GROUPS / MAX_SIZE_OF_SET) + 1, ERROR_MESSAGE);
    }

    // Test Broker side trimming
    Map<String, Comparable> finalDistinctCountResultMap = new HashMap<>(trimSize);
    for (Map.Entry<String, Object> entry : trimmedDistinctCountResultMap.entrySet()) {
      finalDistinctCountResultMap.put(entry.getKey(), ((IntOpenHashSet) entry.getValue()).size());
    }
    List[] groupByResultLists =
        _trimmingService.trimFinalResults(new Map[]{trimmedSumResultMap, finalDistinctCountResultMap});
    List<GroupByResult> sumGroupByResultList = groupByResultLists[0];
    List<GroupByResult> distinctCountGroupByResultList = groupByResultLists[1];
    for (int i = 0; i < GROUP_BY_TOP_N; i++) {
      int expectedGroupIndex = NUM_GROUPS - 1 - i;
      GroupByResult sumGroupByResult = sumGroupByResultList.get(i);
      List<String> sumGroup = sumGroupByResult.getGroup();
      Assert.assertEquals(sumGroup.size(), NUM_GROUP_KEYS, ERROR_MESSAGE);
      Assert.assertEquals(buildGroupString(sumGroup), _groups.get(expectedGroupIndex), ERROR_MESSAGE);
      Double value = (Double) sumGroupByResult.getValue();
      Assert.assertEquals(value.intValue(), expectedGroupIndex, ERROR_MESSAGE);
      // For distinctCount, because multiple groups have same value, so there is no guarantee on the order of groups,
      // just check the value
      GroupByResult distinctCountGroupByResult = distinctCountGroupByResultList.get(i);
      Assert
          .assertEquals(distinctCountGroupByResult.getValue(), expectedGroupIndex / (NUM_GROUPS / MAX_SIZE_OF_SET) + 1,
              ERROR_MESSAGE);
    }
  }

  private static String buildGroupString(List<String> group) {
    StringBuilder groupStringBuilder = new StringBuilder();
    for (int i = 0; i < NUM_GROUP_KEYS; i++) {
      if (i != 0) {
        groupStringBuilder.append(GroupKeyGenerator.DELIMITER);
      }
      groupStringBuilder.append(group.get(i));
    }
    return groupStringBuilder.toString();
  }
}
