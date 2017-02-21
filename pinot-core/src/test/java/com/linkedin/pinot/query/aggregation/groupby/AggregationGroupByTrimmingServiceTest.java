/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.aggregation.groupby;

import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AggregationGroupByTrimmingServiceTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final AggregationFunction SUM = AggregationFunctionFactory.getAggregationFunction("SUM");
  private static final AggregationFunctionContext[] AGGREGATION_FUNCTION_CONTEXTS =
      {new AggregationFunctionContext(new String[]{"column"}, SUM)};
  private static final AggregationFunction[] AGGREGATION_FUNCTIONS = {SUM};
  private static final int NUM_GROUP_KEYS = 3;
  private static final int GROUP_BY_TOP_N = 100;
  private static final int NUM_GROUPS = 50000;

  private List<String> _groups;
  private AggregationGroupByTrimmingService _serverTrimmingService;
  private AggregationGroupByTrimmingService _brokerTrimmingService;

  @BeforeClass
  public void setUp() {
    // Generate a list of random groups.
    Set<String> groupSet = new HashSet<>(NUM_GROUPS);
    while (groupSet.size() < NUM_GROUPS) {
      String group = "";
      for (int i = 0; i < NUM_GROUP_KEYS; i++) {
        if (i != 0) {
          group += '\t';
        }
        // Random generate group key without '\t'.
        String groupKey = RandomStringUtils.random(RANDOM.nextInt(10));
        while (groupKey.contains("\t")) {
          groupKey = RandomStringUtils.random(RANDOM.nextInt(10));
        }
        group += groupKey;
      }
      groupSet.add(group);
    }
    _groups = new ArrayList<>(groupSet);

    // Explicitly set an empty group.
    String emptyGroup = "";
    for (int i = 1; i < NUM_GROUP_KEYS; i++) {
      emptyGroup += '\t';
    }
    _groups.set(NUM_GROUPS - 1, emptyGroup);

    _serverTrimmingService = new AggregationGroupByTrimmingService(AGGREGATION_FUNCTION_CONTEXTS, GROUP_BY_TOP_N);
    _brokerTrimmingService = new AggregationGroupByTrimmingService(AGGREGATION_FUNCTIONS, GROUP_BY_TOP_N);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTrimming() {
    // Test server side trimming.
    Map<String, Object[]> intermediateResultsMap = new HashMap<>(NUM_GROUPS);
    for (int i = 0; i < NUM_GROUPS; i++) {
      intermediateResultsMap.put(_groups.get(i), new Double[]{(double) i});
    }
    Map<String, Object> trimmedIntermediateResultsMap =
        _serverTrimmingService.trimIntermediateResultsMap(intermediateResultsMap).get(0);
    int trimSize = trimmedIntermediateResultsMap.size();
    for (int i = NUM_GROUPS - trimSize; i < NUM_GROUPS; i++) {
      Assert.assertEquals(trimmedIntermediateResultsMap.get(_groups.get(i)), (double) i, ERROR_MESSAGE);
    }

    // Test broker side trimming.
    List<GroupByResult> groupByResults =
        _brokerTrimmingService.trimFinalResults(new Map[]{trimmedIntermediateResultsMap})[0];
    for (int i = 0; i < GROUP_BY_TOP_N; i++) {
      int expectedGroupIndex = NUM_GROUPS - 1 - i;
      GroupByResult groupByResult = groupByResults.get(i);
      List<String> group = groupByResult.getGroup();
      Assert.assertEquals(group.size(), NUM_GROUP_KEYS, ERROR_MESSAGE);
      String groupString = "";
      for (int j = 0; j < NUM_GROUP_KEYS; j++) {
        if (j != 0) {
          groupString += '\t';
        }
        groupString += group.get(j);
      }
      Assert.assertEquals(groupString, _groups.get(expectedGroupIndex), ERROR_MESSAGE);
      Assert.assertEquals(Double.parseDouble((String) groupByResult.getValue()), (double) expectedGroupIndex,
          ERROR_MESSAGE);
    }
  }
}
