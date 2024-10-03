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

package org.apache.pinot.common.utils.config;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.*;


public class QueryOptionsUtilsTest {

  @Test
  public void shouldConvertCaseInsensitiveMapToUseCorrectValues() {
    // Given:
    Map<String, String> configs = ImmutableMap.of(
        "ENABLENullHandling", "true",
        "useMULTISTAGEEngine", "false"
    );

    // When:
    Map<String, String> resolved = QueryOptionsUtils.resolveCaseInsensitiveOptions(configs);

    // Then:
    Assert.assertEquals(resolved.get(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING), "true");
    Assert.assertEquals(resolved.get(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE), "false");
  }

  @Test
  public void testSkipIndexesParsing() {
    String skipIndexesStr = "col1=inverted,range&col2=sorted";
    Map<String, String> queryOptions =
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.SKIP_INDEXES, skipIndexesStr);
    Map<String, Set<FieldConfig.IndexType>> skipIndexes = QueryOptionsUtils.getSkipIndexes(queryOptions);
    Assert.assertEquals(skipIndexes.get("col1"),
        Set.of(FieldConfig.IndexType.RANGE, FieldConfig.IndexType.INVERTED));
    Assert.assertEquals(skipIndexes.get("col2"),
        Set.of(FieldConfig.IndexType.SORTED));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testSkipIndexesParsingInvalid() {
    String skipIndexesStr = "col1=inverted,range&col2";
    Map<String, String> queryOptions =
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.SKIP_INDEXES, skipIndexesStr);
     QueryOptionsUtils.getSkipIndexes(queryOptions);
  }

  @Test
  public void testIntegerSettingParseSuccess() {
    HashMap<String, String> map = new HashMap<>();

    for (String setting : Arrays.asList(NUM_GROUPS_LIMIT, MAX_INITIAL_RESULT_HOLDER_CAPACITY, MULTI_STAGE_LEAF_LIMIT,
        GROUP_TRIM_THRESHOLD, MAX_STREAMING_PENDING_BLOCKS, MAX_ROWS_IN_JOIN, MAX_STREAMING_PENDING_BLOCKS,
        MAX_EXECUTION_THREADS, MIN_SEGMENT_GROUP_TRIM_SIZE, MIN_SERVER_GROUP_TRIM_SIZE)) {
      map.clear();
      for (Integer val : new Integer[]{null, 1, 10, Integer.MAX_VALUE}) {
        map.put(setting, val != null ? String.valueOf(val) : null);
        Assert.assertEquals(getValue(map, setting), val);
      }
    }

    for (String setting : Arrays.asList(TIMEOUT_MS, MAX_SERVER_RESPONSE_SIZE_BYTES, MAX_QUERY_RESPONSE_SIZE_BYTES)) {
      map.clear();
      for (Long val : new Long[]{null, 1L, 10L, Long.MAX_VALUE}) {
        map.put(setting, val != null ? String.valueOf(val) : null);
        Assert.assertEquals(getValue(map, setting), val);
      }
    }
  }

  @Test
  public void testIntegerSettingParseErrors() {
    HashMap<String, String> map = new HashMap<>();

    for (String setting : Arrays.asList(NUM_GROUPS_LIMIT, MAX_INITIAL_RESULT_HOLDER_CAPACITY, MULTI_STAGE_LEAF_LIMIT,
        GROUP_TRIM_THRESHOLD, MAX_STREAMING_PENDING_BLOCKS, MAX_ROWS_IN_JOIN, MAX_STREAMING_PENDING_BLOCKS,
        MAX_EXECUTION_THREADS, MIN_SEGMENT_GROUP_TRIM_SIZE, MIN_SERVER_GROUP_TRIM_SIZE)) {
      for (String val : new String[]{"-10000000000", "-2147483648", "-1", "2147483648", "10000000000"}) {
        map.clear();
        map.put(setting, val);
        try {
          getValue(map, setting);
          Assert.fail();
        } catch (IllegalArgumentException ise) {
          Assert.assertEquals(ise.getMessage(), setting + " must be a number between 0 and 2^31-1, got: " + val);
        }
      }
    }

    for (String setting : Arrays.asList(TIMEOUT_MS, MAX_SERVER_RESPONSE_SIZE_BYTES, MAX_QUERY_RESPONSE_SIZE_BYTES)) {
      for (String val : new String[]{
          "-100000000000000000000", "-9223372036854775809", "-1", "0", "9223372036854775808", "100000000000000000000"
      }) {
        map.clear();
        map.put(setting, val);
        try {
          getValue(map, setting);
          Assert.fail();
        } catch (IllegalArgumentException ise) {
          Assert.assertEquals(ise.getMessage(), setting + " must be a number between 1 and 2^63-1, got: " + val);
        }
      }
    }
  }

  private static Object getValue(Map<String, String> map, String key) {
    switch (key) {
      //ints
      case NUM_GROUPS_LIMIT:
        return QueryOptionsUtils.getNumGroupsLimit(map);
      case MAX_INITIAL_RESULT_HOLDER_CAPACITY:
        return QueryOptionsUtils.getMaxInitialResultHolderCapacity(map);
      case MULTI_STAGE_LEAF_LIMIT:
        return QueryOptionsUtils.getMultiStageLeafLimit(map);
      case GROUP_TRIM_THRESHOLD:
        return QueryOptionsUtils.getGroupTrimThreshold(map);
      case MAX_STREAMING_PENDING_BLOCKS:
        return QueryOptionsUtils.getMaxStreamingPendingBlocks(map);
      case MAX_ROWS_IN_JOIN:
        return QueryOptionsUtils.getMaxRowsInJoin(map);
      case MAX_EXECUTION_THREADS:
        return QueryOptionsUtils.getMaxExecutionThreads(map);
      case MIN_SEGMENT_GROUP_TRIM_SIZE:
        return QueryOptionsUtils.getMinSegmentGroupTrimSize(map);
      case MIN_SERVER_GROUP_TRIM_SIZE:
        return QueryOptionsUtils.getMinServerGroupTrimSize(map);
      //longs
      case TIMEOUT_MS:
        return QueryOptionsUtils.getTimeoutMs(map);
      case MAX_SERVER_RESPONSE_SIZE_BYTES:
        return QueryOptionsUtils.getMaxServerResponseSizeBytes(map);
      case MAX_QUERY_RESPONSE_SIZE_BYTES:
        return QueryOptionsUtils.getMaxQueryResponseSizeBytes(map);
      default:
        throw new IllegalArgumentException("Unexpected key!");
    }
  }
}
