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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class QueryOptionsUtilsTest {
  private static final List<String> POSITIVE_INT_KEYS =
      List.of(NUM_REPLICA_GROUPS_TO_QUERY, MAX_EXECUTION_THREADS, NUM_GROUPS_LIMIT, MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          MAX_STREAMING_PENDING_BLOCKS, MAX_ROWS_IN_JOIN, MAX_ROWS_IN_WINDOW);
  private static final List<String> NON_NEGATIVE_INT_KEYS = List.of(MULTI_STAGE_LEAF_LIMIT);
  private static final List<String> UNBOUNDED_INT_KEYS =
      List.of(MIN_SEGMENT_GROUP_TRIM_SIZE, MIN_SERVER_GROUP_TRIM_SIZE, MIN_BROKER_GROUP_TRIM_SIZE,
          GROUP_TRIM_THRESHOLD);
  private static final List<String> INT_KEYS = new ArrayList<>() {{
    addAll(POSITIVE_INT_KEYS);
    addAll(NON_NEGATIVE_INT_KEYS);
    addAll(UNBOUNDED_INT_KEYS);
  }};
  private static final List<String> POSITIVE_LONG_KEYS =
      List.of(TIMEOUT_MS, MAX_SERVER_RESPONSE_SIZE_BYTES, MAX_QUERY_RESPONSE_SIZE_BYTES);

  @Test
  public void shouldConvertCaseInsensitiveMapToUseCorrectValues() {
    // Given:
    Map<String, String> configs = Map.of("ENABLENullHandling", "true", "useMULTISTAGEEngine", "false");

    // When:
    Map<String, String> resolved = QueryOptionsUtils.resolveCaseInsensitiveOptions(configs);

    // Then:
    assertEquals(resolved.get(ENABLE_NULL_HANDLING), "true");
    assertEquals(resolved.get(USE_MULTISTAGE_ENGINE), "false");
  }

  @Test
  public void testSkipIndexesParsing() {
    String skipIndexesStr = "col1=inverted,range&col2=sorted";
    Map<String, String> queryOptions = Map.of(SKIP_INDEXES, skipIndexesStr);
    Map<String, Set<FieldConfig.IndexType>> skipIndexes = QueryOptionsUtils.getSkipIndexes(queryOptions);
    assertEquals(skipIndexes.get("col1"), Set.of(FieldConfig.IndexType.RANGE, FieldConfig.IndexType.INVERTED));
    assertEquals(skipIndexes.get("col2"), Set.of(FieldConfig.IndexType.SORTED));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testSkipIndexesParsingInvalid() {
    String skipIndexesStr = "col1=inverted,range&col2";
    Map<String, String> queryOptions = Map.of(SKIP_INDEXES, skipIndexesStr);
    QueryOptionsUtils.getSkipIndexes(queryOptions);
  }

  @Test
  public void testIntegerSettingParseSuccess() {
    HashMap<String, String> map = new HashMap<>();

    for (String key : INT_KEYS) {
      for (Integer value : new Integer[]{null, 1, 10, Integer.MAX_VALUE}) {
        map.put(key, value != null ? String.valueOf(value) : null);
        assertEquals(getValue(map, key), value);
      }
    }

    for (String key : POSITIVE_LONG_KEYS) {
      for (Long value : new Long[]{null, 1L, 10L, Long.MAX_VALUE}) {
        map.put(key, value != null ? String.valueOf(value) : null);
        assertEquals(getValue(map, key), value);
      }
    }
  }

  @Test
  public void testIntegerSettingParseErrors() {
    for (String key : POSITIVE_INT_KEYS) {
      for (String value : new String[]{"-10000000000", "-2147483648", "-1", "0", "2147483648", "10000000000"}) {
        try {
          getValue(Map.of(key, value), key);
          fail(key);
        } catch (IllegalArgumentException ise) {
          assertEquals(ise.getMessage(), key + " must be a number between 1 and 2^31-1, got: " + value);
        }
      }
    }

    for (String key : NON_NEGATIVE_INT_KEYS) {
      for (String value : new String[]{"-10000000000", "-2147483648", "-1", "2147483648", "10000000000"}) {
        try {
          getValue(Map.of(key, value), key);
          fail();
        } catch (IllegalArgumentException ise) {
          assertEquals(ise.getMessage(), key + " must be a number between 0 and 2^31-1, got: " + value);
        }
      }
    }

    for (String key : UNBOUNDED_INT_KEYS) {
      for (String value : new String[]{"-10000000000", "2147483648", "10000000000"}) {
        try {
          getValue(Map.of(key, value), key);
          fail();
        } catch (IllegalArgumentException ise) {
          assertEquals(ise.getMessage(), key + " must be an integer, got: " + value);
        }
      }
    }

    for (String key : POSITIVE_LONG_KEYS) {
      for (String value : new String[]{
          "-100000000000000000000", "-9223372036854775809", "-1", "0", "9223372036854775808", "100000000000000000000"
      }) {
        try {
          getValue(Map.of(key, value), key);
          fail();
        } catch (IllegalArgumentException ise) {
          assertEquals(ise.getMessage(), key + " must be a number between 1 and 2^63-1, got: " + value);
        }
      }
    }
  }

  private static Object getValue(Map<String, String> map, String key) {
    switch (key) {
      // Positive ints
      case NUM_REPLICA_GROUPS_TO_QUERY:
        return QueryOptionsUtils.getNumReplicaGroupsToQuery(map);
      case MAX_EXECUTION_THREADS:
        return QueryOptionsUtils.getMaxExecutionThreads(map);
      case NUM_GROUPS_LIMIT:
        return QueryOptionsUtils.getNumGroupsLimit(map);
      case MAX_INITIAL_RESULT_HOLDER_CAPACITY:
        return QueryOptionsUtils.getMaxInitialResultHolderCapacity(map);
      case MAX_STREAMING_PENDING_BLOCKS:
        return QueryOptionsUtils.getMaxStreamingPendingBlocks(map);
      case MAX_ROWS_IN_JOIN:
        return QueryOptionsUtils.getMaxRowsInJoin(map);
      case MAX_ROWS_IN_WINDOW:
        return QueryOptionsUtils.getMaxRowsInWindow(map);
      // Non-negative ints
      case MULTI_STAGE_LEAF_LIMIT:
        return QueryOptionsUtils.getMultiStageLeafLimit(map);
      // Unbounded ints
      case MIN_SEGMENT_GROUP_TRIM_SIZE:
        return QueryOptionsUtils.getMinSegmentGroupTrimSize(map);
      case MIN_SERVER_GROUP_TRIM_SIZE:
        return QueryOptionsUtils.getMinServerGroupTrimSize(map);
      case MIN_BROKER_GROUP_TRIM_SIZE:
        return QueryOptionsUtils.getMinBrokerGroupTrimSize(map);
      case GROUP_TRIM_THRESHOLD:
        return QueryOptionsUtils.getGroupTrimThreshold(map);
      // Positive longs
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
