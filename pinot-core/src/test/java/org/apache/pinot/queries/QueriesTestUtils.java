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
package org.apache.pinot.queries;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.customobject.AvgPair;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class QueriesTestUtils {
  private QueriesTestUtils() {
  }

  public static void testInnerSegmentExecutionStatistics(ExecutionStatistics executionStatistics,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalDocs) {
    assertEquals(executionStatistics.getNumDocsScanned(), expectedNumDocsScanned);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    assertEquals(executionStatistics.getNumTotalDocs(), expectedNumTotalDocs);
  }

  public static void testInnerSegmentAggregationResult(List<Object> aggregationResult, long... expectedResults) {
    assertEquals(((Number) aggregationResult.get(0)).longValue(), expectedResults[0]);
    assertEquals(((Number) aggregationResult.get(1)).longValue(), expectedResults[1]);
    assertEquals(((Number) aggregationResult.get(2)).longValue(), expectedResults[2]);
    assertEquals(((Number) aggregationResult.get(3)).longValue(), expectedResults[3]);
    AvgPair avgResult = (AvgPair) aggregationResult.get(4);
    assertEquals((long) avgResult.getSum(), expectedResults[4]);
    assertEquals(avgResult.getCount(), expectedResults[5]);
  }

  public static void testInnerSegmentAggregationGroupByResult(AggregationGroupByResult aggregationGroupByResult,
      Object[] expectedGroupKeys, long... expectedResults) {
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      if (Arrays.equals(groupKey._keys, expectedGroupKeys)) {
        int groupId = groupKey._groupId;
        assertEquals(((Number) aggregationGroupByResult.getResultForGroupId(0, groupId)).longValue(),
            expectedResults[0]);
        assertEquals(((Number) aggregationGroupByResult.getResultForGroupId(1, groupId)).longValue(),
            expectedResults[1]);
        assertEquals(((Number) aggregationGroupByResult.getResultForGroupId(2, groupId)).longValue(),
            expectedResults[2]);
        assertEquals(((Number) aggregationGroupByResult.getResultForGroupId(3, groupId)).longValue(),
            expectedResults[3]);
        AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForGroupId(4, groupId);
        assertEquals((long) avgResult.getSum(), expectedResults[4]);
        assertEquals(avgResult.getCount(), expectedResults[5]);
        return;
      }
    }
    fail("Failed to find group key: " + Arrays.toString(expectedGroupKeys));
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, Object[] expectedResults) {
    testInterSegmentsResult(brokerResponse, Collections.singletonList(expectedResults));
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, List<Object[]> expectedRows) {
    validateRows(brokerResponse.getResultTable().getRows(), expectedRows);
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, ResultTable expectedResultTable) {
    validateResultTable(brokerResponse.getResultTable(), expectedResultTable);
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      Object[] expectedResults) {
    testInterSegmentsResult(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, Collections.singletonList(expectedResults));
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      List<Object[]> expectedRows) {
    validateExecutionStatistics(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs);
    validateRows(brokerResponse.getResultTable().getRows(), expectedRows);
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      ResultTable expectedResultTable) {
    validateExecutionStatistics(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs);
    validateResultTable(brokerResponse.getResultTable(), expectedResultTable);
  }

  private static void validateExecutionStatistics(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs) {
    assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    assertEquals(brokerResponse.getTotalDocs(), expectedNumTotalDocs);
  }

  private static void validateResultTable(ResultTable actual, ResultTable expected) {
    assertEquals(actual.getDataSchema(), expected.getDataSchema());
    validateRows(actual.getRows(), expected.getRows());
  }

  private static void validateRows(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); i++) {
      // Generic assertEquals delegates to assertArrayEquals, which can test for equality of array values in rows.
      assertEquals((Object) actual.get(i), (Object) expected.get(i));
    }
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      ResultTable expectedResultTable, Function<Object, Object> responseMapper) {
    validateExecutionStatistics(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs);
    ResultTable resultTable = brokerResponse.getResultTable();
    // NOTE: Do not check data schema with response mapper
    validateRows(resultTable.getRows().stream().map(row -> Arrays.stream(row).map(responseMapper).toArray())
        .collect(Collectors.toList()), expectedResultTable.getRows());
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      ResultTable expectedResultTable, double delta) {
    validateExecutionStatistics(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs);
    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getDataSchema(), expectedResultTable.getDataSchema());
    List<Object[]> rows = resultTable.getRows();
    List<Object[]> expectedRows = expectedResultTable.getRows();
    assertEquals(rows.size(), expectedRows.size());
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      assertEquals(row.length, expectedRow.length);
      for (int j = 0; j < row.length; j++) {
        if (row[i] instanceof Number) {
          assertEquals(((Number) row[i]).doubleValue(), ((Number) expectedRow[i]).doubleValue(), delta);
        } else {
          assertEquals(row[i], expectedRow[i]);
        }
      }
    }
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      ResultTable expectedResultTable, Function<Object, Object> responseMapper, double delta) {
    validateExecutionStatistics(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs);
    ResultTable resultTable = brokerResponse.getResultTable();
    // NOTE: Do not check data schema with response mapper
    List<Object[]> rows = resultTable.getRows();
    List<Object[]> expectedRows = expectedResultTable.getRows();
    assertEquals(rows.size(), expectedRows.size());
    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      assertEquals(row.length, expectedRow.length);
      for (int j = 0; j < row.length; j++) {
        Object value = responseMapper.apply(row[i]);
        if (value instanceof Number) {
          assertEquals(((Number) value).doubleValue(), ((Number) expectedRow[i]).doubleValue(), delta);
        } else {
          assertEquals(value, expectedRow[i]);
        }
      }
    }
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse,
      BrokerResponseNative referenceBrokerResponse, Function<Object, Object> responseMapper) {
    testInterSegmentsResult(brokerResponse, referenceBrokerResponse.getNumDocsScanned(),
        referenceBrokerResponse.getNumEntriesScannedInFilter(),
        referenceBrokerResponse.getNumEntriesScannedPostFilter(), referenceBrokerResponse.getTotalDocs(),
        referenceBrokerResponse.getResultTable(), responseMapper);
  }

  public static void testInterSegmentsResult(BrokerResponseNative brokerResponse,
      BrokerResponseNative referenceBrokerResponse, Function<Object, Object> responseMapper, double delta) {
    testInterSegmentsResult(brokerResponse, referenceBrokerResponse.getNumDocsScanned(),
        referenceBrokerResponse.getNumEntriesScannedInFilter(),
        referenceBrokerResponse.getNumEntriesScannedPostFilter(), referenceBrokerResponse.getTotalDocs(),
        referenceBrokerResponse.getResultTable(), responseMapper, delta);
  }
}
