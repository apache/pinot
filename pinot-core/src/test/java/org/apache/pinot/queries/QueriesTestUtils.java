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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.testng.Assert;


public class QueriesTestUtils {
  private QueriesTestUtils() {
  }

  public static void testInnerSegmentExecutionStatistics(ExecutionStatistics executionStatistics,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalDocs) {
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), expectedNumTotalDocs);
  }

  public static void testInnerSegmentAggregationResult(List<Object> aggregationResult, long expectedCountResult,
      long expectedSumResult, int expectedMaxResult, int expectedMinResult, long expectedAvgResultSum,
      long expectedAvgResultCount) {
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), expectedCountResult);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), expectedSumResult);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), expectedMaxResult);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), expectedMinResult);
    AvgPair avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), expectedAvgResultSum);
    Assert.assertEquals(avgResult.getCount(), expectedAvgResultCount);
  }

  public static void testInnerSegmentAggregationGroupByResult(AggregationGroupByResult aggregationGroupByResult,
      String expectedGroupKey, long expectedCountResult, long expectedSumResult, int expectedMaxResult,
      int expectedMinResult, long expectedAvgResultSum, long expectedAvgResultCount) {
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      if (groupKey._stringKey.equals(expectedGroupKey)) {
        Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(groupKey, 0)).longValue(),
            expectedCountResult);
        Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(groupKey, 1)).longValue(),
            expectedSumResult);
        Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(groupKey, 2)).intValue(),
            expectedMaxResult);
        Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(groupKey, 3)).intValue(),
            expectedMinResult);
        AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(groupKey, 4);
        Assert.assertEquals((long) avgResult.getSum(), expectedAvgResultSum);
        Assert.assertEquals(avgResult.getCount(), expectedAvgResultCount);
        return;
      }
    }
    Assert.fail("Failed to find group key: " + expectedGroupKey);
  }

  public static void testInterSegmentAggregationResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      String[] expectedAggregationResults) {
    testInterSegmentAggregationResult(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, Serializable::toString, expectedAggregationResults);
  }

  public static void testInterSegmentAggregationResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      Function<Serializable, String> responseMapper, String[] expectedAggregationResults) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedNumTotalDocs);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int length = expectedAggregationResults.length;
    Assert.assertEquals(aggregationResults.size(), length);
    for (int i = 0; i < length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      String expectedAggregationResult = expectedAggregationResults[i];
      Serializable value = aggregationResult.getValue();
      if (value != null) {
        // Aggregation.
        Assert.assertEquals(responseMapper.apply(value), expectedAggregationResult);
      } else {
        // Group-by.
        Assert.assertEquals(responseMapper.apply(aggregationResult.getGroupByResult().get(0).getValue()),
            expectedAggregationResult);
      }
    }
  }

  public static void testInterSegmentAggregationGroupByResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      List<String[]>expectedGroupKeys, List<String[]> expectedAggregationResults) {
    testInterSegmentAggregationGroupByResult(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, Serializable::toString, expectedGroupKeys, expectedAggregationResults);
  }

  private static void testInterSegmentAggregationGroupByResult(BrokerResponseNative brokerResponse,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalDocs, Function<Serializable, String> responseMapper, List<String[]> expectedGroupKeys,
      List<String[]> expectedAggregationResults) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedNumTotalDocs);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, expectedAggregationResults.get(0).length);
    int numKeyColumns = expectedGroupKeys.get(0).length;
    for (int i = 0; i < numAggregationColumns; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      int numGroups = groupByResults.size();
      for (int j = 0; j < numGroups; j++) {
        GroupByResult groupByResult = groupByResults.get(j);
        Assert.assertEquals(responseMapper.apply(groupByResult.getValue()), expectedAggregationResults.get(j)[i]);
        List<String> groupValues = groupByResult.getGroup();
        Assert.assertEquals(groupValues.size(), numKeyColumns);
        for (int k = 0; k < numKeyColumns; k++) {
          Assert.assertEquals(expectedGroupKeys.get(j)[k], groupValues.get(k));
        }
      }
    }
  }

  static void testInterSegmentResultTable(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      List<Object[]> expectedResults, int expectedResultsSize, DataSchema expectedDataSchema) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedNumTotalDocs);

    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema actualDataSchema = resultTable.getDataSchema();
    List<Object[]> actualResults = resultTable.getRows();

    Assert.assertEquals(actualResults.size(), expectedResultsSize);
    Assert.assertEquals(actualDataSchema.size(), expectedDataSchema.size());
    Assert.assertEquals(actualDataSchema.getColumnNames(), expectedDataSchema.getColumnNames());
    Assert.assertEquals(actualDataSchema.getColumnDataTypes(), expectedDataSchema.getColumnDataTypes());

    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertEquals(Arrays.asList(actualResults.get(i)), Arrays.asList(expectedResults.get(i)));
    }
  }

  static void testInterSegmentGroupByOrderByResultPQL(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      List<String[]> expectedGroups, List<List<Serializable>> expectedValues, boolean preserveType) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedNumTotalDocs);

    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    if (aggregationResults == null) {
      Assert.assertEquals(expectedGroups.size(), 0);
      Assert.assertEquals(expectedValues.size(), 0);
    } else {
      for (int i = 0; i < aggregationResults.size(); i++) {
        List<GroupByResult> groupByResults = aggregationResults.get(i).getGroupByResult();
        List<Serializable> expectedSerializables = expectedValues.get(i);
        Assert.assertEquals(groupByResults.size(), expectedGroups.size());
        Assert.assertEquals(groupByResults.size(), expectedSerializables.size());
        for (int j = 0; j < groupByResults.size(); j++) {
          GroupByResult groupByResult = groupByResults.get(j);
          List<String> group = groupByResult.getGroup();
          Assert.assertEquals(group, Arrays.asList(expectedGroups.get(j)));
          Serializable expectedValue = expectedSerializables.get(j);
          if (!preserveType) {
            expectedValue = AggregationFunctionUtils.formatValue(expectedValue);
          }
          Assert.assertEquals(groupByResult.getValue(), expectedValue);
        }
      }
    }
  }
}
