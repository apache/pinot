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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.testng.Assert;


public class QueriesTestUtils {
  private QueriesTestUtils() {
  }

  public static void testInnerSegmentExecutionStatistics(ExecutionStatistics executionStatistics,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalRawDocs) {
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), expectedNumEntriesScannedInFilter);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), expectedNumEntriesScannedPostFilter);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), expectedNumTotalRawDocs);
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
    testInterSegmentAggregationResult(
        brokerResponse,
        expectedNumDocsScanned,
        expectedNumEntriesScannedInFilter,
        expectedNumEntriesScannedPostFilter,
        expectedNumTotalDocs,
        Serializable::toString,
        expectedAggregationResults);
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
        Assert.assertEquals(responseMapper.apply(aggregationResult.getGroupByResult().get(0).getValue()), expectedAggregationResult);
      }
    }
  }
}
