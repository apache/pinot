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
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import java.io.Serializable;
import java.util.List;
import org.testng.Assert;


public class QueriesTestUtils {
  private QueriesTestUtils() {
  }

  public static void verifyAggregationResult(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      String[] expectedAggregationResults) {
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
        Assert.assertEquals(value, expectedAggregationResult);
      } else {
        // Group-by.
        Assert.assertEquals(aggregationResult.getGroupByResult().get(0).getValue(), expectedAggregationResult);
      }
    }
  }
}
