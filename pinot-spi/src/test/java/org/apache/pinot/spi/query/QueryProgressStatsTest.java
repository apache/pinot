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
package org.apache.pinot.spi.query;

import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class QueryProgressStatsTest {

  @Test
  public void testProgressPercent() {
    QueryProgressStats progressStats = new QueryProgressStats(3, 10);
    assertEquals(progressStats.getProcessedWorkUnits(), 3);
    assertEquals(progressStats.getTotalWorkUnits(), 10);
    assertEquals(progressStats.getProcessedSegments(), 3);
    assertEquals(progressStats.getTotalSegmentsToProcess(), 10);
    assertEquals(progressStats.getProgressPercent(), 30.0);

    assertEquals(new QueryProgressStats(0, 0).getProgressPercent(), 100.0);
    assertEquals(new QueryProgressStats(1, -1).getProgressPercent(), -1.0);
    assertEquals(new QueryProgressStats(12, 10).getProgressPercent(), 100.0);
  }

  @Test
  public void testAggregate() {
    QueryProgressStats progressStats = QueryProgressStats.aggregate(List.of(
        new QueryProgressStats(5, 20, 3, 10, true),
        new QueryProgressStats(15, 60, 7, 30, false)));

    assertEquals(progressStats.getProcessedWorkUnits(), 20);
    assertEquals(progressStats.getTotalWorkUnits(), 80);
    assertEquals(progressStats.getProcessedSegments(), 10);
    assertEquals(progressStats.getTotalSegmentsToProcess(), 40);
    assertEquals(progressStats.getProgressPercent(), 25.0);
    assertEquals(progressStats.isEstimated(), true);
  }

  @Test
  public void testAggregateWithUnknownTotal() {
    QueryProgressStats progressStats = QueryProgressStats.aggregate(List.of(
        new QueryProgressStats(3, 10),
        new QueryProgressStats(1, -1)));

    assertEquals(progressStats.getProcessedWorkUnits(), 4);
    assertEquals(progressStats.getTotalWorkUnits(), -1);
    assertEquals(progressStats.getProcessedSegments(), 4);
    assertEquals(progressStats.getTotalSegmentsToProcess(), -1);
    assertEquals(progressStats.getProgressPercent(), -1.0);
  }

  @Test
  public void testAggregateWithUnknownDetailKeepsUnknownTotal() {
    QueryProgressStats progressStats = QueryProgressStats.aggregate(List.of(
        new QueryProgressStats("Server A", 10, 10, 0, -1, true),
        QueryProgressStats.unknown("Server B", true))).withLabel("Servers");

    assertEquals(progressStats.getLabel(), "Servers");
    assertEquals(progressStats.getProcessedWorkUnits(), 10);
    assertEquals(progressStats.getTotalWorkUnits(), -1);
    assertEquals(progressStats.getProgressPercent(), -1.0);
    assertEquals(progressStats.isEstimated(), true);
  }

  @Test
  public void testJsonRoundTrip()
      throws Exception {
    QueryProgressStats progressStats = new QueryProgressStats(6, 12, 4, 8, true);
    QueryProgressStats deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(progressStats), QueryProgressStats.class);

    assertEquals(deserialized.getProcessedWorkUnits(), 6);
    assertEquals(deserialized.getTotalWorkUnits(), 12);
    assertEquals(deserialized.getProcessedSegments(), 4);
    assertEquals(deserialized.getTotalSegmentsToProcess(), 8);
    assertEquals(deserialized.getProgressPercent(), 50.0);
    assertEquals(deserialized.isEstimated(), true);
  }

  @Test
  public void testJsonRoundTripWithDetails()
      throws Exception {
    QueryProgressStats brokerProgressStats = new QueryProgressStats("Broker", 1, 2, 0, -1, true);
    QueryProgressStats serverProgressStats = new QueryProgressStats("Server a", 3, 6, 2, 4, true);
    QueryProgressStats progressStats = QueryProgressStats.aggregate(List.of(brokerProgressStats, serverProgressStats))
        .withLabel("Query").withDetails(List.of(brokerProgressStats, serverProgressStats));

    QueryProgressStats deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(progressStats), QueryProgressStats.class);

    assertEquals(deserialized.getLabel(), "Query");
    assertEquals(deserialized.getProcessedWorkUnits(), 4);
    assertEquals(deserialized.getTotalWorkUnits(), 8);
    assertEquals(deserialized.getDetails().size(), 2);
    assertEquals(deserialized.getDetails().get(0).getLabel(), "Broker");
    assertEquals(deserialized.getDetails().get(0).getProcessedWorkUnits(), 1);
    assertEquals(deserialized.getDetails().get(1).getLabel(), "Server a");
    assertEquals(deserialized.getDetails().get(1).getTotalSegmentsToProcess(), 4);
  }

  @Test
  public void testJsonRoundTripWithSegmentOnlyPayload()
      throws Exception {
    QueryProgressStats deserialized = JsonUtils.stringToObject(
        "{\"processedSegments\":4,\"totalSegmentsToProcess\":8}", QueryProgressStats.class);

    assertEquals(deserialized.getProcessedWorkUnits(), 4);
    assertEquals(deserialized.getTotalWorkUnits(), 8);
    assertEquals(deserialized.getProcessedSegments(), 4);
    assertEquals(deserialized.getTotalSegmentsToProcess(), 8);
    assertEquals(deserialized.getProgressPercent(), 50.0);
    assertEquals(deserialized.isEstimated(), false);
  }

  @Test
  public void testExecutionContextAccumulatesSegmentTotals() {
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    assertEquals(executionContext.getProgressStats().getTotalSegmentsToProcess(), -1);

    executionContext.addTotalSegmentsToProcess(2);
    executionContext.incrementProcessedSegments();
    executionContext.addTotalSegmentsToProcess(3);

    QueryProgressStats progressStats = executionContext.getProgressStats();
    assertEquals(progressStats.getProcessedWorkUnits(), 1);
    assertEquals(progressStats.getTotalWorkUnits(), 5);
    assertEquals(progressStats.getProcessedSegments(), 1);
    assertEquals(progressStats.getTotalSegmentsToProcess(), 5);
    assertEquals(progressStats.getProgressPercent(), 20.0);
  }

  @Test
  public void testExecutionContextAccumulatesWorkUnits() {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseTest();
    assertEquals(executionContext.getProgressStats().getTotalWorkUnits(), -1);

    executionContext.addTotalWorkUnits(2);
    executionContext.incrementProcessedWorkUnits();
    executionContext.addTotalWorkUnits(3);

    QueryProgressStats progressStats = executionContext.getProgressStats();
    assertEquals(progressStats.getProcessedWorkUnits(), 1);
    assertEquals(progressStats.getTotalWorkUnits(), 5);
    assertEquals(progressStats.getProcessedSegments(), 0);
    assertEquals(progressStats.getTotalSegmentsToProcess(), -1);
    assertEquals(progressStats.getProgressPercent(), 20.0);
    assertEquals(progressStats.isEstimated(), true);
  }
}
