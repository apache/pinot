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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.RebalanceSummaryResult;
import org.apache.pinot.integration.tests.SharedKafkaRealtimeIntegrationTestSuite.ScenarioLease;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Final, topology-mutating scenario in the shared Kafka realtime suite.
public class KafkaConsumingSegmentToBeMovedSummaryIntegrationTest {
  private static final String TABLE_NAME = "mytableConsumingSegmentSummary";
  private static final String TOPIC_NAME = "KafkaConsumingSegmentToBeMovedSummaryIntegrationTest";

  @Test
  public void testConsumingSegmentSummary()
      throws Throwable {
    SharedKafkaRealtimeIntegrationTestSuite owner =
        SharedKafkaRealtimeIntegrationTestSuite.getSharedSuiteOwner();
    ScenarioLease lease = owner.newScenario(TABLE_NAME, TOPIC_NAME);
    Throwable primaryFailure = null;
    try {
      owner.createScenarioTopic(lease);
      List<File> avroFiles = owner.unpackScenarioData(lease);
      owner.addScenarioSchema(lease);

      Map<String, String> streamConfigs = owner.getScenarioStreamConfigs(lease._topicName, false);
      streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, "1000000");
      streamConfigs.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
      TableConfig tableConfig = owner.createScenarioTableConfig(lease, avroFiles.get(0), streamConfigs);
      owner.addScenarioTable(lease, tableConfig);

      ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles, owner.getKafkaBrokerList(), lease._topicName,
          owner.getMaxNumKafkaMessagesPerBatch(), owner.getKafkaMessageHeader(), owner.getPartitionColumn(), false);
      owner.waitForScenarioCount(lease, owner.getDefaultScenarioCount(), 600_000L);

      assertEmptyConsumingMoveSummary(rebalance(owner, lease._tableName, true));

      owner.startAdditionalServer();
      assertSingleConsumingMoveSummary(rebalance(owner, lease._tableName, true));

      assertEmptyConsumingMoveSummary(rebalance(owner, lease._tableName, false));

      // Kafka loss is part of the contract under test. This scenario is last so stopping the suite-owned broker does
      // not invalidate any following scenario.
      owner.stopSharedKafkaForFinalScenario();
      RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary summary =
          getConsumingMoveSummary(rebalance(owner, lease._tableName, true));
      Assert.assertEquals(summary.getNumConsumingSegmentsToBeMoved(), 1);
      Assert.assertEquals(summary.getNumServersGettingConsumingSegmentsAdded(), 1);
      Assert.assertNotNull(summary.getServerConsumingSegmentSummary());
      Assert.assertNull(summary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp());
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      owner.closeScenario(lease, primaryFailure, null);
    }
  }

  private static RebalanceResult rebalance(SharedKafkaRealtimeIntegrationTestSuite owner, String tableName,
      boolean includeConsuming)
      throws Exception {
    RebalanceResult result = owner.getOrCreateAdminClient().getRebalanceClient()
        .rebalanceTable(tableName, "REALTIME", true, false, includeConsuming, false, -1);
    Assert.assertNotNull(result);
    return result;
  }

  private static RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary getConsumingMoveSummary(
      RebalanceResult result) {
    Assert.assertNotNull(result.getRebalanceSummaryResult());
    Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
    RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary summary =
        result.getRebalanceSummaryResult().getSegmentInfo().getConsumingSegmentToBeMovedSummary();
    Assert.assertNotNull(summary);
    return summary;
  }

  private static void assertEmptyConsumingMoveSummary(RebalanceResult result) {
    RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary summary = getConsumingMoveSummary(result);
    Assert.assertEquals(summary.getNumConsumingSegmentsToBeMoved(), 0);
    Assert.assertEquals(summary.getNumServersGettingConsumingSegmentsAdded(), 0);
    Assert.assertEquals(summary.getServerConsumingSegmentSummary().size(), 0);
  }

  private static void assertSingleConsumingMoveSummary(RebalanceResult result) {
    RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary summary = getConsumingMoveSummary(result);
    Assert.assertEquals(summary.getNumConsumingSegmentsToBeMoved(), 1);
    Assert.assertEquals(summary.getNumServersGettingConsumingSegmentsAdded(), 1);
    Assert.assertEquals(summary.getServerConsumingSegmentSummary().size(), 1);
    Assert.assertTrue(summary.getServerConsumingSegmentSummary().values().stream()
        .allMatch(value -> value.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 57_801L
            || value.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 0L));
    Assert.assertEquals(summary.getServerConsumingSegmentSummary().values().stream()
        .reduce(0L, (total, value) -> total + value.getTotalOffsetsToCatchUpAcrossAllConsumingSegments(), Long::sum),
        57_801L);
  }
}
