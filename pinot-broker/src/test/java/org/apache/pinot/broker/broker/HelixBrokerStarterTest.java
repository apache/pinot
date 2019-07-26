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
package org.apache.pinot.broker.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.TimeBoundaryService;
import org.apache.pinot.broker.routing.TimeBoundaryService.TimeBoundaryInfo;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class HelixBrokerStarterTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";
  private static final int NUM_BROKERS = 3;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_OFFLINE_SEGMENTS = 5;

  private HelixBrokerStarter _brokerStarter;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();

    Configuration brokerConf = new BaseConfiguration();
    brokerConf.addProperty(Helix.KEY_OF_BROKER_QUERY_PORT, 18099);
    brokerConf.addProperty(Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL, 100L);
    _brokerStarter = new HelixBrokerStarter(brokerConf, getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR);
    _brokerStarter.start();

    ControllerRequestBuilderUtil
        .addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, NUM_BROKERS - 1,
            true);
    ControllerRequestBuilderUtil
        .addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, NUM_SERVERS, true);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addTime(TIME_COLUMN_NAME, TimeUnit.DAYS, FieldSpec.DataType.INT).build();
    _helixResourceManager.addOrUpdateSchema(schema);
    TableConfig offlineTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).build();
    _helixResourceManager.addTable(offlineTableConfig);
    TableConfig realtimeTimeConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).
            setStreamConfigs(getStreamConfigs()).build();
    _helixResourceManager.addTable(realtimeTimeConfig);

    for (int i = 0; i < NUM_OFFLINE_SEGMENTS; i++) {
      _helixResourceManager
          .addNewSegment(OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME),
              "downloadUrl");
    }

    TestUtils.waitForCondition(aVoid -> {
      ExternalView offlineTableExternalView =
          _helixAdmin.getResourceExternalView(getHelixClusterName(), OFFLINE_TABLE_NAME);
      return offlineTableExternalView != null
          && offlineTableExternalView.getPartitionSet().size() == NUM_OFFLINE_SEGMENTS;
    }, 30_000L, "Failed to find all OFFLINE segments in the ExternalView");
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "highLevel");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs
        .put("stream.kafka.decoder.class.name", "org.apache.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    return streamConfigs;
  }

  @Test
  public void testResourceAndTagAssignment()
      throws Exception {
    assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getBrokerTagForTenant(null))
            .size(), NUM_BROKERS);

    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(getHelixClusterName(), Helix.BROKER_RESOURCE_INSTANCE);
    assertEquals(brokerResourceIdealState.getInstanceSet(OFFLINE_TABLE_NAME).size(), NUM_BROKERS);
    assertEquals(brokerResourceIdealState.getInstanceSet(REALTIME_TABLE_NAME).size(), NUM_BROKERS);

    ExternalView brokerResourceExternalView =
        _helixAdmin.getResourceExternalView(getHelixClusterName(), Helix.BROKER_RESOURCE_INSTANCE);
    assertEquals(brokerResourceExternalView.getStateMap(OFFLINE_TABLE_NAME).size(), NUM_BROKERS);
    assertEquals(brokerResourceExternalView.getStateMap(REALTIME_TABLE_NAME).size(), NUM_BROKERS);

    HelixExternalViewBasedRouting routing = _brokerStarter.getHelixExternalViewBasedRouting();
    assertTrue(routing.routingTableExists(OFFLINE_TABLE_NAME));
    assertTrue(routing.routingTableExists(REALTIME_TABLE_NAME));

    RoutingTableLookupRequest routingTableLookupRequest = new RoutingTableLookupRequest(OFFLINE_TABLE_NAME);
    Map<String, List<String>> routingTable = routing.getRoutingTable(routingTableLookupRequest);
    assertEquals(routingTable.size(), NUM_SERVERS);
    assertEquals(routingTable.values().iterator().next().size(), NUM_OFFLINE_SEGMENTS);

    // Add a new segment into the OFFLINE table
    _helixResourceManager
        .addNewSegment(OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");

    TestUtils.waitForCondition(
        aVoid -> routing.getRoutingTable(routingTableLookupRequest).values().iterator().next().size()
            == NUM_OFFLINE_SEGMENTS + 1, 30_000L, "Failed to add the new segment into the routing table");

    // Add a new table with different broker tenant
    String newRawTableName = "newTable";
    String newOfflineTableName = TableNameBuilder.OFFLINE.tableNameWithType(newRawTableName);
    TableConfig newTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(newRawTableName).setBrokerTenant("testBroker").build();
    _helixResourceManager.addTable(newTableConfig);

    // Broker tenant should be overridden to DefaultTenant
    TableConfig newTableConfigInCluster = _helixResourceManager.getTableConfig(newOfflineTableName);
    assertNotNull(newTableConfigInCluster);
    assertEquals(newTableConfigInCluster.getTenantConfig().getBroker(), TagNameUtils.DEFAULT_TENANT_NAME);

    brokerResourceIdealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), Helix.BROKER_RESOURCE_INSTANCE);
    assertEquals(brokerResourceIdealState.getInstanceSet(newOfflineTableName).size(), NUM_BROKERS);

    TestUtils.waitForCondition(aVoid -> {
      Map<String, String> newTableStateMap =
          _helixAdmin.getResourceExternalView(getHelixClusterName(), Helix.BROKER_RESOURCE_INSTANCE)
              .getStateMap(newOfflineTableName);
      return newTableStateMap != null && newTableStateMap.size() == NUM_BROKERS;
    }, 30_000L, "Failed to find all brokers for the new table in the brokerResource ExternalView");

    assertTrue(routing.routingTableExists(newOfflineTableName));
  }

  /**
   * This test verifies that when the segments of an OFFLINE are refreshed, the TimeBoundaryInfo is also updated.
   */
  @Test
  public void testTimeBoundaryUpdate() {
    TimeBoundaryService timeBoundaryService = _brokerStarter.getHelixExternalViewBasedRouting().
        getTimeBoundaryService();

    // Time boundary should be 1 day smaller than the end time
    int currentEndTime = 10;
    TimeBoundaryInfo timeBoundaryInfo = timeBoundaryService.getTimeBoundaryInfoFor(OFFLINE_TABLE_NAME);
    assertEquals(timeBoundaryInfo.getTimeValue(), Integer.toString(currentEndTime - 1));

    // Refresh a segment with a new end time
    String segmentToRefresh = _helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME).get(0);
    int newEndTime = currentEndTime + 10;
    OfflineSegmentZKMetadata segmentZKMetadata =
        _helixResourceManager.getOfflineSegmentZKMetadata(RAW_TABLE_NAME, segmentToRefresh);
    _helixResourceManager.refreshSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadataWithEndTimeInfo(RAW_TABLE_NAME, segmentToRefresh, newEndTime),
        segmentZKMetadata);

    TestUtils.waitForCondition(aVoid -> timeBoundaryService.getTimeBoundaryInfoFor(OFFLINE_TABLE_NAME).getTimeValue()
        .equals(Integer.toString(newEndTime - 1)), 30_000L, "Failed to update the time boundary for refreshed segment");
  }

  @AfterClass
  public void tearDown() {
    _brokerStarter.shutdown();
    stopController();
    stopZk();
  }
}
