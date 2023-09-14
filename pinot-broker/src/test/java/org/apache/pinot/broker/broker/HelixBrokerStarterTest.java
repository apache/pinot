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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
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
  private static final int EXPECTED_VERSION = -1;

  private HelixBrokerStarter _brokerStarter;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();

    Map<String, Object> properties = new HashMap<>();
    properties.put(Helix.KEY_OF_BROKER_QUERY_PORT, 18099);
    properties.put(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    properties.put(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, true);
    properties.put(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);

    _brokerStarter = new HelixBrokerStarter();
    _brokerStarter.init(new PinotConfiguration(properties));
    _brokerStarter.start();

    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKERS - 1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVERS, true);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.INT, "EPOCH|DAYS", "1:DAYS").build();
    _helixResourceManager.addSchema(schema, true, false);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).build();
    _helixResourceManager.addTable(offlineTableConfig);
    TableConfig realtimeTimeConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).setStreamConfigs(getStreamConfigs()).setNumReplicas(1)
            .build();
    _helixResourceManager.addTable(realtimeTimeConfig);

    for (int i = 0; i < NUM_OFFLINE_SEGMENTS; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");
    }

    TestUtils.waitForCondition(aVoid -> {
      // should wait for both realtime and offline table external view to be live.
      ExternalView offlineTableExternalView =
          _helixAdmin.getResourceExternalView(getHelixClusterName(), OFFLINE_TABLE_NAME);
      ExternalView realtimeTableExternalView =
          _helixAdmin.getResourceExternalView(getHelixClusterName(), REALTIME_TABLE_NAME);
      return offlineTableExternalView != null
          && offlineTableExternalView.getPartitionSet().size() == NUM_OFFLINE_SEGMENTS
          && realtimeTableExternalView != null;
    }, 30_000L, "Failed to find all OFFLINE segments in the ExternalView");
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "lowlevel");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder");
    streamConfigs.put("stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.broker.broker.FakeStreamConsumerFactory");
    return streamConfigs;
  }

  @Test
  public void testClusterConfigOverride() {
    PinotConfiguration config = _brokerStarter.getConfig();
    assertTrue(config.getProperty(Helix.ENABLE_CASE_INSENSITIVE_KEY, false));
    assertEquals(config.getProperty(Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, 0), 12);

    // NOTE: It is disabled in cluster config, but enabled in instance config. Instance config should take precedence.
    assertTrue(config.getProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, false));
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

    BrokerRoutingManager routingManager = _brokerStarter.getRoutingManager();
    assertTrue(routingManager.routingExists(OFFLINE_TABLE_NAME));
    assertTrue(routingManager.routingExists(REALTIME_TABLE_NAME));

    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + OFFLINE_TABLE_NAME);
    RoutingTable routingTable = routingManager.getRoutingTable(brokerRequest, 0);
    assertNotNull(routingTable);
    assertEquals(routingTable.getServerInstanceToSegmentsMap().size(), NUM_SERVERS);
    assertEquals(routingTable.getServerInstanceToSegmentsMap().values().iterator().next().size(), NUM_OFFLINE_SEGMENTS);
    assertTrue(routingTable.getUnavailableSegments().isEmpty());

    // Add a new segment into the OFFLINE table
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");

    TestUtils.waitForCondition(aVoid ->
        routingManager.getRoutingTable(brokerRequest, 0).getServerInstanceToSegmentsMap()
            .values().iterator().next().size() == NUM_OFFLINE_SEGMENTS + 1, 30_000L, "Failed to add the new segment "
        + "into the routing table");

    // Add a new table with different broker tenant
    String newRawTableName = "newTable";
    String newOfflineTableName = TableNameBuilder.OFFLINE.tableNameWithType(newRawTableName);
    TableConfig newTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(newRawTableName).setBrokerTenant("testBroker").build();
    try {
      _helixResourceManager.addTable(newTableConfig);
      Assert.fail("Table creation should fail as testBroker does not exist");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Add a new table with same broker tenant
    addDummySchema(newRawTableName);
    newTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(newRawTableName)
        .setServerTenant(TagNameUtils.DEFAULT_TENANT_NAME).build();
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

    assertTrue(routingManager.routingExists(newOfflineTableName));
  }

  /**
   * This test verifies that when the segments of an OFFLINE are refreshed, the TimeBoundaryInfo is also updated.
   */
  @Test
  public void testTimeBoundaryUpdate() {
    BrokerRoutingManager routingManager = _brokerStarter.getRoutingManager();

    // Time boundary should be 1 day smaller than the end time
    int currentEndTime = 10;
    TimeBoundaryInfo timeBoundaryInfo = routingManager.getTimeBoundaryInfo(OFFLINE_TABLE_NAME);
    assertNotNull(timeBoundaryInfo);
    assertEquals(timeBoundaryInfo.getTimeValue(), Integer.toString(currentEndTime - 1));

    // Refresh a segment with a new end time
    String segmentToRefresh = _helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true).get(0);
    int newEndTime = currentEndTime + 10;
    SegmentZKMetadata segmentZKMetadata =
        _helixResourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, segmentToRefresh);
    _helixResourceManager.refreshSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadataWithEndTimeInfo(RAW_TABLE_NAME, segmentToRefresh, newEndTime),
        segmentZKMetadata, EXPECTED_VERSION, "downloadUrl");

    TestUtils.waitForCondition(aVoid -> routingManager.getTimeBoundaryInfo(OFFLINE_TABLE_NAME).getTimeValue()
        .equals(Integer.toString(newEndTime - 1)), 30_000L, "Failed to update the time boundary for refreshed segment");
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    _brokerStarter.stop();
    stopController();
    stopZk();
  }
}
