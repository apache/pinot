/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.broker;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.broker.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.controller.utils.SegmentMetadataMockUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.MetricsRegistry;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class HelixBrokerStarterTest {
  private static final int SEGMENT_COUNT = 6;
  private PinotHelixResourceManager _pinotResourceManager;
  private static final String HELIX_CLUSTER_NAME = "TestHelixBrokerStarter";
  private static final String RAW_DINING_TABLE_NAME = "dining";
  private static final String DINING_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_DINING_TABLE_NAME);
  private static final String COFFEE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType("coffee");
  private final Configuration _pinotHelixBrokerProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();

  private ZkClient _zkClient;
  private HelixAdmin _helixAdmin;
  private HelixBrokerStarter _helixBrokerStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  @BeforeTest
  public void setUp() throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    final String instanceId = "localhost_helixController";
    _pinotResourceManager =
        new PinotHelixResourceManager(ZkStarter.DEFAULT_ZK_STR, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true, /*isUpdateStateModel=*/false);
    _pinotResourceManager.start();
    _helixAdmin = _pinotResourceManager.getHelixAdmin();

    _pinotHelixBrokerProperties.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, 8943);
    _pinotHelixBrokerProperties.addProperty(
            CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL, 100L);
    _helixBrokerStarter =
        new HelixBrokerStarter(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR, _pinotHelixBrokerProperties);

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 5, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 1, true);

    while (_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size() == 0 ||
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size() == 0) {
      Thread.sleep(100);
    }

    TableConfig offlineTableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
                .setTableName(RAW_DINING_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS").build();
    _pinotResourceManager.addTable(offlineTableConfig);
    setupRealtimeTable();

    for (int i = 0; i < 5; i++) {
      _pinotResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(RAW_DINING_TABLE_NAME),
          "downloadUrl");
    }

    Thread.sleep(1000);

    ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, DINING_TABLE_NAME);
    Assert.assertEquals(externalView.getPartitionSet().size(), 5);
  }

  private void setupRealtimeTable() throws IOException {
    // Set up the realtime table.
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "highLevel");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs.put("stream.kafka.decoder.class.name",
            "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    streamConfigs.put("stream.kafka.hlc.zk.connect.string", "localhost:1111/zkConnect");
    streamConfigs.put("stream.kafka.decoder.prop.schema.registry.rest.url", "http://localhost:2222/schemaRegistry");
    TableConfig realtimeTimeConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME)
            .setTableName(RAW_DINING_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS").
                    setStreamConfigs(streamConfigs).build();
    Schema schema = new Schema();
    schema.setSchemaName(RAW_DINING_TABLE_NAME);
    _pinotResourceManager.addOrUpdateSchema(schema);
    // Fake an PinotLLCRealtimeSegmentManager instance: required for a realtime table creation.
    PinotLLCRealtimeSegmentManager.create(_pinotResourceManager, new ControllerConf(),
            new ControllerMetrics(new MetricsRegistry()));
    _pinotResourceManager.addTable(realtimeTimeConfig);
    _helixBrokerStarter.getHelixExternalViewBasedRouting().markDataResourceOnline(realtimeTimeConfig, null,
            new ArrayList<InstanceConfig>());
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    IdealState idealState;

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 6);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    ExternalView externalView =
        _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    HelixExternalViewBasedRouting helixExternalViewBasedRouting = _helixBrokerStarter.getHelixExternalViewBasedRouting();
    Field brokerRoutingTableBuilderMapField;
    brokerRoutingTableBuilderMapField = HelixExternalViewBasedRouting.class.getDeclaredField("_routingTableBuilderMap");
    brokerRoutingTableBuilderMapField.setAccessible(true);

    final Map<String, RoutingTableBuilder> brokerRoutingTableBuilderMap =
        (Map<String, RoutingTableBuilder>)brokerRoutingTableBuilderMapField.get(helixExternalViewBasedRouting);

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerRoutingTableBuilderMap.size() == 1;
      }
    }, 30000L);

    Assert.assertEquals(Arrays.toString(brokerRoutingTableBuilderMap.keySet().toArray()), "[dining_OFFLINE, dining_REALTIME]");

    final String tableName = "coffee";
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
        .setBrokerTenant("testBroker")
        .setServerTenant("testServer")
        .build();
    _pinotResourceManager.addTable(tableConfig);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 6);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(COFFEE_TABLE_NAME).size(), SEGMENT_COUNT);
    Assert.assertEquals(idealState.getInstanceSet(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    // Wait up to 30s for broker external view to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getStateMap(COFFEE_TABLE_NAME).size() == SEGMENT_COUNT;
      }
    }, 30000L);

    externalView =
        _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(COFFEE_TABLE_NAME).size(), SEGMENT_COUNT);

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerRoutingTableBuilderMap.size() == 2;
      }
    }, 30000L);

    Object[] tableArray = brokerRoutingTableBuilderMap.keySet().toArray();
    Arrays.sort(tableArray);
    Assert.assertEquals(Arrays.toString(tableArray), "[coffee_OFFLINE, dining_OFFLINE, dining_REALTIME]");

    Assert.assertEquals(
        brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0).values().iterator().next().size(),
        5);

    _pinotResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(RAW_DINING_TABLE_NAME),
        "downloadUrl");

    // Wait up to 30s for external view to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, DINING_TABLE_NAME).getPartitionSet().size() ==
            SEGMENT_COUNT;
      }
    }, 30000L);

    externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, DINING_TABLE_NAME);
    Assert.assertEquals(externalView.getPartitionSet().size(), SEGMENT_COUNT);
    tableArray = brokerRoutingTableBuilderMap.keySet().toArray();
    Arrays.sort(tableArray);
    Assert.assertEquals(Arrays.toString(tableArray), "[coffee_OFFLINE, dining_OFFLINE, dining_REALTIME]");

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Map<String, List<String>> routingTable =
            brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0);
        return routingTable.values().iterator().next().size() == SEGMENT_COUNT;
      }
    }, 30000L);

    Assert.assertEquals(brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0)
        .values().iterator().next().size(), SEGMENT_COUNT);
  }

  @Test
  public void testTimeBoundaryUpdate() throws Exception {
    // This test verifies that when the segments of an offline table are refreshed, the TimeBoundaryInfo is also updated
    // to a newer timestamp.
    final long currentTimeBoundary = 10;
    TimeBoundaryService.TimeBoundaryInfo tbi =
            _helixBrokerStarter.getHelixExternalViewBasedRouting().
                    getTimeBoundaryService().getTimeBoundaryInfoFor(DINING_TABLE_NAME);

    Assert.assertEquals(tbi.getTimeValue(), Long.toString(currentTimeBoundary));

    List<String> segmentNames = _pinotResourceManager.getSegmentsFor(DINING_TABLE_NAME);
    long endTime = currentTimeBoundary + 10;
    // Refresh all 5 segments.
    for (String segment : segmentNames) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
              _pinotResourceManager.getOfflineSegmentZKMetadata(RAW_DINING_TABLE_NAME, segment);
      Assert.assertNotNull(offlineSegmentZKMetadata);
      _pinotResourceManager.refreshSegment(
              SegmentMetadataMockUtils.mockSegmentMetadataWithEndTimeInfo(RAW_DINING_TABLE_NAME, segment, endTime ++),
              offlineSegmentZKMetadata);
    }
    // Due to the asynchronous nature of the TimeboundaryInfo update and thread scheduling, the updated time boundary
    // may not always be the max endtime of segments. We do not expect such exact update either as long as the timestamp
    // is updated to some newer value.
    waitForPredicate(() -> {
      TimeBoundaryService.TimeBoundaryInfo timeBoundaryInfo = _helixBrokerStarter.getHelixExternalViewBasedRouting().
              getTimeBoundaryService().getTimeBoundaryInfoFor(DINING_TABLE_NAME);
      return currentTimeBoundary < Long.parseLong(timeBoundaryInfo.getTimeValue());
    }, 5 * _pinotHelixBrokerProperties.getLong(
            CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL));
    tbi = _helixBrokerStarter.getHelixExternalViewBasedRouting().
            getTimeBoundaryService().getTimeBoundaryInfoFor(DINING_TABLE_NAME);
    Assert.assertTrue(currentTimeBoundary < Long.parseLong(tbi.getTimeValue()));
  }

  private void waitForPredicate(Callable<Boolean> predicate, long timeout) {
    long deadline = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (predicate.call()) {
          return;
        }
      } catch (Exception e) {
        // Do nothing
      }

      Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }
  }
}
