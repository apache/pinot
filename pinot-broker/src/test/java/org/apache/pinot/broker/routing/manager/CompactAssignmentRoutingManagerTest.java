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
package org.apache.pinot.broker.routing.manager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.broker.broker.helix.CompactAssignmentZkHelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Checks that a [BrokerRoutingManager] on a [CompactAssignmentZkHelixManager] routes exactly like one on the default
/// Helix manager. The routing manager wraps the compact records without copying them (see
/// [HelixHelper#toIdealState]), so every routing component (instance selector, segment pruners, time
/// boundary manager, segment partition metadata manager) reads the immutable outer map of the parsed record.
///
/// The test writes a hybrid table into a local ZooKeeper, builds the routing in both managers, compares what they
/// return, then changes the assignment and compares again after the assignment change.
public class CompactAssignmentRoutingManagerTest {
  private static final String CLUSTER_NAME = "CompactAssignmentRoutingManagerTest";
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String TIME_COLUMN = "ts";
  private static final String PARTITION_COLUMN = "memberId";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SERVERS = 4;
  private static final long BASE_TIME_MS = 1_700_000_000_000L;
  private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);
  private static final List<String> QUERIES = List.of("SELECT * FROM myTable",
      "SELECT * FROM myTable WHERE memberId = 2",
      "SELECT * FROM myTable WHERE ts > " + (BASE_TIME_MS + 10 * HOUR_MS),
      "SELECT * FROM myTable WHERE memberId IN (1, 3) AND ts < " + (BASE_TIME_MS + 6 * HOUR_MS));

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ZkClient _zkClient;
  private HelixManager _defaultHelixManager;
  private HelixManager _compactHelixManager;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private BrokerRoutingManager _defaultRoutingManager;
  private BrokerRoutingManager _compactRoutingManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String zkUrl = _zookeeperInstance.getZkUrl();
    HelixAdmin helixAdmin = new ZKHelixAdmin.Builder().setZkAddress(zkUrl).build();
    try {
      helixAdmin.addCluster(CLUSTER_NAME);
      for (int i = 0; i < NUM_SERVERS; i++) {
        InstanceConfig instanceConfig = new InstanceConfig(server(i));
        instanceConfig.setHostName("host" + i);
        instanceConfig.setPort(Integer.toString(8000 + i));
        helixAdmin.addInstance(CLUSTER_NAME, instanceConfig);
      }
    } finally {
      helixAdmin.close();
    }
    _zkClient = new ZkClient.Builder().setZkServer(zkUrl).setZkSerializer(new ZNRecordSerializer()).build();

    _defaultHelixManager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Broker_default_8099", InstanceType.SPECTATOR, zkUrl);
    _defaultHelixManager.connect();
    _compactHelixManager =
        new CompactAssignmentZkHelixManager(CLUSTER_NAME, "Broker_compact_8099", InstanceType.SPECTATOR, zkUrl);
    _compactHelixManager.connect();
    _propertyStore = _defaultHelixManager.getHelixPropertyStore();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(PARTITION_COLUMN, FieldSpec.DataType.INT)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    ZKMetadataProvider.setSchema(_propertyStore, schema);
    ZKMetadataProvider.setTableConfig(_propertyStore, tableConfig(TableType.OFFLINE));
    ZKMetadataProvider.setTableConfig(_propertyStore, tableConfig(TableType.REALTIME));

    // Offline: 20 segments on 2 replicas each; one replica in ERROR and one segment missing from the external view
    Map<String, Map<String, String>> offlineIdealState = new TreeMap<>();
    Map<String, Map<String, String>> offlineExternalView = new TreeMap<>();
    for (int i = 0; i < 20; i++) {
      String segment = offlineSegment(i);
      setSegmentZkMetadata(OFFLINE_TABLE_NAME, segment, i % NUM_PARTITIONS, BASE_TIME_MS + i * HOUR_MS, false);
      Map<String, String> instanceStateMap = replicas(i, SegmentStateModel.ONLINE);
      offlineIdealState.put(segment, instanceStateMap);
      if (i == 7) {
        continue;
      }
      Map<String, String> externalViewStateMap = new TreeMap<>(instanceStateMap);
      if (i == 5) {
        externalViewStateMap.put(server(i % NUM_SERVERS), SegmentStateModel.ERROR);
      }
      offlineExternalView.put(segment, externalViewStateMap);
    }
    writeAssignment(OFFLINE_TABLE_NAME, offlineIdealState, offlineExternalView, true);

    // Realtime: 3 sequences per partition, the last one CONSUMING
    Map<String, Map<String, String>> realtimeAssignment = new TreeMap<>();
    for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
      for (int sequence = 0; sequence < 3; sequence++) {
        String segment = RAW_TABLE_NAME + "__" + partition + "__" + sequence + "__20240101T0000Z";
        boolean consuming = sequence == 2;
        setSegmentZkMetadata(REALTIME_TABLE_NAME, segment, partition,
            BASE_TIME_MS + (15 + sequence) * HOUR_MS, consuming);
        realtimeAssignment.put(segment,
            replicas(partition, consuming ? SegmentStateModel.CONSUMING : SegmentStateModel.ONLINE));
      }
    }
    writeAssignment(REALTIME_TABLE_NAME, realtimeAssignment, realtimeAssignment, true);

    _defaultRoutingManager = newRoutingManager(_defaultHelixManager);
    _compactRoutingManager = newRoutingManager(_compactHelixManager);
  }

  @AfterClass
  public void tearDown() {
    if (_defaultRoutingManager != null) {
      _defaultRoutingManager.stop();
    }
    if (_compactRoutingManager != null) {
      _compactRoutingManager.stop();
    }
    if (_defaultHelixManager != null) {
      _defaultHelixManager.disconnect();
    }
    if (_compactHelixManager != null) {
      _compactHelixManager.disconnect();
    }
    if (_zkClient != null) {
      _zkClient.close();
    }
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  private static String server(int i) {
    return "Server_host" + i + "_" + (8000 + i);
  }

  private static String offlineSegment(int i) {
    return OFFLINE_TABLE_NAME + "_" + i;
  }

  private static Map<String, String> replicas(int i, String state) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    instanceStateMap.put(server(i % NUM_SERVERS), state);
    instanceStateMap.put(server((i + 1) % NUM_SERVERS), state);
    return instanceStateMap;
  }

  private static TableConfig tableConfig(TableType tableType) {
    return new TableConfigBuilder(tableType).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(PARTITION_COLUMN, new ColumnPartitionConfig("Modulo", NUM_PARTITIONS))))
        .setRoutingConfig(new RoutingConfig(null,
            List.of(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE, RoutingConfig.TIME_SEGMENT_PRUNER_TYPE), null, false))
        .build();
  }

  private void setSegmentZkMetadata(String tableNameWithType, String segment, int partition, long endTimeMs,
      boolean consuming) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    // Old segments, so that the instance selectors do not treat them as new
    segmentZKMetadata.setCreationTime(BASE_TIME_MS);
    segmentZKMetadata.setPushTime(BASE_TIME_MS);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Map.of(PARTITION_COLUMN,
        new ColumnPartitionMetadata("Modulo", NUM_PARTITIONS, Set.of(partition), null))));
    if (consuming) {
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    } else {
      segmentZKMetadata.setStartTime(endTimeMs - HOUR_MS);
      segmentZKMetadata.setEndTime(endTimeMs);
      segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
      segmentZKMetadata.setTotalDocs(1000);
    }
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  private void writeAssignment(String tableNameWithType, Map<String, Map<String, String>> idealState,
      Map<String, Map<String, String>> externalView, boolean create) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    ZNRecord idealStateRecord = new ZNRecord(tableNameWithType);
    idealStateRecord.setSimpleField("IDEAL_STATE_MODE", "CUSTOMIZED");
    idealStateRecord.setSimpleField("REBALANCE_MODE", "CUSTOMIZED");
    idealStateRecord.setSimpleField("STATE_MODEL_DEF_REF", "SegmentOnlineOfflineStateModel");
    idealStateRecord.setSimpleField("NUM_PARTITIONS", Integer.toString(idealState.size()));
    idealStateRecord.setSimpleField("HELIX_ENABLED", "true");
    idealStateRecord.setMapFields(idealState);
    ZNRecord externalViewRecord = new ZNRecord(tableNameWithType);
    externalViewRecord.setMapFields(externalView);
    // Compressed, because the compact serializer only parses gzip-compressed znodes itself
    idealStateRecord.setBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, true);
    externalViewRecord.setBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, true);
    String idealStatePath = keyBuilder.idealStates(tableNameWithType).getPath();
    String externalViewPath = keyBuilder.externalView(tableNameWithType).getPath();
    if (create) {
      _zkClient.createPersistent(idealStatePath, idealStateRecord);
      _zkClient.createPersistent(externalViewPath, externalViewRecord);
    } else {
      _zkClient.writeData(idealStatePath, idealStateRecord);
      _zkClient.writeData(externalViewPath, externalViewRecord);
    }
  }

  private BrokerRoutingManager newRoutingManager(HelixManager helixManager) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER, true);
    BrokerRoutingManager routingManager =
        new BrokerRoutingManager(mock(BrokerMetrics.class), mock(ServerRoutingStatsManager.class), config);
    routingManager.init(helixManager);
    routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
    routingManager.buildRouting(OFFLINE_TABLE_NAME);
    routingManager.buildRouting(REALTIME_TABLE_NAME);
    return routingManager;
  }

  @Test
  public void testSameRoutingAsDefaultReader() {
    assertReadsAreCompact();
    Map<String, Object> expected = snapshot(_defaultRoutingManager);
    assertEquals(snapshot(_compactRoutingManager), expected);

    // Sanity checks so that the comparison is not vacuous
    assertEquals(expected.get(OFFLINE_TABLE_NAME + ".timeBoundary"),
        TIME_COLUMN + "=" + (BASE_TIME_MS + 19 * HOUR_MS - TimeUnit.DAYS.toMillis(1)));
    assertEquals(expected.get(OFFLINE_TABLE_NAME + ".disabled"), false);
    assertNotNull(expected.get(OFFLINE_TABLE_NAME + ".partitions"));
    assertNotNull(expected.get(REALTIME_TABLE_NAME + ".replicatedServers"));
    @SuppressWarnings("unchecked")
    Map<String, Object> filtered = (Map<String, Object>) expected.get(OFFLINE_TABLE_NAME + "." + QUERIES.get(1) + ".0");
    assertEquals(filtered.get("numPruned"), 15);
    @SuppressWarnings("unchecked")
    Map<String, Object> all = (Map<String, Object>) expected.get(OFFLINE_TABLE_NAME + "." + QUERIES.get(0) + ".0");
    assertEquals(all.get("unavailable"), List.of(offlineSegment(7)));

    // Change the offline assignment: add a segment, recover the ERROR replica, drop a segment
    Map<String, Map<String, String>> idealState = new TreeMap<>();
    Map<String, Map<String, String>> externalView = new TreeMap<>();
    for (int i = 1; i < 21; i++) {
      String segment = offlineSegment(i);
      if (i == 20) {
        setSegmentZkMetadata(OFFLINE_TABLE_NAME, segment, i % NUM_PARTITIONS, BASE_TIME_MS + i * HOUR_MS, false);
      }
      idealState.put(segment, replicas(i, SegmentStateModel.ONLINE));
      externalView.put(segment, replicas(i, SegmentStateModel.ONLINE));
    }
    writeAssignment(OFFLINE_TABLE_NAME, idealState, externalView, false);
    _defaultRoutingManager.processClusterChange(ChangeType.IDEAL_STATE);
    _defaultRoutingManager.processClusterChange(ChangeType.EXTERNAL_VIEW);
    _compactRoutingManager.processClusterChange(ChangeType.IDEAL_STATE);
    _compactRoutingManager.processClusterChange(ChangeType.EXTERNAL_VIEW);

    assertReadsAreCompact();
    Map<String, Object> expectedAfterChange = snapshot(_defaultRoutingManager);
    assertEquals(snapshot(_compactRoutingManager), expectedAfterChange);
    assertEquals(expectedAfterChange.get(OFFLINE_TABLE_NAME + ".timeBoundary"),
        TIME_COLUMN + "=" + (BASE_TIME_MS + 20 * HOUR_MS - TimeUnit.DAYS.toMillis(1)));
    @SuppressWarnings("unchecked")
    Map<String, Object> allAfterChange =
        (Map<String, Object>) expectedAfterChange.get(OFFLINE_TABLE_NAME + "." + QUERIES.get(0) + ".0");
    assertEquals(allAfterChange.get("unavailable"), List.of());
  }

  /// The compact manager reads compact records and the routing manager wraps them without a copy; the default
  /// manager reads regular records.
  private void assertReadsAreCompact() {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    for (String table : List.of(OFFLINE_TABLE_NAME, REALTIME_TABLE_NAME)) {
      for (String path : List.of(keyBuilder.idealStates(table).getPath(), keyBuilder.externalView(table).getPath())) {
        ZNRecord compact =
            _compactHelixManager.getHelixDataAccessor().getBaseDataAccessor().get(path, null, AccessOption.PERSISTENT);
        assertTrue(HelixHelper.isCompact(compact), path);
        ZNRecord standard =
            _defaultHelixManager.getHelixDataAccessor().getBaseDataAccessor().get(path, null, AccessOption.PERSISTENT);
        assertFalse(HelixHelper.isCompact(standard), path);
      }
    }
  }

  /// Returns what the routing manager exposes for both tables, in a form that compares by value.
  private static Map<String, Object> snapshot(BrokerRoutingManager routingManager) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    for (String table : List.of(OFFLINE_TABLE_NAME, REALTIME_TABLE_NAME)) {
      snapshot.put(table + ".exists", routingManager.routingExists(table));
      snapshot.put(table + ".disabled", routingManager.isTableDisabled(table));
      snapshot.put(table + ".servingInstances", new TreeSet<>(routingManager.getServingInstances(table)));
      TablePartitionInfo partitionInfo = routingManager.getTablePartitionInfo(table);
      if (partitionInfo != null) {
        List<List<String>> segmentsByPartition = new ArrayList<>();
        for (List<String> segments : partitionInfo.getSegmentsByPartition()) {
          segmentsByPartition.add(sorted(segments));
        }
        snapshot.put(table + ".partitions", partitionInfo.getPartitionColumn() + "/"
            + partitionInfo.getNumPartitions() + "/" + segmentsByPartition);
      }
      TablePartitionReplicatedServersInfo replicatedServersInfo =
          routingManager.getTablePartitionReplicatedServersInfo(table);
      if (replicatedServersInfo != null) {
        List<String> partitions = new ArrayList<>();
        for (TablePartitionReplicatedServersInfo.PartitionInfo info : replicatedServersInfo.getPartitionInfoMap()) {
          partitions.add(info == null ? "null"
              : new TreeSet<>(info._fullyReplicatedServers) + ":" + sorted(info._segments));
        }
        snapshot.put(table + ".replicatedServers",
            partitions + "/" + sorted(replicatedServersInfo.getSegmentsWithInvalidPartition()));
      }
      for (String query : QUERIES) {
        BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
        for (int requestId = 0; requestId < 4; requestId++) {
          snapshot.put(table + "." + query + "." + requestId,
              routing(routingManager.getRoutingTable(brokerRequest, table, requestId)));
        }
      }
    }
    TimeBoundaryInfo timeBoundaryInfo = routingManager.getTimeBoundaryInfo(OFFLINE_TABLE_NAME);
    snapshot.put(OFFLINE_TABLE_NAME + ".timeBoundary",
        timeBoundaryInfo == null ? null : timeBoundaryInfo.getTimeColumn() + "=" + timeBoundaryInfo.getTimeValue());
    return snapshot;
  }

  private static Map<String, Object> routing(RoutingTable routingTable) {
    Map<String, Object> routing = new LinkedHashMap<>();
    Map<String, String> servers = new TreeMap<>();
    for (Map.Entry<ServerInstance, SegmentsToQuery> entry : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
      servers.put(entry.getKey().getInstanceId(),
          sorted(entry.getValue().getSegments()) + "+" + sorted(entry.getValue().getOptionalSegments()));
    }
    routing.put("servers", servers);
    routing.put("unavailable", sorted(routingTable.getUnavailableSegments()));
    routing.put("numPruned", routingTable.getNumPrunedSegments());
    return routing;
  }

  private static List<String> sorted(List<String> list) {
    List<String> sorted = new ArrayList<>(list);
    sorted.sort(null);
    return sorted;
  }
}
