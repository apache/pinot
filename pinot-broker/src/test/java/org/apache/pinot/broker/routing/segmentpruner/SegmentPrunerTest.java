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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentPrunerTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String QUERY_1 = "SELECT * FROM testTable";
  private static final String QUERY_2 = "SELECT * FROM testTable where memberId = 0";
  private static final String QUERY_3 = "SELECT * FROM testTable where memberId IN (1, 2)";

  private ZkStarter.ZookeeperInstance _zkInstance;
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    _zkInstance = ZkStarter.startLocalZkServer();
    _zkClient =
        new ZkClient(ZkStarter.DEFAULT_ZK_STR, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/SegmentPrunerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zkInstance);
  }

  @Test
  public void testSegmentPrunerFactory() {
    TableConfig tableConfig = mock(TableConfig.class);
    IndexingConfig indexingConfig = mock(IndexingConfig.class);
    when(tableConfig.getIndexingConfig()).thenReturn(indexingConfig);

    // Routing config is missing
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());

    // Segment pruner type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());

    // Segment partition config is missing
    when(routingConfig.getSegmentPrunerTypes())
        .thenReturn(Collections.singletonList(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());

    // Column partition config is missing
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    when(indexingConfig.getSegmentPartitionConfig()).thenReturn(new SegmentPartitionConfig(columnPartitionConfigMap));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());

    // Partition-aware segment pruner should be returned
    columnPartitionConfigMap.put(PARTITION_COLUMN, new ColumnPartitionConfig("Modulo", 5));
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);

    // Do not allow multiple partition columns
    columnPartitionConfigMap.put("anotherPartitionColumn", new ColumnPartitionConfig("Modulo", 5));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());

    // Should be backward-compatible with legacy config
    columnPartitionConfigMap.remove("anotherPartitionColumn");
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(null);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptyList());
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(SegmentPrunerFactory.LEGACY_PARTITION_AWARE_OFFLINE_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(SegmentPrunerFactory.LEGACY_PARTITION_AWARE_REALTIME_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);
  }

  @Test
  public void testPartitionAwareSegmentPruner() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest1 = compiler.compileToBrokerRequest(QUERY_1);
    BrokerRequest brokerRequest2 = compiler.compileToBrokerRequest(QUERY_2);
    BrokerRequest brokerRequest3 = compiler.compileToBrokerRequest(QUERY_3);
    // NOTE: External view and ideal state are not used in the current implementation.
    ExternalView externalView = Mockito.mock(ExternalView.class);
    IdealState idealState = Mockito.mock(IdealState.class);

    PartitionSegmentPruner segmentPruner =
        new PartitionSegmentPruner(OFFLINE_TABLE_NAME, PARTITION_COLUMN, _propertyStore);
    Set<String> onlineSegments = new HashSet<>();
    segmentPruner.init(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Collections.emptyList()), Collections.emptyList());
    assertEquals(segmentPruner.prune(brokerRequest2, Collections.emptyList()), Collections.emptyList());
    assertEquals(segmentPruner.prune(brokerRequest3, Collections.emptyList()), Collections.emptyList());

    // Segments without metadata (not updated yet) should not be pruned
    String newSegment = "newSegment";
    assertEquals(segmentPruner.prune(brokerRequest1, Collections.singletonList(newSegment)),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest2, Collections.singletonList(newSegment)),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest3, Collections.singletonList(newSegment)),
        Collections.singletonList(newSegment));

    // Segments without partition metadata should not be pruned
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    onlineSegments.add(segmentWithoutPartitionMetadata);
    OfflineSegmentZKMetadata segmentZKMetadataWithoutPartitionMetadata = new OfflineSegmentZKMetadata();
    segmentZKMetadataWithoutPartitionMetadata.setSegmentName(segmentWithoutPartitionMetadata);
    ZKMetadataProvider
        .setOfflineSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadataWithoutPartitionMetadata);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Collections.singletonList(segmentWithoutPartitionMetadata)),
        Collections.singletonList(segmentWithoutPartitionMetadata));
    assertEquals(segmentPruner.prune(brokerRequest2, Collections.singletonList(segmentWithoutPartitionMetadata)),
        Collections.singletonList(segmentWithoutPartitionMetadata));
    assertEquals(segmentPruner.prune(brokerRequest3, Collections.singletonList(segmentWithoutPartitionMetadata)),
        Collections.singletonList(segmentWithoutPartitionMetadata));

    // Test different partition functions and number of partitions
    // 0 % 5 = 0; 1 % 5 = 1; 2 % 5 = 2
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    setSegmentZKMetadata(segment0, "Modulo", 5, 0);
    // Murmur(0) % 4 = 0; Murmur(1) % 4 = 3; Murmur(2) % 4 = 0
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    setSegmentZKMetadata(segment1, "Murmur", 4, 0);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest2, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest3, Arrays.asList(segment0, segment1)),
        Collections.singletonList(segment1));

    // Update partition metadata without refreshing should have no effect
    setSegmentZKMetadata(segment0, "Modulo", 4, 1);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest2, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest3, Arrays.asList(segment0, segment1)),
        Collections.singletonList(segment1));

    // Refresh the changed segment should update the segment pruner
    segmentPruner.refreshSegment(segment0);
    assertEquals(segmentPruner.prune(brokerRequest1, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest2, Arrays.asList(segment0, segment1)),
        Collections.singletonList(segment1));
    assertEquals(segmentPruner.prune(brokerRequest3, Arrays.asList(segment0, segment1)),
        Arrays.asList(segment0, segment1));
  }

  private void setSegmentZKMetadata(String segment, String partitionFunction, int numPartitions, int partitionId) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata.setSegmentName(segment);
    offlineSegmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections
        .singletonMap(PARTITION_COLUMN,
            new ColumnPartitionMetadata(partitionFunction, numPartitions, Collections.singleton(partitionId)))));
    ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, offlineSegmentZKMetadata);
  }
}
