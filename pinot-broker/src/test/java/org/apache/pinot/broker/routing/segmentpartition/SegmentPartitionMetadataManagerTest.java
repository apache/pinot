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
package org.apache.pinot.broker.routing.segmentpartition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;


public class SegmentPartitionMetadataManagerTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String PARTITION_COLUMN_FUNC = "Murmur";
  private static final int PARTITION_COLUMN_SIZE = 2;
  private static final String PARTITION_COLUMN_FUNC_ALT = "Modulo";
  private static final int PARTITION_COLUMN_SIZE_ALT = 4;
  private static final String SERVER_0 = "server0";
  private static final String SERVER_1 = "server1";


  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    startZk();
    _zkClient =
        new ZkClient(getZkUrl(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/TimeBoundaryManagerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testPartitionMetadataManagerProcessingThroughSegmentChangesSinglePartitionTable() {
    // NOTE: Ideal state and external view are not used in the current implementation
    TableConfig tableConfig = getTableConfig(TABLE_NAME, new String[]{PARTITION_COLUMN},
        new String[]{PARTITION_COLUMN_FUNC}, new int[]{PARTITION_COLUMN_SIZE});
    ExternalView externalView = new ExternalView(tableConfig.getTableName());
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE);
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
    IdealState idealState = new IdealState("");

    SegmentPartitionMetadataManager singlePartitionMetadataManager =
        new SegmentPartitionMetadataManager(TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC, PARTITION_COLUMN_SIZE);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher = new SegmentZkMetadataFetcher(TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(singlePartitionMetadataManager);

    // 1. initial state should be all empty.
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(), Collections.emptyMap());
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), Collections.emptyMap());

    // 2. adding segments without partition metadata should not be recorded in partition metadata manager.
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentAssignment.put(segmentWithoutPartitionMetadata, onlineInstanceStateMap);
    SegmentZKMetadata segmentZKMetadataWithoutPartitionMetadata =
        new SegmentZKMetadata(segmentWithoutPartitionMetadata);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, TABLE_NAME,
        segmentZKMetadataWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(), Collections.emptyMap());
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), Collections.emptyMap());

    // 3. adding segments inline with the partition column config should yield correct partition results.
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    segmentAssignment.put(segment0, Collections.singletonMap(SERVER_0, ONLINE));
    setSegmentZKPartitionMetadata(TABLE_NAME, segment0, PARTITION_COLUMN_FUNC, PARTITION_COLUMN_SIZE, 0);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        Collections.singletonMap(0, Arrays.asList(segment0)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), Collections.singletonMap(0,
        Collections.singleton(SERVER_0)));

    // 4. adding one more segments
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    segmentAssignment.put(segment1, Collections.singletonMap(SERVER_1, ONLINE));
    setSegmentZKPartitionMetadata(TABLE_NAME, segment1, PARTITION_COLUMN_FUNC, PARTITION_COLUMN_SIZE, 1);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.singleton(SERVER_1)));

    // 4. Update partition metadata without refreshing should have no effect
    setSegmentZKPartitionMetadata(TABLE_NAME, segment0, PARTITION_COLUMN_FUNC_ALT, PARTITION_COLUMN_SIZE_ALT, 0);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.singleton(SERVER_1)));

    // 5. Refresh the changed segment should update the partition info.
    segmentZkMetadataFetcher.refreshSegment(segment0);
    // segment0 is no longer inline with the table config. it should not appear on the list:
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(1, Arrays.asList(segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(1,
        Collections.singleton(SERVER_1)));

    // 6. Refresh the changed segment back to inline
    // both segments should now be back on the partition list.
    setSegmentZKPartitionMetadata(TABLE_NAME, segment0, PARTITION_COLUMN_FUNC, PARTITION_COLUMN_SIZE, 0);
    segmentZkMetadataFetcher.refreshSegment(segment0);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.singleton(SERVER_1)));

    // 7. change one of the segments to be on a different server should update the full replica server amp
    segmentAssignment.put(segment1, Collections.singletonMap(SERVER_0, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.singleton(SERVER_0)));

    // 8. adding one more segment to partition-1 but located on a different server will update the partition map.
    // but remove the full replica server map because it is no longer having full replica on a single server
    String segment2 = "segment2";
    onlineSegments.add(segment2);
    segmentAssignment.put(segment2, Collections.singletonMap(SERVER_1, ONLINE));
    setSegmentZKPartitionMetadata(TABLE_NAME, segment2, PARTITION_COLUMN_FUNC, PARTITION_COLUMN_SIZE, 1);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment2, segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.emptySet()));

    // 9. update the new segment to be replicated between 2 servers should add the full replica server back
    segmentAssignment.put(segment2, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment1, ImmutableMap.of(SERVER_0, OFFLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment2, segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        Collections.singleton(SERVER_0), 1, Collections.singleton(SERVER_1)));

    // 10. making all of them replicated will show full list
    segmentAssignment.put(segment2, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment1, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentAssignment.put(segment0, ImmutableMap.of(SERVER_0, ONLINE, SERVER_1, ONLINE));
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionMetadataManager.getPartitionToSegmentsMap(),
        ImmutableMap.of(0, Arrays.asList(segment0), 1, Arrays.asList(segment2, segment1)));
    assertEquals(singlePartitionMetadataManager.getPartitionToFullyReplicatedServersMap(), ImmutableMap.of(0,
        ImmutableSet.of(SERVER_0, SERVER_1), 1, ImmutableSet.of(SERVER_0, SERVER_1)));
  }

  private TableConfig getTableConfig(String rawTableName, String[] partitionColumns, String[] partitionFunctions,
      int[] partitionSizes) {
    Map<String, ColumnPartitionConfig> partitionColumnMetadataMap = new HashMap<>();
    for (int idx = 0; idx < partitionColumns.length; idx++) {
      partitionColumnMetadataMap.put(partitionColumns[idx], new ColumnPartitionConfig(partitionFunctions[idx],
          partitionSizes[idx]));
    }
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(partitionColumnMetadataMap))
        .build();
  }

  private void setSegmentZKPartitionMetadata(String tableNameWithType, String segment, String partitionFunction,
      int numPartitions, int partitionId) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
        new ColumnPartitionMetadata(partitionFunction, numPartitions, Collections.singleton(partitionId), null))));
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }
}
