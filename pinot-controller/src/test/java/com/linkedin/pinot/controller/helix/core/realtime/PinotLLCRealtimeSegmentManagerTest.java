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

package com.linkedin.pinot.controller.helix.core.realtime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import static org.mockito.Mockito.mock;


public class PinotLLCRealtimeSegmentManagerTest {
  private static final String clusterName = "testCluster";
  private String[] serverNames;

  private List<String> getInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= serverNames.length);
    String[] instanceArray = Arrays.copyOf(serverNames, nServers);
    return Arrays.asList(instanceArray);
  }

  @BeforeTest
  public void setUp() {
    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
    }
  }

  @Test
  public void testKafkaAssignment() throws Exception {
    testKafkaAssignment(8, 3, 2);
    testKafkaAssignment(16, 4, 3);
    testKafkaAssignment(16, 13, 3);
    testKafkaAssignment(16, 6, 5);
  }

  public void testKafkaAssignment(final int nPartitions, final int nInstances, final int nReplicas) {
    MockPinotLLCRealtimeSegmentManager segmentManager = new MockPinotLLCRealtimeSegmentManager(false, null);

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(nInstances);
//    String[] instances = {server1, server2, server3, server4};
    final long startOffset = 7L;

    // Populate 'partitionSet' with all kafka partitions,
    // As we find partitions in the assigment, we will remove the partition from this set.
    Set<Integer> partitionSet = new HashSet<>(nPartitions);
    for (int i = 0; i < nPartitions; i++) {
      partitionSet.add(i);
    }

    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset, null, false);

    Map<String, List<String>> assignmentMap = segmentManager._partitionAssignment.getListFields();
    Assert.assertEquals(assignmentMap.size(), nPartitions);
    // The map looks something like this:
    // {
    //  "0" : [S1, S2],
    //  "1" : [S2, S3],
    //  "2" : [S3, S4],
    // }
    // Walk through the map, making sure that every partition (and no more) appears in the key, and
    // every one of them has exactly as many elements as the number of replicas.
    for (Map.Entry<String, List<String>> entry : assignmentMap.entrySet()) {
      int p = Integer.valueOf(entry.getKey());
      Assert.assertTrue(partitionSet.contains(p));
      partitionSet.remove(p);

      Assert.assertEquals(entry.getValue().size(), nReplicas, "Mismatch for partition " + p);
      // Make sure that we have unique server entries in the list for that partition
      Set allServers = new HashSet(instances);
      for (String server : entry.getValue()) {
        Assert.assertTrue(allServers.contains(server));
        allServers.remove(server);
      }
      // allServers may not be empty here.
    }
    Assert.assertTrue(partitionSet.isEmpty());    // We should have no more partitions left.
  }

  @Test
  public void testInitialSegmentAssignments() throws Exception {
    testInitialSegmentAssignments(8, 3, 2, false);
    testInitialSegmentAssignments(16, 4, 3, true);
    testInitialSegmentAssignments(16, 13, 3, false);
    testInitialSegmentAssignments(16, 6, 5, true);
  }

  private void testInitialSegmentAssignments(final int nPartitions, final int nInstances, final int nReplicas,
      boolean existingIS) {
    MockPinotLLCRealtimeSegmentManager segmentManager = new MockPinotLLCRealtimeSegmentManager(true, null);

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(nInstances);
    final long startOffset = 81L;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName);
    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset, idealState,
        !existingIS);

    final String actualRtTableName = segmentManager._realtimeTableName;
    final Map<String, List<String>> idealStateEntries = segmentManager._idealStateEntries;
    final List<String> propStorePaths = segmentManager._paths;
    final List<ZNRecord> propStoreEntries = segmentManager._records;
    final boolean createNew = segmentManager._createNew;

    Assert.assertEquals(propStorePaths.size(), nPartitions);
    Assert.assertEquals(propStoreEntries.size(), nPartitions);
    Assert.assertEquals(idealStateEntries.size(), nPartitions);
    Assert.assertEquals(actualRtTableName, rtTableName);
    Assert.assertEquals(createNew, !existingIS);

    Map<Integer, ZNRecord> segmentPropStoreMap = new HashMap<>(propStorePaths.size());
    Map<Integer, String> segmentPathsMap = new HashMap<>(propStorePaths.size());
    for (String path : propStorePaths) {
      String segNameStr = path.split("/")[3];
      int partition = new LLCSegmentName(segNameStr).getPartitionId();
      segmentPathsMap.put(partition, path);
    }

    for (ZNRecord znRecord : propStoreEntries) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
      segmentPropStoreMap.put(new LLCSegmentName(metadata.getSegmentName()).getPartitionId(), znRecord);
    }

    Assert.assertEquals(segmentPathsMap.size(), nPartitions);
    Assert.assertEquals(segmentPropStoreMap.size(), nPartitions);

    for (int partition = 0; partition < nPartitions; partition++) {
      final LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata(segmentPropStoreMap.get(partition));
      metadata.toString();  // Just for coverage
      ZNRecord znRecord = metadata.toZNRecord();
      LLCRealtimeSegmentZKMetadata metadataCopy = new LLCRealtimeSegmentZKMetadata(znRecord);
      Assert.assertEquals(metadata, metadataCopy);
      final String path = segmentPathsMap.get(partition);
      final String segmentName = metadata.getSegmentName();
      Assert.assertEquals(metadata.getStartOffset(), startOffset);
      Assert.assertEquals(path, "/SEGMENTS/" + rtTableName + "/" + segmentName);
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      Assert.assertEquals(llcSegmentName.getPartitionId(), partition);
      Assert.assertEquals(llcSegmentName.getTableName(), TableNameBuilder.extractRawTableName(rtTableName));
      Assert.assertEquals(metadata.getNumReplicas(), nReplicas);
    }
  }

  @Test
  public void testPreExistingSegments() throws Exception {
    LLCSegmentName existingSegmentName = new LLCSegmentName("someTable", 1, 31, 12355L);
    String[] existingSegs = {existingSegmentName.getSegmentName()};
    MockPinotLLCRealtimeSegmentManager segmentManager = new MockPinotLLCRealtimeSegmentManager(true, Arrays.asList(existingSegs));

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(3);
    final long startOffset = 81L;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName);
    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, idealState, false);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }

    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, idealState, true);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  static class MockPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {

    private final boolean _setupInitialSegments;
    private final List<String> _existingLLCSegments;

    private static HelixManager createMockHelixManager() {
      HelixManager helixManager = mock(HelixManager.class);
      return  helixManager;
    }

    private static HelixAdmin createMockHelixAdmin() {
      HelixAdmin helixAdmin = mock(HelixAdmin.class);
      return helixAdmin;
    }

    private static ZkHelixPropertyStore createMockPropertyStore() {
      ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
      return propertyStore;
    }

    private static PinotHelixResourceManager createMockHelixResourceManager() {
      PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
      return helixResourceManager;
    }

    public String _realtimeTableName;
    public Map<String, List<String>> _idealStateEntries;
    public List<String> _paths;
    public List<ZNRecord> _records;
    public ZNRecord _partitionAssignment;
    public long _startOffset;
    public boolean _createNew;

    protected MockPinotLLCRealtimeSegmentManager(boolean setupInitialSegments, List<String> existingLLCSegments) {
      super(createMockHelixAdmin(), clusterName, createMockHelixManager(), createMockPropertyStore(),
          createMockHelixResourceManager());
      _setupInitialSegments = setupInitialSegments;
      _existingLLCSegments = existingLLCSegments;
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records) {
      _paths = paths;
      _records = records;
    }

    @Override
    protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, long startOffset,
        IdealState idealState, boolean create) {
      _realtimeTableName = realtimeTableName;
      _partitionAssignment = partitionAssignment;
      _startOffset = startOffset;
      if (_setupInitialSegments) {
        super.setupInitialSegments(realtimeTableName, partitionAssignment, startOffset, idealState, create);
      }
    }

    @Override
    protected List<String> getExistingLLCSegments(String realtimeTableName) {
      return _existingLLCSegments;
    }

    @Override
    protected void updateHelixIdealState(IdealState idealState, String realtimeTableName, Map<String, List<String>> idealStateEntries,
        boolean create) {
      _realtimeTableName = realtimeTableName;
      _idealStateEntries = idealStateEntries;
      _createNew = create;
    }
  }
}
