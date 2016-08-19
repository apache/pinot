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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;


public class PinotLLCRealtimeSegmentManagerTest {
  private static final String clusterName = "testCluster";
  private static final String DUMMY_HOST = "dummyHost:1234";
  private static final String KAFKA_OFFSET = "testDummy";
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
    final String startOffset = KAFKA_OFFSET;

    // Populate 'partitionSet' with all kafka partitions,
    // As we find partitions in the assigment, we will remove the partition from this set.
    Set<Integer> partitionSet = new HashSet<>(nPartitions);
    for (int i = 0; i < nPartitions; i++) {
      partitionSet.add(i);
    }

    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset,
        DUMMY_HOST, null, true);

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
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, nReplicas);
    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset,
        DUMMY_HOST, idealState, !existingIS);

    final String actualRtTableName = segmentManager._realtimeTableName;
    final Map<String, List<String>> idealStateEntries = segmentManager._idealStateEntries;
    final int idealStateNReplicas = segmentManager._nReplicas;
    final List<String> propStorePaths = segmentManager._paths;
    final List<ZNRecord> propStoreEntries = segmentManager._records;
    final boolean createNew = segmentManager._createNew;

    Assert.assertEquals(propStorePaths.size(), nPartitions);
    Assert.assertEquals(propStoreEntries.size(), nPartitions);
    Assert.assertEquals(idealStateEntries.size(), nPartitions);
    Assert.assertEquals(actualRtTableName, rtTableName);
    Assert.assertEquals(createNew, !existingIS);
    Assert.assertEquals(idealStateNReplicas, nReplicas);

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
      Assert.assertEquals(metadata.getStartOffset(), -1L);
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
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, 10);
    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, DUMMY_HOST, idealState, false);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }

    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, DUMMY_HOST, idealState, true);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  @Test
  public void testCommittingSegment() throws Exception {
    MockPinotLLCRealtimeSegmentManager segmentManager = new MockPinotLLCRealtimeSegmentManager(true, null);

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(rtTableName);
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    final boolean existingIS = false;
    List<String> instances = getInstanceList(nInstances);
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, nReplicas);
    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset, DUMMY_HOST, idealState,
        !existingIS);
    // Now commit the first segment of partition 6.
    final int committingPartition = 6;
    final long nextOffset = 3425666L;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =  new LLCRealtimeSegmentZKMetadata(segmentManager._records.get(committingPartition));
    boolean status = segmentManager.commitSegment(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset);
    Assert.assertTrue(status);

    // Get the old and new segment metadata and make sure that they are correct.
    Assert.assertEquals(segmentManager._paths.size(), 2);
    ZNRecord oldZnRec = segmentManager._records.get(0);
    ZNRecord newZnRec = segmentManager._records.get(1);

    LLCRealtimeSegmentZKMetadata oldMetadata = new LLCRealtimeSegmentZKMetadata(oldZnRec);
    LLCRealtimeSegmentZKMetadata newMetadata = new LLCRealtimeSegmentZKMetadata(newZnRec);

    LLCSegmentName oldSegmentName = new LLCSegmentName(oldMetadata.getSegmentName());
    LLCSegmentName newSegmentName = new LLCSegmentName(newMetadata.getSegmentName());

    // Assert on propertystore entries
    Assert.assertEquals(oldSegmentName.getSegmentName(), committingSegmentMetadata.getSegmentName());
    Assert.assertEquals(newSegmentName.getPartitionId(), oldSegmentName.getPartitionId());
    Assert.assertEquals(newSegmentName.getSequenceNumber(), oldSegmentName.getSequenceNumber()+1);
    Assert.assertEquals(oldMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    Assert.assertEquals(newMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    Assert.assertNotNull(oldMetadata.getDownloadUrl());
  }

  @Test
  public void testUpdateHelixForSegmentClosing() throws Exception {
    final IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor("someTable_REALTIME",
        17);
    final String s1 = "S1";
    final String s2 = "S2";
    final String s3 = "S3";
    String[] instanceArr = {s1, s2, s3};
    final String oldSegmentNameStr = "oldSeg";
    final String newSegmentNameStr = "newSeg";

    idealState.setPartitionState(oldSegmentNameStr, s1, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    idealState.setPartitionState(oldSegmentNameStr, s2, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    idealState.setPartitionState(oldSegmentNameStr, s3, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    PinotLLCRealtimeSegmentManager.updateForNewRealtimeSegment(idealState, Arrays.asList(instanceArr),
        oldSegmentNameStr, newSegmentNameStr);
    // Now verify that the old segment state is online in the idealstate and the new segment state is CONSUMING
    Map<String, String> oldsegStateMap = idealState.getInstanceStateMap(oldSegmentNameStr);
    Assert.assertEquals(oldsegStateMap.get(s1), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    Assert.assertEquals(oldsegStateMap.get(s2), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    Assert.assertEquals(oldsegStateMap.get(s3), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);

    Map<String, String> newsegStateMap = idealState.getInstanceStateMap(oldSegmentNameStr);
    Assert.assertEquals(oldsegStateMap.get(s1), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    Assert.assertEquals(oldsegStateMap.get(s2), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    Assert.assertEquals(oldsegStateMap.get(s3), PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
  }

  @Test
  public void testCompleteCommittingSegments() throws Exception {
    // Run multiple times randomizing the situation.
    for (int i = 0; i < 100; i++) {
      final List<ZNRecord> existingSegmentMetadata = new ArrayList<>(64);
      final int nPartitions = 16;
      final long seed = new Random().nextLong();
      Random random = new Random(seed);
      final int maxSeq = 10;
      final long now = System.currentTimeMillis();
      final String tableName = "table";
      final String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      final IdealState idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(realtimeTableName,
          19);
      int nIncompleteCommits = 0;
      final String topic = "someTopic";
      final int nInstances = 5;
      final int nReplicas = 3;
      List<String> instances = getInstanceList(nInstances);
      final String startOffset = KAFKA_OFFSET;

      MockPinotLLCRealtimeSegmentManager segmentManager = new MockPinotLLCRealtimeSegmentManager(false, null);
      segmentManager.setupHelixEntries(topic, realtimeTableName, nPartitions, instances, nReplicas, startOffset, DUMMY_HOST, idealState, false);
      ZNRecord partitionAssignment = segmentManager.getKafkaPartitionAssignment(realtimeTableName);

      for (int p = 0; p < nPartitions; p++) {
        int curSeq = random.nextInt(maxSeq);  // Current segment sequence ID for that partition
        if (curSeq == 0) {
          curSeq++;
        }
        boolean incomplete = false;
        if (random.nextBoolean()) {
          incomplete = true;
        }
        for (int s = 0; s < curSeq; s++) {
          LLCSegmentName segmentName = new LLCSegmentName(tableName, p, s, now);
          String segNameStr = segmentName.getSegmentName();
          String state = PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE;
          CommonConstants.Segment.Realtime.Status status = CommonConstants.Segment.Realtime.Status.DONE;
          if (s == curSeq - 1) {
            state = PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE;
            if (!incomplete) {
              status = CommonConstants.Segment.Realtime.Status.IN_PROGRESS;
            }
          }
          List<String> instancesForThisSeg = partitionAssignment.getListField(Integer.toString(p));
          for (String instance : instancesForThisSeg) {
            idealState.setPartitionState(segNameStr, instance, state);
          }
          LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
          metadata.setSegmentName(segNameStr);
          metadata.setStatus(status);
          existingSegmentMetadata.add(metadata.toZNRecord());
        }
        // Add an incomplete commit to some of them
        if (incomplete) {
          nIncompleteCommits++;
          LLCSegmentName segmentName = new LLCSegmentName(tableName, p, curSeq, now);
          LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
          metadata.setSegmentName(segmentName.getSegmentName());
          metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
          existingSegmentMetadata.add(metadata.toZNRecord());
        }
      }

      segmentManager._tableIdealState = idealState;
      segmentManager._existingSegmentMetadata = existingSegmentMetadata;

      segmentManager.completeCommittingSegments(TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName));

      Assert.assertEquals(segmentManager._nCallsToUpdateHelix, nIncompleteCommits, "Failed with seed " + seed);
    }
  }

  static class MockPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {

    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    private final boolean _setupInitialSegments;
    private final List<String> _existingLLCSegments;

    public String _realtimeTableName;
    public Map<String, List<String>> _idealStateEntries;
    public List<String> _paths;
    public List<ZNRecord> _records;
    public ZNRecord _partitionAssignment;
    public String _startOffset;
    public boolean _createNew;
    public int _nReplicas;

    public List<String> _newInstances;
    public String _oldSegmentNameStr;
    public String _newSegmentNameStr;

    public List<ZNRecord> _existingSegmentMetadata;

    public int _nCallsToUpdateHelix = 0;
    public IdealState _tableIdealState;

    protected MockPinotLLCRealtimeSegmentManager(boolean setupInitialSegments, List<String> existingLLCSegments) {
      super(null, clusterName, null, null, null, CONTROLLER_CONF);
      _setupInitialSegments = setupInitialSegments;
      _existingLLCSegments = existingLLCSegments;
      CONTROLLER_CONF.setControllerVipHost("vip");
      CONTROLLER_CONF.setControllerPort("9000");
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records) {
      _paths = paths;
      _records = records;
    }

    @Override
    protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, String topicName, String startOffset, String bootstrapHostList,
        IdealState idealState, boolean create, int nReplicas) {
      _realtimeTableName = realtimeTableName;
      _partitionAssignment = partitionAssignment;
      _startOffset = startOffset;
      if (_setupInitialSegments) {
        super.setupInitialSegments(realtimeTableName, partitionAssignment, topicName, startOffset, bootstrapHostList, idealState, create, nReplicas);
      }
    }

    @Override
    protected List<String> getExistingSegments(String realtimeTableName) {
      return _existingLLCSegments;
    }

    @Override
    protected void updateHelixIdealState(IdealState idealState, String realtimeTableName, Map<String, List<String>> idealStateEntries,
        boolean create, int nReplicas) {
      _realtimeTableName = realtimeTableName;
      _idealStateEntries = idealStateEntries;
      _nReplicas = nReplicas;
      _createNew = create;
    }

    protected void updateHelixIdealState(final String realtimeTableName, final List<String> newInstances,
        final String oldSegmentNameStr, final String newSegmentNameStr) {
      _realtimeTableName = realtimeTableName;
      _newInstances = newInstances;
      _oldSegmentNameStr = oldSegmentNameStr;
      _newSegmentNameStr = newSegmentNameStr;
      _nCallsToUpdateHelix++;
    }

    protected void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
      _partitionAssignment = znRecord;
    }

    protected ZNRecord getKafkaPartitionAssignment(final String realtimeTableName) {
      return _partitionAssignment;
    }

    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(segmentName);
      return metadata;
    }

    @Override
    protected List<ZNRecord> getExistingSegmentMetadata(String realtimeTableName) {
      return _existingSegmentMetadata;
    }

    @Override
    protected IdealState getTableIdealState(String realtimeTableName) {
      return _tableIdealState;
    }
  }
}
