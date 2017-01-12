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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.yammer.metrics.core.MetricsRegistry;
import static org.mockito.Mockito.*;


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
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(false, null);

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

    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset, DUMMY_HOST,
        null, true, 1000000);

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
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(true, null);

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(nInstances);
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, nReplicas);
    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset,
        DUMMY_HOST, idealState, !existingIS, 1000000);

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
    FakePinotLLCRealtimeSegmentManager
        segmentManager = new FakePinotLLCRealtimeSegmentManager(true, Arrays.asList(existingSegs));

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(3);
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, 10);
    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, DUMMY_HOST, idealState, false,
          10000);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }

    try {
      segmentManager.setupHelixEntries(topic, rtTableName, 8, instances, 3, startOffset, DUMMY_HOST, idealState, true,
          10000);
      Assert.fail("Did not get expected exception when setting up helix with existing segments in propertystore");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  @Test
  public void testCommittingSegment() throws Exception {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(true, null);

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
        !existingIS, 10000);
    // Now commit the first segment of partition 6.
    final int committingPartition = 6;
    final long nextOffset = 3425666L;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =  new LLCRealtimeSegmentZKMetadata(segmentManager._records.get(committingPartition));
    segmentManager._paths.clear();
    segmentManager._records.clear();
    boolean status = segmentManager.commitSegment(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset);
    segmentManager.verifyMetadataInteractions();
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
    Assert.assertEquals(Long.valueOf(oldMetadata.getCrc()), Long.valueOf(FakePinotLLCRealtimeSegmentManager.CRC));
    Assert.assertEquals(oldMetadata.getStartTime(), FakePinotLLCRealtimeSegmentManager.INTERVAL.getStartMillis());
    Assert.assertEquals(oldMetadata.getEndTime(), FakePinotLLCRealtimeSegmentManager.INTERVAL.getEndMillis());
    Assert.assertEquals(oldMetadata.getTotalRawDocs(), FakePinotLLCRealtimeSegmentManager.NUM_DOCS);
    Assert.assertEquals(oldMetadata.getIndexVersion(), FakePinotLLCRealtimeSegmentManager.SEGMENT_VERSION);
  }

  @Test
  public void testUpdateHelixForSegmentClosing() throws Exception {
    final IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(
        "someTable_REALTIME", 17);
    final String s1 = "S1";
    final String s2 = "S2";
    final String s3 = "S3";
    String[] instanceArr = {s1, s2, s3};
    final String oldSegmentNameStr = "oldSeg";
    final String newSegmentNameStr = "newSeg";

    idealState.setPartitionState(oldSegmentNameStr, s1,
        PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    idealState.setPartitionState(oldSegmentNameStr, s2,
        PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    idealState.setPartitionState(oldSegmentNameStr, s3,
        PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
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

  // Make a tableconfig that returns the topic name and nReplicas per partition as we need it.
  private AbstractTableConfig makeTableConfig(int nReplicas, String topic) {
    AbstractTableConfig mockTableConfig = mock(AbstractTableConfig.class);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartition()).thenReturn(Integer.toString(nReplicas));
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    Map<String, String> streamConfigMap = new HashMap<>(1);
    String consumerTypesCsv =streamConfigMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
            CommonConstants.Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE), "simple");
    streamConfigMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.TOPIC_NAME), topic);
    IndexingConfig mockIndexConfig = mock(IndexingConfig.class);
    when(mockIndexConfig.getStreamConfigs()).thenReturn(streamConfigMap);
    when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexConfig);
    TenantConfig mockTenantConfig = mock(TenantConfig.class);
    when(mockTenantConfig.getServer()).thenReturn("freeTenant");
    when(mockTableConfig.getTenantConfig()).thenReturn(mockTenantConfig);
    return mockTableConfig;
  }

  @Test
  public void testUpdatingKafkaPartitions() throws Exception {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(false, null);

    int nInstances = 3;
    int nKafkaPartitions = 8;
    int nReplicas = 3;

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(nInstances);
    final String startOffset = KAFKA_OFFSET;

    // Populate 'partitionSet' with all kafka partitions,
    // As we find partitions in the assigment, we will remove the partition from this set.
    Set<Integer> partitionSet = new HashSet<>(nKafkaPartitions);
    for (int i = 0; i < nKafkaPartitions; i++) {
      partitionSet.add(i);
    }

    // Setup initial entries
    segmentManager.setupHelixEntries(topic, rtTableName, nKafkaPartitions, instances, nReplicas, startOffset, DUMMY_HOST,
        null, true, 1000);

    ZNRecord partitionAssignment = segmentManager._partitionAssignment;
    segmentManager._currentKafkaPartitionCount = nKafkaPartitions;
    segmentManager._currentInstanceList = instances;

    // Call to update the partition list should do nothing.
    AbstractTableConfig tableConfig = makeTableConfig(nReplicas, topic);
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    Assert.assertTrue(segmentManager._partitionAssignment == partitionAssignment);

    // Change the number of kafka partitions to 9, and we should generate a new partition assignment
    nKafkaPartitions = 9;
    segmentManager._currentKafkaPartitionCount = nKafkaPartitions;
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    partitionAssignment = validatePartitionAssignment(segmentManager, nKafkaPartitions, nReplicas);

    // Now reduce the number of instances and, we should not be updating anything.
    segmentManager._currentInstanceList = getInstanceList(nInstances-1);
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    Assert.assertTrue(partitionAssignment == segmentManager._partitionAssignment);

    // Change the number of servers to 1 more, and we should update it again.
    nInstances++;
    segmentManager._currentInstanceList = getInstanceList(nInstances);
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    Assert.assertTrue(partitionAssignment != segmentManager._partitionAssignment);
    partitionAssignment = validatePartitionAssignment(segmentManager, nKafkaPartitions, nReplicas);

    // Change the replica count to one more, and we should update the assignment
    nReplicas++;
    tableConfig = makeTableConfig(nReplicas, topic);
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    Assert.assertTrue(partitionAssignment != segmentManager._partitionAssignment);
    partitionAssignment = validatePartitionAssignment(segmentManager, nKafkaPartitions, nReplicas);

    // Change the list of servers while keeping the number of servers the same.
    // We should see a change in the partition assignment.
    String server1 = segmentManager._currentInstanceList.get(0);
    segmentManager._currentInstanceList.set(0, server1 + "_new");
    Assert.assertEquals(nInstances, segmentManager._currentInstanceList.size());
    segmentManager.updateKafkaPartitionsIfNecessary(rtTableName, tableConfig);
    Assert.assertTrue(partitionAssignment != segmentManager._partitionAssignment);
    partitionAssignment = validatePartitionAssignment(segmentManager, nKafkaPartitions, nReplicas);
  }

  private ZNRecord validatePartitionAssignment(FakePinotLLCRealtimeSegmentManager segmentManager, int nKafkaPartitions,
      int nReplicas) {
    ZNRecord partitionAssignment;
    Map<String, List<String>> partitionToServerListMap;
    partitionAssignment = segmentManager._partitionAssignment;
    partitionToServerListMap = partitionAssignment.getListFields();
    Assert.assertEquals(partitionToServerListMap.size(), nKafkaPartitions);
    for (List<String> serverList : partitionToServerListMap.values()) {
      Assert.assertEquals(serverList.size(), nReplicas);
    }
    return partitionAssignment;
  }

  public void testAutoReplaceConsumingSegment(final String tableConfigStartOffset) throws Exception {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(true, null);
    final int nPartitions = 8;
    final int nInstances = 3;
    final int nReplicas = 2;

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    List<String> instances = getInstanceList(nInstances);
    final String startOffset = KAFKA_OFFSET;

    IdealState  idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName,
        nReplicas);
    segmentManager.setupHelixEntries(topic, rtTableName, nPartitions, instances, nReplicas, startOffset, DUMMY_HOST,
        idealState, false, 10000);
    // Add another segment for each partition
    long now = System.currentTimeMillis();
    List<String> existingSegments = new ArrayList<>(segmentManager._idealStateEntries.keySet());
    final int partitionToBeFixed = 3;
    final int partitionWithHigherOffset = 4;
    final int emptyPartition = 5;
    final long smallestPartitionOffset = 0x259080984568L;
    final long largestPartitionOffset = smallestPartitionOffset + 100000;
    final long higherOffset = smallestPartitionOffset + 100;
    for (String segmentNameStr : existingSegments) {
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      switch (segmentName.getPartitionId()) {
        case partitionToBeFixed:
          // Do nothing, we will test adding a new segment for this partition when there is only one segment in there.
          break;
        case emptyPartition:
          // Remove existing segment, so we can test adding a new segment for this partition when none exists
          segmentManager._idealStateEntries.remove(segmentNameStr);
          break;
        case partitionWithHigherOffset:
          // Set segment metadata for this segment such that its offset is higher than startOffset we get from kafka.
          // In that case, we should choose the new segment offset as this one rather than the one kafka hands us.
          LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
          metadata.setSegmentName(segmentName.getSegmentName());
          metadata.setEndOffset(higherOffset);
          metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
          segmentManager._metadataMap.put(segmentName.getSegmentName(), metadata);
          break;
        default:
          // Add a second segment for this partition. It will not be repaired.
          LLCSegmentName newSegmentName = new LLCSegmentName(segmentName.getTableName(), segmentName.getPartitionId(),
              segmentName.getSequenceNumber() + 1, now);
          List<String> hosts = segmentManager._idealStateEntries.get(segmentNameStr);
          segmentManager._idealStateEntries.put(newSegmentName.getSegmentName(), hosts);
          break;
      }
    }

    Map<String, String> streamPropMap = new HashMap<>(1);
    streamPropMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE), "simple");
    streamPropMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET), tableConfigStartOffset);
    KafkaStreamMetadata kafkaStreamMetadata = new KafkaStreamMetadata(streamPropMap);
    AbstractTableConfig tableConfig = mock(AbstractTableConfig.class);
    IndexingConfig indexingConfig = mock(IndexingConfig.class);
    when(indexingConfig.getStreamConfigs()).thenReturn(streamPropMap);
    when(tableConfig.getIndexingConfig()).thenReturn(indexingConfig);
    List<Integer> nonConsumingPartitions = new ArrayList<>(1);
    nonConsumingPartitions.add(partitionToBeFixed);
    nonConsumingPartitions.add(partitionWithHigherOffset);
    nonConsumingPartitions.add(emptyPartition);
    segmentManager._kafkaSmallestOffsetToReturn = smallestPartitionOffset;
    segmentManager._kafkaLargestOffsetToReturn = largestPartitionOffset;
    existingSegments = new ArrayList<>(segmentManager._idealStateEntries.keySet());
    segmentManager._paths.clear();
    segmentManager._records.clear();
    segmentManager.createConsumingSegment(rtTableName, nonConsumingPartitions, existingSegments, tableConfig);
    Assert.assertEquals(segmentManager._paths.size(), 3);
    Assert.assertEquals(segmentManager._records.size(), 3);
    Assert.assertEquals(segmentManager._oldSegmentNameStr.size(), 3);
    Assert.assertEquals(segmentManager._newSegmentNameStr.size(), 3);

    int found = 0;
    int index = 0;
    while (index < segmentManager._paths.size()) {
      String znodePath = segmentManager._paths.get(index);
      int slash = znodePath.lastIndexOf('/');
      String segmentNameStr = znodePath.substring(slash + 1);
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      ZNRecord znRecord;
      LLCRealtimeSegmentZKMetadata metadata;
      switch (segmentName.getPartitionId()) {
        case partitionToBeFixed:
          // We had left this partition with one segment. So, a second one should be created with a sequence number one
          // higher than starting. Its start offset should be what kafka returns.
          found++;
          Assert.assertEquals(segmentName.getSequenceNumber(),
              PinotLLCRealtimeSegmentManager.STARTING_SEQUENCE_NUMBER + 1);
          znRecord = segmentManager._records.get(index);
          metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
          Assert.assertEquals(metadata.getNumReplicas(), 2);
          Assert.assertEquals(metadata.getStartOffset(), smallestPartitionOffset);
          break;
        case emptyPartition:
          // We had removed any segments in this partition. A new one should be created with the offset as returned
          // by kafka and with the starting sequence number.
          found++;
          Assert.assertEquals(segmentName.getSequenceNumber(), PinotLLCRealtimeSegmentManager.STARTING_SEQUENCE_NUMBER);
          znRecord = segmentManager._records.get(index);
          metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
          Assert.assertEquals(metadata.getNumReplicas(), 2);
          if (tableConfigStartOffset.equals("smallest")) {
            Assert.assertEquals(metadata.getStartOffset(), smallestPartitionOffset);
          } else {
            Assert.assertEquals(metadata.getStartOffset(), largestPartitionOffset);
          }
          break;
        case partitionWithHigherOffset:
          // We had left this partition with one segment. In addition, we had the end-offset of the first segment set to
          // a value higher than that returned by kafka. So, a second one should be created with a sequence number one
          // equal to the end offset of the first one.
          found++;
          Assert.assertEquals(segmentName.getSequenceNumber(),
              PinotLLCRealtimeSegmentManager.STARTING_SEQUENCE_NUMBER + 1);
          znRecord = segmentManager._records.get(index);
          metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
          Assert.assertEquals(metadata.getNumReplicas(), 2);
          Assert.assertEquals(metadata.getStartOffset(), higherOffset);
          break;
      }
      index++;
    }

    // We should see all three cases here.
    Assert.assertEquals(3, found);

    // Now, if we make 'partitionToBeFixed' a non-consuming partition, a second one should get added with the same start offset as
    // as the first one, since the kafka offset to return has not changed.
    List<Integer> ncPartitions = new ArrayList<>(1);
    ncPartitions.add(partitionToBeFixed);
    segmentManager.createConsumingSegment(rtTableName, ncPartitions, segmentManager.getExistingSegments(rtTableName), tableConfig);
    Assert.assertEquals(segmentManager._paths.size(), 4);
    Assert.assertEquals(segmentManager._records.size(), 4);
    Assert.assertEquals(segmentManager._oldSegmentNameStr.size(), 4);
    Assert.assertEquals(segmentManager._newSegmentNameStr.size(), 4);
    // The latest zn record should be that of the new one we added.
    ZNRecord znRecord = segmentManager._records.get(3);
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
    Assert.assertEquals(metadata.getNumReplicas(), 2);
    Assert.assertEquals(metadata.getStartOffset(), smallestPartitionOffset);
    LLCSegmentName llcSegmentName = new LLCSegmentName(metadata.getSegmentName());
    Assert.assertEquals(llcSegmentName.getSequenceNumber(), PinotLLCRealtimeSegmentManager.STARTING_SEQUENCE_NUMBER + 2);
    Assert.assertEquals(llcSegmentName.getPartitionId(), partitionToBeFixed);

    // Now pretend the prev segment ended successfully, and set the end offset
    metadata.setEndOffset(metadata.getStartOffset() + 10);
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentManager._records.remove(3);
    segmentManager._records.add(metadata.toZNRecord());
    segmentManager._metadataMap.put(metadata.getSegmentName(), metadata);

    segmentManager._kafkaLargestOffsetToReturn *= 2;
    segmentManager._kafkaSmallestOffsetToReturn *= 2;
    ncPartitions.clear();
    ncPartitions.add(partitionToBeFixed);
    segmentManager.createConsumingSegment(rtTableName, ncPartitions, segmentManager.getExistingSegments(rtTableName),
        tableConfig);
    Assert.assertEquals(segmentManager._paths.size(), 5);
    Assert.assertEquals(segmentManager._records.size(), 5);
    Assert.assertEquals(segmentManager._oldSegmentNameStr.size(), 5);
    Assert.assertEquals(segmentManager._newSegmentNameStr.size(), 5);
    znRecord = segmentManager._records.get(4);
    metadata = new LLCRealtimeSegmentZKMetadata(znRecord);
    Assert.assertEquals(metadata.getNumReplicas(), 2);
    // In this case, since we have data loss, we will always put the smallest kafka partition available.
    Assert.assertEquals(metadata.getStartOffset(), segmentManager.getKafkaPartitionOffset(null, "smallest" ,partitionToBeFixed));
    llcSegmentName = new LLCSegmentName(metadata.getSegmentName());
    Assert.assertEquals(llcSegmentName.getSequenceNumber(), PinotLLCRealtimeSegmentManager.STARTING_SEQUENCE_NUMBER + 3);
    Assert.assertEquals(llcSegmentName.getPartitionId(), partitionToBeFixed);
  }

  @Test
  public void testAutoReplaceConsumingSegment() throws Exception {
    testAutoReplaceConsumingSegment("smallest");
    testAutoReplaceConsumingSegment("largest");
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

      FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(false, null);
      segmentManager.setupHelixEntries(topic, realtimeTableName, nPartitions, instances, nReplicas, startOffset,
          DUMMY_HOST, idealState, false, 10000);
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

  static class FakePinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {

    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    private final boolean _setupInitialSegments;
    private List<String> _existingLLCSegments = new ArrayList<>(1);

    public String _realtimeTableName;
    public Map<String, List<String>> _idealStateEntries = new HashMap<>(1);
    public List<String> _paths = new ArrayList<>(16);
    public List<ZNRecord> _records = new ArrayList<>(16);
    public ZNRecord _partitionAssignment;
    public String _startOffset;
    public boolean _createNew;
    public int _nReplicas;
    public Map<String, LLCRealtimeSegmentZKMetadata> _metadataMap = new HashMap<>(4);

    public List<String> _newInstances;
    public List<String> _oldSegmentNameStr = new ArrayList<>(16);
    public List<String> _newSegmentNameStr = new ArrayList<>(16);

    public long _kafkaLargestOffsetToReturn;
    public long _kafkaSmallestOffsetToReturn;

    public List<ZNRecord> _existingSegmentMetadata;

    public int _nCallsToUpdateHelix = 0;
    public IdealState _tableIdealState;
    public List<String> _currentInstanceList;
    public int _currentKafkaPartitionCount;

    public static final String CRC = "5680988776500";
    public static final Interval INTERVAL = new Interval(3000, 4000);
    public static final String SEGMENT_VERSION = SegmentVersion.v1.toString();
    public static final int NUM_DOCS = 5099775;

    private SegmentMetadataImpl segmentMetadata;

    protected FakePinotLLCRealtimeSegmentManager(boolean setupInitialSegments, List<String> existingLLCSegments) {
      super(null, clusterName, null, null, null, CONTROLLER_CONF, new ControllerMetrics(new MetricsRegistry()));
      _setupInitialSegments = setupInitialSegments;
      if (existingLLCSegments != null) {
        _existingLLCSegments = existingLLCSegments;
      }
      CONTROLLER_CONF.setControllerVipHost("vip");
      CONTROLLER_CONF.setControllerPort("9000");
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records) {
      _paths.addAll(paths);
      _records.addAll(records);
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        ZNRecord znRecord = records.get(i);
        String segmentId = getSegmentNameFromPath(path);
        _existingLLCSegments.add(segmentId);
        _metadataMap.put(segmentId,new LLCRealtimeSegmentZKMetadata(znRecord));
      }
    }

    private String getSegmentNameFromPath(String path) {
      return path.substring(path.lastIndexOf("/")+1);
    }

    @Override
    protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, String topicName, String startOffset, String bootstrapHostList,
        IdealState idealState, boolean create, int nReplicas, int flushSize) {
      _realtimeTableName = realtimeTableName;
      _partitionAssignment = partitionAssignment;
      _startOffset = startOffset;
      if (_setupInitialSegments) {
        super.setupInitialSegments(realtimeTableName, partitionAssignment, topicName, startOffset, bootstrapHostList, idealState, create, nReplicas, flushSize);
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
      for (Map.Entry<String, List<String>> entry: idealStateEntries.entrySet()) {
        _idealStateEntries.put(entry.getKey(), entry.getValue());
      }
    }

    protected void updateHelixIdealState(final String realtimeTableName, final List<String> newInstances,
        final String oldSegmentNameStr, final String newSegmentNameStr) {
      _realtimeTableName = realtimeTableName;
      _newInstances = newInstances;
      _oldSegmentNameStr.add(oldSegmentNameStr);
      _newSegmentNameStr.add(newSegmentNameStr);
      _idealStateEntries.put(newSegmentNameStr, newInstances);
      _nCallsToUpdateHelix++;
    }

    protected void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
      _partitionAssignment = znRecord;
    }

    public ZNRecord getKafkaPartitionAssignment(final String realtimeTableName) {
      return _partitionAssignment;
    }

    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName) {
      if (_metadataMap.containsKey(segmentName)) {
        return _metadataMap.get(segmentName);
      }
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(segmentName);
      return metadata;
    }

    @Override
    protected SegmentMetadataImpl extractSegmentMetadata(final String rawTableName, final String segmentNameStr) {
      segmentMetadata = mock(SegmentMetadataImpl.class);
      when(segmentMetadata.getCrc()).thenReturn(CRC);
      when(segmentMetadata.getTimeInterval()).thenReturn(INTERVAL);
      when(segmentMetadata.getVersion()).thenReturn(SEGMENT_VERSION);
      when(segmentMetadata.getTotalRawDocs()).thenReturn(NUM_DOCS);
      return segmentMetadata;
    }

    public void verifyMetadataInteractions() {
      verify(segmentMetadata, times(1)).getCrc();
      verify(segmentMetadata, times(2)).getTimeInterval();
      verify(segmentMetadata, times(1)).getVersion();
      verify(segmentMetadata, times(1)).getTotalRawDocs();
      verifyNoMoreInteractions(segmentMetadata);
    }

    @Override
    protected long getKafkaPartitionOffset(KafkaStreamMetadata kafkaStreamMetadata, final String offsetCriteria,
        int partitionId) {
      if (offsetCriteria.equals("largest")) {
        return _kafkaLargestOffsetToReturn;
      } else {
        return _kafkaSmallestOffsetToReturn;
      }
    }

    @Override
    protected List<ZNRecord> getExistingSegmentMetadata(String realtimeTableName) {
      return _existingSegmentMetadata;
    }

    @Override
    protected int getRealtimeTableFlushSizeForTable(String tableName) {
      return 1000;
    }

    @Override
    protected IdealState getTableIdealState(String realtimeTableName) {
      return _tableIdealState;
    }

    @Override
    protected List<String> getInstances(String tenantName) {
      return _currentInstanceList;
    }

    @Override
    protected int getKafkaPartitionCount(KafkaStreamMetadata metadata) {
      return _currentKafkaPartitionCount;
    }
  }

  private String makeFakeSegmentName(int id) {
    return new LLCSegmentName("fakeTable_REALTIME", id, 0, 1234L).getSegmentName();
  }

  @Test
  public void testUpdateFlushThresholdForSegmentMetadata() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = new FakePinotLLCRealtimeSegmentManager(false,
        Collections.<String>emptyList());

    ZNRecord partitionAssignment = new ZNRecord("fakeTable_REALTIME");
    // 4 partitions assigned to 4 servers, 4 replicas => the segments should have 250k rows each (1M / 4)
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for(int replicaId = 1; replicaId <= 4; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + replicaId);
      }

      partitionAssignment.setListField(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 250k rows each
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 250000);
    }

    // 4 partitions assigned to 4 servers, 2 partitions/server => the segments should have 500k rows each (1M / 2)
    partitionAssignment.getListFields().clear();
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for(int replicaId = 1; replicaId <= 2; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + ((replicaId + segmentId) % 4));
      }

      partitionAssignment.setListField(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 500k rows each
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }

    // 4 partitions assigned to 4 servers, 1 partition/server => the segments should have 1M rows each (1M / 1)
    partitionAssignment.getListFields().clear();
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();
      instances.add("Server_1.2.3.4_123" + segmentId);
      partitionAssignment.setListField(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 1M rows each
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 1000000);
    }

    // Assign another partition to all servers => the servers should have 500k rows each (1M / 2)
    List<String> instances = new ArrayList<>();
    for(int replicaId = 1; replicaId <= 4; ++replicaId) {
      instances.add("Server_1.2.3.4_123" + replicaId);
    }
    partitionAssignment.setListField("5", instances);

    // Check that each segment has 500k rows each
    for(int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }
  }
}
