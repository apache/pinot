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
package com.linkedin.pinot.controller.helix.core.realtime;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.partition.IdealStateBuilderUtil;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.StreamPartitionAssignmentGenerator;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.resources.LLCSegmentCompletionHandlers;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import com.linkedin.pinot.controller.util.SegmentCompletionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerFactory;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties;
import com.linkedin.pinot.core.realtime.stream.OffsetCriteria;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.data.Stat;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class PinotLLCRealtimeSegmentManagerTest {
  private static final String clusterName = "testCluster";
  private static final String DUMMY_HOST = "dummyHost:1234";
  private static final String DEFAULT_SERVER_TENANT = "freeTenant";
  private static final String SCHEME = LLCSegmentCompletionHandlers.getScheme();
  private String[] serverNames;
  private static File baseDir;
  private Random random;

  private enum ExternalChange {
    N_INSTANCES_CHANGED, N_PARTITIONS_INCREASED, N_INSTANCES_CHANGED_AND_PARTITIONS_INCREASED
  }

  private List<String> getInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= serverNames.length);
    String[] instanceArray = Arrays.copyOf(serverNames, nServers);
    return Arrays.asList(instanceArray);
  }

  @BeforeTest
  public void setUp() {
    // Printing out the random seed to console so that we can use the seed to reproduce failure conditions
    long seed = new Random().nextLong();
    System.out.println("Random seed for " + PinotLLCRealtimeSegmentManagerTest.class.getSimpleName() + " is " + seed);
    random = new Random(seed);

    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
    }
    try {
      baseDir = Files.createTempDir();
      baseDir.deleteOnExit();
    } catch (Exception e) {

    }
    FakePinotLLCRealtimeSegmentManager.IS_CONNECTED = true;
    FakePinotLLCRealtimeSegmentManager.IS_LEADER = true;
  }

  @AfterTest
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(baseDir);
  }

  /**
   * Test cases for new table being created, and initial segments setup that follows. The tables are
   * set up with stream offset to be the 'largest' and the corresponding ideal state offset is
   * accordingly verified.
   */
  @Test
  public void testSetupNewTable() {
    String tableName = "validateThisTable_REALTIME";
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    boolean invalidConfig;
    boolean badStream = false;
    List<String> instances = getInstanceList(1);

    // insufficient instances
    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    invalidConfig = true;
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    // noop path - 0 partitions
    badStream = false;
    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    idealState = idealStateBuilder.build();
    nPartitions = 0;
    invalidConfig = false;
    instances = getInstanceList(3);
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    // noop path - ideal state disabled - this can happen in the code path only if there is already an idealstate with HLC segments in it, and it has been disabled.
    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    idealState = idealStateBuilder.disableIdealState().build();
    nPartitions = 4;
    invalidConfig = false;
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    // clear builder to enable ideal state
    idealStateBuilder.clear();
    // happy paths - new table config with nPartitions and sufficient instances
    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    invalidConfig = false;
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    idealState = idealStateBuilder.build();
    nPartitions = 8;
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    idealState = idealStateBuilder.build();
    nPartitions = 8;
    instances = getInstanceList(10);
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);

    idealState = idealStateBuilder.build();
    nPartitions = 12;
    testSetupNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, invalidConfig, badStream);
  }

  private void testSetupNewTable(TableConfig tableConfig, IdealState idealState, int nPartitions, int nReplicas,
      List<String> instances, boolean invalidConfig, boolean badStream) {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager.addTableToStore(tableConfig.getTableName(), tableConfig, nPartitions);

    try {
      segmentManager.setupNewTable(tableConfig, idealState);
    } catch (InvalidConfigException e) {
      Assert.assertTrue(invalidConfig);
      return;
    } catch (Exception e) { // Bad stream configs, offset fetcher exception
      Assert.assertTrue(badStream);
      return;
    }
    Assert.assertFalse(invalidConfig);
    Assert.assertFalse(badStream);
    Map<String, LLCSegmentName> partitionToLatestSegments =
        segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
    if (!idealState.isEnabled()) {
      Assert.assertTrue(partitionToLatestSegments.isEmpty());
    } else {
      Assert.assertEquals(mapFields.size(), nPartitions);
      for (int p = 0; p < nPartitions; p++) {
        LLCSegmentName llcSegmentName = partitionToLatestSegments.get(String.valueOf(p));
        String segmentName = llcSegmentName.getSegmentName();
        Map<String, String> instanceStateMap = mapFields.get(segmentName);
        Assert.assertEquals(instanceStateMap.size(), nReplicas);
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          Assert.assertEquals(entry.getValue(), "CONSUMING");
        }
        Assert.assertEquals(segmentManager._metadataMap.get(segmentName).getStatus(),
            CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      }

      final List<String> propStorePaths = segmentManager._paths;
      final List<ZNRecord> propStoreEntries = segmentManager._records;

      Assert.assertEquals(propStorePaths.size(), nPartitions);
      Assert.assertEquals(propStoreEntries.size(), nPartitions);

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
        final LLCRealtimeSegmentZKMetadata metadata =
            new LLCRealtimeSegmentZKMetadata(segmentPropStoreMap.get(partition));

        metadata.toString();  // Just for coverage
        ZNRecord znRecord = metadata.toZNRecord();
        LLCRealtimeSegmentZKMetadata metadataCopy = new LLCRealtimeSegmentZKMetadata(znRecord);
        Assert.assertEquals(metadata, metadataCopy);
        final String path = segmentPathsMap.get(partition);
        final String segmentName = metadata.getSegmentName();
        // verify the expected offset is set to the largest as configured in the table config
        Assert.assertEquals(metadata.getStartOffset(), segmentManager.getLargestStreamOffset());
        Assert.assertEquals(path, "/SEGMENTS/" + tableConfig.getTableName() + "/" + segmentName);
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        Assert.assertEquals(llcSegmentName.getPartitionId(), partition);
        Assert.assertEquals(llcSegmentName.getTableName(),
            TableNameBuilder.extractRawTableName(tableConfig.getTableName()));
        Assert.assertEquals(metadata.getNumReplicas(), nReplicas);
      }
    }
  }

  /**
   * Test cases for the scenario where stream partitions increase, and the validation manager is attempting to create segments for new partitions
   * This test assumes that all other factors remain same (no error conditions/inconsistencies in metadata and ideal state)
   *
   * The tables are created with "largest" offset and the consuming segments are expected to be set
   * to consume from the smallest offset.
   */
  @Test
  public void testValidateLLCPartitionIncrease() {

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    String tableName = "validateThisTable_REALTIME";
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    boolean skipNewPartitions = false;
    List<String> instances = getInstanceList(5);

    // empty to 4 partitions
    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // only initial segments present CONSUMING - metadata INPROGRESS - increase numPartitions
    nPartitions = 6;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // 2 partitions advanced a seq number
    PartitionAssignment partitionAssignment =
        segmentManager._partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig,
            idealState);
    for (int p = 0; p < 2; p++) {
      String segmentName = idealStateBuilder.getSegment(p, 0);
      advanceASeqForPartition(idealState, segmentManager, partitionAssignment, segmentName, p, 1, 100, tableConfig);
    }
    idealState = idealStateBuilder.build();
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // increase num partitions
    nPartitions = 10;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // keep num partitions same - noop
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // keep num partitions same, but bad instances - error
    instances = getInstanceList(1);
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // increase num partitions, but bad instances - error
    nPartitions = 12;
    skipNewPartitions = true;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);

    // increase num partitions, but disabled ideal state - noop
    idealState = idealStateBuilder.disableIdealState().build();
    instances = getInstanceList(6);
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        skipNewPartitions);
  }

  private void advanceASeqForPartition(IdealState idealState, FakePinotLLCRealtimeSegmentManager segmentManager,
      PartitionAssignment partitionAssignment, String segmentName, int partition, int nextSeqNum, long nextOffset,
      TableConfig tableConfig) {
    String tableName = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    segmentManager.updateOldSegmentMetadataZNRecord(tableName, llcSegmentName, nextOffset);
    LLCSegmentName newLlcSegmentName =
        new LLCSegmentName(rawTableName, partition, nextSeqNum, System.currentTimeMillis());
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, nextOffset, 0);
    segmentManager.createNewSegmentMetadataZNRecord(tableConfig, llcSegmentName, newLlcSegmentName, partitionAssignment,
        committingSegmentDescriptor, false);
    segmentManager.updateIdealStateOnSegmentCompletion(idealState, segmentName, newLlcSegmentName.getSegmentName(),
        partitionAssignment);
  }

  private void validateLLCPartitionsIncrease(FakePinotLLCRealtimeSegmentManager segmentManager, IdealState idealState,
      TableConfig tableConfig, int nPartitions, int nReplicas, List<String> instances, boolean skipNewPartitions) {
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    IdealState idealStateCopy =
        new IdealState((ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();
    Map<String, LLCRealtimeSegmentZKMetadata> oldMetadataMap = new HashMap<>(segmentManager._metadataMap.size());
    for (Map.Entry<String, LLCRealtimeSegmentZKMetadata> entry : segmentManager._metadataMap.entrySet()) {
      oldMetadataMap.put(entry.getKey(), new LLCRealtimeSegmentZKMetadata(entry.getValue().toZNRecord()));
    }
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    IdealState updatedIdealState = segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
    Map<String, Map<String, String>> updatedMapFields = updatedIdealState.getRecord().getMapFields();
    Map<String, LLCRealtimeSegmentZKMetadata> updatedMetadataMap = segmentManager._metadataMap;

    // Verify - all original metadata and segments unchanged
    Set<Integer> oldPartitions = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : oldMapFields.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      Assert.assertTrue(updatedMapFields.containsKey(segmentName));
      Map<String, String> updatedInstanceStateMap = updatedMapFields.get(segmentName);
      Assert.assertEquals(instanceStateMap.size(), updatedInstanceStateMap.size());
      Assert.assertTrue(instanceStateMap.keySet().containsAll(updatedInstanceStateMap.keySet()));
      for (Map.Entry<String, String> instanceToState : instanceStateMap.entrySet()) {
        Assert.assertEquals(instanceToState.getValue(), updatedInstanceStateMap.get(instanceToState.getKey()));
      }
      Assert.assertEquals(oldMetadataMap.get(segmentName), updatedMetadataMap.get(segmentName));
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      oldPartitions.add(llcSegmentName.getPartitionId());
    }

    List<Integer> allPartitions = new ArrayList<>(nPartitions);
    for (int p = 0; p < nPartitions; p++) {
      allPartitions.add(p);
    }
    List<Integer> newPartitions = new ArrayList<>(allPartitions);
    newPartitions.removeAll(oldPartitions);

    Map<Integer, List<String>> partitionToAllSegmentsMap = new HashMap<>(nPartitions);
    for (Integer p : allPartitions) {
      partitionToAllSegmentsMap.put(p, new ArrayList<String>());
    }
    for (Map.Entry<String, Map<String, String>> entry : updatedMapFields.entrySet()) {
      String segmentName = entry.getKey();
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      partitionToAllSegmentsMap.get(partitionId).add(segmentName);
    }

    // if skipNewPartitions, new partitions should not have any assignment

    // each new partition should have exactly 1 new segment in CONSUMING state, and metadata in IN_PROGRESS state
    for (Integer partitionId : newPartitions) {
      List<String> allSegmentsForPartition = partitionToAllSegmentsMap.get(partitionId);
      if (!skipNewPartitions && idealState.isEnabled()) {
        Assert.assertEquals(allSegmentsForPartition.size(), 1);
        for (String segment : allSegmentsForPartition) {
          Map<String, String> instanceStateMap = updatedMapFields.get(segment);
          Assert.assertEquals(instanceStateMap.size(), nReplicas);
          for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
            Assert.assertEquals(entry.getValue(), "CONSUMING");
          }
          LLCRealtimeSegmentZKMetadata newPartitionMetadata = updatedMetadataMap.get(segment);
          // for newly added partitions, we expect offset to be the smallest even though the offset is set to be largest for these tables
          Assert.assertEquals(newPartitionMetadata.getStartOffset(), segmentManager.getSmallestStreamOffset());

          Assert.assertNotNull(newPartitionMetadata);
          Assert.assertEquals(newPartitionMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        }
      } else {
        Assert.assertEquals(allSegmentsForPartition.size(), 0);
      }
    }
  }

  /**
   * Tests that we can repair all invalid scenarios that can generate during segment completion which leave the ideal state in an incorrect state
   * Segment completion takes place in 3 steps:
   * 1. Update committing segment metadata to DONE
   * 2. Create new segment metadata IN_PROGRESS
   * 3. Update ideal state as committing segment state ONLINE and create new segment with state CONSUMING
   *
   * If a failure happens before step 1 or after step 3, we do not need to fix it
   * If a failure happens after step 1 is done and before step 3 completes, we will be left in an incorrect state, and should be able to fix it
   *
   * Scenarios:
   * 1. Step 3 failed - we will find new segment metadata IN_PROGRESS but no segment in ideal state
   * Correction: create new segment in ideal state with CONSUMING, update prev segment (if exists) in ideal state to ONLINE
   *
   * 2. Step 2 failed - we will find segment metadata DONE but ideal state CONSUMING
   * Correction: create new segment metadata with state IN_PROGRESS, create new segment in ideal state CONSUMING, update prev segment to ONLINE
   *
   * 3. All replicas of a latest segment are in OFFLINE
   * Correction: create new metadata IN_PROGRESS, new segment in ideal state CONSUMING. If prev metadata was DONE, start from end offset. If prev metadata was IN_PROGRESS, start from prev start offset
   *
   * 4. TooSoonToCorrect: Segment completion has 10 minutes to retry and complete between steps 1 and 3.
   * Correction: Don't correct a segment before the allowed time
   *
   * 5. Num Partitions increased
   * Correction: create new metadata and segment in ideal state for new partition
   *
   * 6. Num instances changed
   * Correction: use updated instances for new segment assignments
   *
   * @throws InvalidConfigException
   */
  @Test
  public void testValidateLLCRepair() throws InvalidConfigException {

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    String tableName = "repairThisTable_REALTIME";
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    List<String> instances = getInstanceList(5);
    final int[] numInstancesSet = new int[]{4, 6, 8, 10};

    tableConfig = makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    nPartitions = 4;
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    PartitionAssignment expectedPartitionAssignment =
        segmentManager._partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, nPartitions);

    // set up a happy path - segment is present in ideal state, is CONSUMING, metadata says IN_PROGRESS
    idealState = clearAndSetupHappyPathIdealState(idealStateBuilder, segmentManager, tableConfig, nPartitions);

    // randomly introduce an error condition and assert that we repair it
    for (int run = 0; run < 200; run++) {

      final boolean step3NotDone = random.nextBoolean();

      final boolean tooSoonToCorrect = random.nextBoolean();
      segmentManager.tooSoonToCorrect = false;
      if (tooSoonToCorrect) {
        segmentManager.tooSoonToCorrect = true;
      }

      if (step3NotDone) {
        // failed after completing step 2: hence new segment not present in ideal state

        final boolean prevMetadataNull = random.nextBoolean();

        if (prevMetadataNull) {
          // very first seq number of this partition i.e. prev metadata null
          clearAndSetupInitialSegments(idealStateBuilder, segmentManager, tableConfig, nPartitions);

          // generate the error scenario - remove segment from ideal state, but metadata is IN_PROGRESS
          int randomlySelectedPartition = random.nextInt(nPartitions);
          Map<String, LLCSegmentName> partitionToLatestSegments =
              segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
          LLCSegmentName llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
          idealStateBuilder.removeSegment(llcSegmentName.getSegmentName());

          // get old state
          Assert.assertNull(idealState.getRecord().getMapFields().get(llcSegmentName.getSegmentName()));
          nPartitions = expectedPartitionAssignment.getNumPartitions();
          IdealState idealStateCopy = new IdealState(
              (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
          Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

          if (tooSoonToCorrect) {
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            // validate that all entries in oldMapFields are unchanged in new ideal state
            verifyNoChangeToOldEntries(oldMapFields, idealState);
            segmentManager.tooSoonToCorrect = false;
          }

          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

          // verify that new segment gets created in ideal state with CONSUMING
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(llcSegmentName.getSegmentName()));
          Assert.assertNotNull(idealState.getRecord()
              .getMapFields()
              .get(llcSegmentName.getSegmentName())
              .values()
              .contains("CONSUMING"));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, llcSegmentName.getSegmentName(), null));

          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        } else {
          // not the very first seq number in this partition i.e. prev metadata not null

          // generate the error scenario - segment missing from ideal state, but metadata IN_PROGRESS
          int randomlySelectedPartition = random.nextInt(nPartitions);
          Map<String, LLCSegmentName> partitionToLatestSegments =
              segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
          LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
          LLCRealtimeSegmentZKMetadata latestMetadata =
              segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null);
          segmentManager.updateOldSegmentMetadataZNRecord(tableName, latestSegment,
              latestMetadata.getStartOffset() + 100);
          LLCSegmentName newLlcSegmentName =
              new LLCSegmentName(rawTableName, randomlySelectedPartition, latestSegment.getSequenceNumber() + 1,
                  System.currentTimeMillis());
          CommittingSegmentDescriptor committingSegmentDescriptor =
              new CommittingSegmentDescriptor(latestSegment.getSegmentName(), latestMetadata.getStartOffset() + 100, 0);
          segmentManager.createNewSegmentMetadataZNRecord(tableConfig, latestSegment, newLlcSegmentName,
              expectedPartitionAssignment, committingSegmentDescriptor, false);

          // get old state
          Assert.assertNull(idealState.getRecord().getMapFields().get(newLlcSegmentName.getSegmentName()));
          nPartitions = expectedPartitionAssignment.getNumPartitions();
          IdealState idealStateCopy = new IdealState(
              (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
          Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

          if (tooSoonToCorrect) {
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            // validate nothing changed and try again with disabled
            verifyNoChangeToOldEntries(oldMapFields, idealState);
            segmentManager.tooSoonToCorrect = false;
          }

          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

          // verify that new segment gets created in ideal state with CONSUMING and old segment ONLINE
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(latestSegment.getSegmentName()).values().
              contains("ONLINE"));
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(newLlcSegmentName.getSegmentName()));
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(newLlcSegmentName.getSegmentName()).values().
              contains("CONSUMING"));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, newLlcSegmentName.getSegmentName(), null));

          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        }
      } else {

        // latest segment metadata is present in ideal state

        // segment is in OFFLINE state
        final boolean allReplicasInOffline = random.nextBoolean();

        if (allReplicasInOffline) {

          // generate error scenario - all replicas in OFFLINE state
          int randomlySelectedPartition = random.nextInt(nPartitions);
          Map<String, LLCSegmentName> partitionToLatestSegments =
              segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
          LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
          idealState = idealStateBuilder.setSegmentState(latestSegment.getSegmentName(), "OFFLINE").build();

          // get old state
          nPartitions = expectedPartitionAssignment.getNumPartitions();
          IdealState idealStateCopy = new IdealState(
              (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
          Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        } else {
          final boolean allReplicasInOnline = random.nextBoolean();
          if (allReplicasInOnline) {
            // race condition occurred between retentionManager and segment commit.
            // The metadata was marked to DONE, new metadata created IN_PROGRESS segment state ONLINE and new segment state CONSUMING.
            // Retention manager saw the new segment metadata before the ideal state was updated, and hence marked segment for deletion
            // After ideal state update, retention manager deleted the new segment from metadata and ideal state

            // generate error scenario
            int randomlySelectedPartition = random.nextInt(nPartitions);
            Map<String, LLCSegmentName> partitionToLatestSegments =
                segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
            LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
            LLCRealtimeSegmentZKMetadata latestMetadata =
                segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null);
            segmentManager.updateOldSegmentMetadataZNRecord(tableName, latestSegment,
                latestMetadata.getStartOffset() + 100);
            idealState = idealStateBuilder.setSegmentState(latestSegment.getSegmentName(), "ONLINE").build();

            // get old state
            nPartitions = expectedPartitionAssignment.getNumPartitions();
            IdealState idealStateCopy = new IdealState(
                (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
            Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

            if (tooSoonToCorrect) {
              segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
              // validate nothing changed and try again with disabled
              verifyNoChangeToOldEntries(oldMapFields, idealState);
              segmentManager.tooSoonToCorrect = false;
            }
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

            verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
          } else {
            // at least 1 replica of latest segments in ideal state is in CONSUMING state

            final boolean step2NotDone = random.nextBoolean();

            if (step2NotDone) {
              // failed after step 1 : hence metadata is DONE but segment is still CONSUMING

              // generate error scenario
              int randomlySelectedPartition = random.nextInt(nPartitions);
              Map<String, LLCSegmentName> partitionToLatestSegments =
                  segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
              LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
              LLCRealtimeSegmentZKMetadata latestMetadata =
                  segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null);
              segmentManager.updateOldSegmentMetadataZNRecord(tableName, latestSegment,
                  latestMetadata.getStartOffset() + 100);

              // get old state
              nPartitions = expectedPartitionAssignment.getNumPartitions();
              IdealState idealStateCopy = new IdealState(
                  (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
              Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

              if (tooSoonToCorrect) {
                segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
                // validate nothing changed and try again with disabled
                verifyNoChangeToOldEntries(oldMapFields, idealState);
                segmentManager.tooSoonToCorrect = false;
              }

              segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

              verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
            } else {
              // happy path

              // get old state
              nPartitions = expectedPartitionAssignment.getNumPartitions();
              IdealState idealStateCopy = new IdealState(
                  (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
              Map<String, Map<String, String>> oldMapFields = idealStateCopy.getRecord().getMapFields();

              segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);

              // verify that nothing changed
              verifyNoChangeToOldEntries(oldMapFields, idealState);

              verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
            }
          }
        }
      }

      // randomly change instances used, or increase partition count
      double randomExternalChange = random.nextDouble();
      if (randomExternalChange <= 0.2) { // introduce an external change for 20% runs
        ExternalChange externalChange = ExternalChange.values()[random.nextInt(ExternalChange.values().length)];
        int numInstances;
        switch (externalChange) {
          case N_INSTANCES_CHANGED:
            // randomly change instances and hence set a new expected partition assignment
            numInstances = numInstancesSet[random.nextInt(numInstancesSet.length)];
            instances = getInstanceList(numInstances);
            segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
            expectedPartitionAssignment =
                segmentManager._partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig,
                    nPartitions);
            break;
          case N_PARTITIONS_INCREASED:
            // randomly increase partitions and hence set a new expected partition assignment
            expectedPartitionAssignment =
                segmentManager._partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig,
                    nPartitions + 1);
            break;
          case N_INSTANCES_CHANGED_AND_PARTITIONS_INCREASED:
            // both changed and hence set a new expected partition assignment
            numInstances = numInstancesSet[random.nextInt(numInstancesSet.length)];
            instances = getInstanceList(numInstances);
            segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
            expectedPartitionAssignment =
                segmentManager._partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig,
                    nPartitions + 1);
            break;
        }
      }
    }
  }

  /**
   * Verifies that all entries in old ideal state are unchanged in the new ideal state
   * There could be new entries in the ideal state due to num partitions increase
   * @param oldMapFields
   * @param idealState
   */
  private void verifyNoChangeToOldEntries(Map<String, Map<String, String>> oldMapFields, IdealState idealState) {
    Map<String, Map<String, String>> newMapFields = idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> oldMapFieldsEntry : oldMapFields.entrySet()) {
      String oldSegment = oldMapFieldsEntry.getKey();
      Map<String, String> oldInstanceStateMap = oldMapFieldsEntry.getValue();
      Assert.assertTrue(newMapFields.containsKey(oldSegment));
      Assert.assertTrue(oldInstanceStateMap.equals(newMapFields.get(oldSegment)));
    }
  }

  private void verifyRepairs(TableConfig tableConfig, IdealState idealState,
      PartitionAssignment expectedPartitionAssignment, FakePinotLLCRealtimeSegmentManager segmentManager,
      Map<String, Map<String, String>> oldMapFields) {

    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // latest metadata for each partition
    Map<Integer, LLCRealtimeSegmentZKMetadata[]> latestMetadata =
        segmentManager.getLatestMetadata(tableConfig.getTableName());

    // latest segment in ideal state for each partition
    Map<String, LLCSegmentName> partitionToLatestSegments =
        segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);

    // Assert that we have a latest metadata and segment for each partition
    List<String> latestSegmentNames = new ArrayList<>();
    for (int p = 0; p < expectedPartitionAssignment.getNumPartitions(); p++) {
      LLCRealtimeSegmentZKMetadata[] llcRealtimeSegmentZKMetadata = latestMetadata.get(p);
      LLCSegmentName latestLlcSegment = partitionToLatestSegments.get(String.valueOf(p));
      Assert.assertNotNull(llcRealtimeSegmentZKMetadata);
      Assert.assertNotNull(llcRealtimeSegmentZKMetadata[0]);
      Assert.assertNotNull(latestLlcSegment);
      Assert.assertEquals(latestLlcSegment.getSegmentName(), llcRealtimeSegmentZKMetadata[0].getSegmentName());
      latestSegmentNames.add(latestLlcSegment.getSegmentName());
    }

    // if it is latest, ideal state should have state CONSUMING. metadata should have status IN_PROGRESS
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (latestSegmentNames.contains(segmentName)) {
        for (Map.Entry<String, String> instanceState : instanceStateMap.entrySet()) {
          Assert.assertEquals(instanceState.getValue(), "CONSUMING");
        }
        LLCRealtimeSegmentZKMetadata llcRealtimeSegmentZKMetadata = segmentManager._metadataMap.get(segmentName);
        Assert.assertEquals(llcRealtimeSegmentZKMetadata.getStatus(),
            CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      } else {
        for (Map.Entry<String, String> instanceState : instanceStateMap.entrySet()) {
          Assert.assertTrue(instanceState.getValue().equals("ONLINE") || instanceState.getValue().equals("OFFLINE"));
        }
      }
    }

    // newer partitions and new segments will follow new partition assignment.
    // older segments will follow old partition assignment
    Set<String> oldSegmentsSet = oldMapFields.keySet();
    Set<String> newSegmentsSet = mapFields.keySet();
    Set<String> difference = new HashSet<>(newSegmentsSet);
    difference.removeAll(oldSegmentsSet);
    for (String addedSegment : difference) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(addedSegment)) {
        LLCSegmentName segmentName = new LLCSegmentName(addedSegment);
        Set<String> actualInstances = mapFields.get(addedSegment).keySet();
        List<String> expectedInstances =
            expectedPartitionAssignment.getInstancesListForPartition(String.valueOf(segmentName.getPartitionId()));
        Assert.assertEquals(actualInstances.size(), expectedInstances.size());
        Assert.assertTrue(actualInstances.containsAll(expectedInstances));
      }
    }
    for (String segment : oldSegmentsSet) {
      Set<String> expectedInstances = oldMapFields.get(segment).keySet();
      Set<String> actualInstances = mapFields.get(segment).keySet();
      Assert.assertEquals(actualInstances.size(), expectedInstances.size());
      Assert.assertTrue(actualInstances.containsAll(expectedInstances));
    }
  }

  private IdealState clearAndSetupHappyPathIdealState(IdealStateBuilderUtil idealStateBuilder,
      FakePinotLLCRealtimeSegmentManager segmentManager, TableConfig tableConfig, int nPartitions) {
    IdealState idealState = idealStateBuilder.clear().build();
    segmentManager._metadataMap.clear();

    segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
    PartitionAssignment partitionAssignment =
        segmentManager._partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig,
            idealState);
    for (int p = 0; p < nPartitions; p++) {
      String segmentName = idealStateBuilder.getSegment(p, 0);
      advanceASeqForPartition(idealState, segmentManager, partitionAssignment, segmentName, p, 1, 100, tableConfig);
    }
    return idealStateBuilder.build();
  }

  private IdealState clearAndSetupInitialSegments(IdealStateBuilderUtil idealStateBuilder,
      FakePinotLLCRealtimeSegmentManager segmentManager, TableConfig tableConfig, int nPartitions) {
    IdealState idealState = idealStateBuilder.clear().build();
    segmentManager._metadataMap.clear();
    segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
    return idealStateBuilder.build();
  }

  @Test
  public void testPreExistingSegments() throws InvalidConfigException {
    LLCSegmentName existingSegmentName = new LLCSegmentName("someTable", 1, 31, 12355L);
    String[] existingSegs = {existingSegmentName.getSegmentName()};
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(Arrays.asList(existingSegs));

    final String rtTableName = "testPreExistingLLCSegments_REALTIME";

    TableConfig tableConfig = makeTableConfig(rtTableName, 3, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    segmentManager.addTableToStore(rtTableName, tableConfig, 8);
    IdealState idealState = PinotTableIdealStateBuilder.buildEmptyRealtimeIdealStateFor(rtTableName, 10);
    try {
      segmentManager.setupNewTable(tableConfig, idealState);
      Assert.fail("Did not get expected exception when setting up new table with existing segments in ");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  // Make sure that if we are either not leader or we are disconnected, we do not process metadata commit.
  @Test
  public void testCommittingSegmentIfDisconnected() throws InvalidConfigException {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);

    final String tableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    List<String> instances = getInstanceList(nInstances);
    SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    TableConfig tableConfig =
        makeTableConfig(tableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);

    segmentManager.addTableToStore(tableName, tableConfig, nPartitions);

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyRealtimeIdealStateFor(tableName, nReplicas);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager.setupNewTable(tableConfig, idealState);
    // Now commit the first segment of partition 6.
    final int committingPartition = 6;
    final long nextOffset = 3425666L;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        new LLCRealtimeSegmentZKMetadata(segmentManager._records.get(committingPartition));
    segmentManager._paths.clear();
    segmentManager._records.clear();
    segmentManager.IS_CONNECTED = false;
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    CommittingSegmentDescriptor committingSegmentDescriptor =
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    boolean status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertFalse(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 0);  // Idealstate not updated
    Assert.assertEquals(segmentManager._paths.size(), 0);   // propertystore not updated
    segmentManager.IS_CONNECTED = true;
    segmentManager.IS_LEADER = false;
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertFalse(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 0);  // Idealstate not updated
    Assert.assertEquals(segmentManager._paths.size(), 0);   // propertystore not updated
    segmentManager.IS_LEADER = true;
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertTrue(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 1);  // Idealstate updated
    Assert.assertEquals(segmentManager._paths.size(), 2);   // propertystore updated
  }

  /**
   * Method which tests segment completion
   * We do not care for partition increases in this method
   * Instances increasing will be accounted for
   */
  @Test
  public void testCommittingSegment() throws InvalidConfigException {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);

    final String rtTableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(rtTableName);
    int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 2;
    List<String> instances = getInstanceList(nInstances);
    SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    TableConfig tableConfig =
        makeTableConfig(rtTableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);

    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(rtTableName);
    IdealState idealState = idealStateBuilder.setNumReplicas(nReplicas).build();

    segmentManager.addTableToStore(rtTableName, tableConfig, nPartitions);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager.setupNewTable(tableConfig, idealState);
    PartitionAssignment partitionAssignment =
        segmentManager._partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, nPartitions);

    // Happy path: committing segment has states {CONSUMING,CONSUMING}
    int committingPartition = 6;
    long nextOffset = 2500;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        new LLCRealtimeSegmentZKMetadata(segmentManager._records.get(committingPartition));
    segmentManager._paths.clear();
    segmentManager._records.clear();
    Set<String> prevInstances = idealState.getInstanceSet(committingSegmentMetadata.getSegmentName());
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    CommittingSegmentDescriptor committingSegmentDescriptor =
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    boolean status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    segmentManager.verifyMetadataInteractions();
    Assert.assertTrue(status);
    Assert.assertEquals(segmentManager._paths.size(), 2);
    ZNRecord oldZnRec = segmentManager._records.get(0);
    ZNRecord newZnRec = segmentManager._records.get(1);
    testCommitSegmentEntries(segmentManager, committingSegmentMetadata, oldZnRec, newZnRec, prevInstances,
        partitionAssignment, committingPartition);

    // committing segment has states {OFFLINE, CONSUMING}
    String latestSegment = newZnRec.getId();
    Map<String, String> instanceStateMap = idealState.getInstanceStateMap(latestSegment);
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      entry.setValue("OFFLINE");
      break;
    }
    nextOffset = 5000;
    committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newZnRec);
    segmentManager._paths.clear();
    segmentManager._records.clear();
    prevInstances = idealState.getInstanceSet(committingSegmentMetadata.getSegmentName());
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    committingSegmentDescriptor = CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    segmentManager.verifyMetadataInteractions();
    Assert.assertTrue(status);
    Assert.assertEquals(segmentManager._paths.size(), 2);
    oldZnRec = segmentManager._records.get(0);
    newZnRec = segmentManager._records.get(1);
    testCommitSegmentEntries(segmentManager, committingSegmentMetadata, oldZnRec, newZnRec, prevInstances,
        partitionAssignment, committingPartition);

    // metadata for committing segment is already DONE
    nextOffset = 7500;
    segmentManager._metadataMap.get(newZnRec.getId()).setStatus(CommonConstants.Segment.Realtime.Status.DONE);

    committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newZnRec);
    segmentManager._paths.clear();
    segmentManager._records.clear();
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    committingSegmentDescriptor = CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    segmentManager.verifyMetadataInteractions();
    Assert.assertFalse(status);

    // failure after step 2: new metadata inprogress, no segment in ideal state
    segmentManager._metadataMap.get(newZnRec.getId()).setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    String deleteSegment = newZnRec.getId();
    Map<String, String> instanceStateMap1 = idealState.getInstanceStateMap(deleteSegment);
    idealState = idealStateBuilder.removeSegment(deleteSegment).setSegmentState(oldZnRec.getId(), "CONSUMING").build();
    nextOffset = 5000;
    committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(oldZnRec);
    segmentManager._paths.clear();
    segmentManager._records.clear();
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    committingSegmentDescriptor = CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    segmentManager.verifyMetadataInteractions();
    Assert.assertFalse(status);

    // instances changed
    idealStateBuilder.addSegment(deleteSegment, instanceStateMap1);
    nInstances = 8;
    instances = getInstanceList(nInstances);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager._paths.clear();
    segmentManager._records.clear();
    committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newZnRec);
    prevInstances = idealState.getInstanceSet(committingSegmentMetadata.getSegmentName());
    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    committingSegmentDescriptor = CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    segmentManager.verifyMetadataInteractions();
    Assert.assertTrue(status);
    Assert.assertEquals(segmentManager._paths.size(), 2);
    oldZnRec = segmentManager._records.get(0);
    newZnRec = segmentManager._records.get(1);
    testCommitSegmentEntries(segmentManager, committingSegmentMetadata, oldZnRec, newZnRec, prevInstances,
        partitionAssignment, committingPartition);
  }

  private void testCommitSegmentEntries(FakePinotLLCRealtimeSegmentManager segmentManager,
      LLCRealtimeSegmentZKMetadata committingSegmentMetadata, ZNRecord oldZnRec, ZNRecord newZnRec,
      Set<String> prevInstances, PartitionAssignment partitionAssignment, int partition) {
    // Get the old and new segment metadata and make sure that they are correct.

    LLCRealtimeSegmentZKMetadata oldMetadata = new LLCRealtimeSegmentZKMetadata(oldZnRec);
    LLCRealtimeSegmentZKMetadata newMetadata = new LLCRealtimeSegmentZKMetadata(newZnRec);

    LLCSegmentName oldSegmentName = new LLCSegmentName(oldMetadata.getSegmentName());
    LLCSegmentName newSegmentName = new LLCSegmentName(newMetadata.getSegmentName());

    // Assert on propertystore entries
    Assert.assertEquals(oldSegmentName.getSegmentName(), committingSegmentMetadata.getSegmentName());
    Assert.assertEquals(newSegmentName.getPartitionId(), oldSegmentName.getPartitionId());
    Assert.assertEquals(newSegmentName.getSequenceNumber(), oldSegmentName.getSequenceNumber() + 1);
    Assert.assertEquals(oldMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    Assert.assertEquals(newMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    Assert.assertNotNull(oldMetadata.getDownloadUrl());
    Assert.assertEquals(Long.valueOf(oldMetadata.getCrc()), Long.valueOf(FakePinotLLCRealtimeSegmentManager.CRC));
    Assert.assertEquals(oldMetadata.getStartTime(), FakePinotLLCRealtimeSegmentManager.INTERVAL.getStartMillis());
    Assert.assertEquals(oldMetadata.getEndTime(), FakePinotLLCRealtimeSegmentManager.INTERVAL.getEndMillis());
    Assert.assertEquals(oldMetadata.getTotalRawDocs(), FakePinotLLCRealtimeSegmentManager.NUM_DOCS);
    Assert.assertEquals(oldMetadata.getIndexVersion(), FakePinotLLCRealtimeSegmentManager.SEGMENT_VERSION);

    // check ideal state
    IdealState idealState = segmentManager._tableIdealState;
    Set<String> currInstances = idealState.getInstanceSet(oldSegmentName.getSegmentName());
    Assert.assertEquals(prevInstances.size(), currInstances.size());
    Assert.assertTrue(prevInstances.containsAll(currInstances));
    Set<String> newSegmentInstances = idealState.getInstanceSet(newSegmentName.getSegmentName());
    List<String> expectedNewInstances = partitionAssignment.getInstancesListForPartition(String.valueOf(partition));
    Assert.assertEquals(newSegmentInstances.size(), expectedNewInstances.size());
    Assert.assertTrue(newSegmentInstances.containsAll(expectedNewInstances));
  }

  @Test
  public void testCommitSegmentWhenControllerWentThroughGC() throws InvalidConfigException {

    FakePinotLLCRealtimeSegmentManager segmentManager1 = new FakePinotLLCRealtimeSegmentManager(null);
    FakePinotLLCRealtimeSegmentManager segmentManager2 = new FakePinotLLCRealtimeSegmentManagerII(null,
        FakePinotLLCRealtimeSegmentManagerII.SCENARIO_1_ZK_VERSION_NUM_HAS_CHANGE);
    FakePinotLLCRealtimeSegmentManager segmentManager3 = new FakePinotLLCRealtimeSegmentManagerII(null,
        FakePinotLLCRealtimeSegmentManagerII.SCENARIO_2_METADATA_STATUS_HAS_CHANGE);

    final String rtTableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(rtTableName);
    setupSegmentManager(segmentManager1, rtTableName);
    setupSegmentManager(segmentManager2, rtTableName);
    setupSegmentManager(segmentManager3, rtTableName);
    // Now commit the first segment of partition 6.
    final int committingPartition = 6;
    final long nextOffset = 3425666L;
    SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        new LLCRealtimeSegmentZKMetadata(segmentManager2._records.get(committingPartition));

    reqParams.withSegmentName(committingSegmentMetadata.getSegmentName()).withOffset(nextOffset);
    CommittingSegmentDescriptor committingSegmentDescriptor =
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
    boolean status = segmentManager1.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertTrue(status);  // Committing segment metadata succeeded.

    status = segmentManager2.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertFalse(status); // Committing segment metadata failed.

    status = segmentManager3.commitSegmentMetadata(rawTableName, committingSegmentDescriptor);
    Assert.assertFalse(status); // Committing segment metadata failed.
  }

  private void setupSegmentManager(FakePinotLLCRealtimeSegmentManager segmentManager, String rtTableName)
      throws InvalidConfigException {
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    List<String> instances = getInstanceList(nInstances);
    TableConfig tableConfig =
        makeTableConfig(rtTableName, nReplicas, DUMMY_HOST, DEFAULT_SERVER_TENANT);
    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyRealtimeIdealStateFor(rtTableName, nReplicas);

    segmentManager.addTableToStore(rtTableName, tableConfig, nPartitions);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager.setupNewTable(tableConfig, idealState);
  }

  @Test
  public void testCommitSegmentFile() throws Exception {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(Collections.<String>emptyList());
    String tableName = "fakeTable_REALTIME";
    String segmentName = "segment";
    String temporarySegmentLocation = SegmentCompletionUtils.generateSegmentFileName(segmentName);

    File temporaryDirectory = new File(baseDir, tableName);
    temporaryDirectory.mkdirs();
    String segmentLocation = SCHEME + temporaryDirectory.toString() + "/" + temporarySegmentLocation;

    FileUtils.write(new File(temporaryDirectory, temporarySegmentLocation), "temporary file contents");
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, 100, 0, segmentLocation);
    Assert.assertTrue(realtimeSegmentManager.commitSegmentFile(tableName, committingSegmentDescriptor));
  }

  @Test
  public void testSegmentAlreadyThereAndExtraneousFilesDeleted() throws Exception {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(Collections.<String>emptyList());
    String tableName = "fakeTable_REALTIME";
    String segmentName = "segment";

    File temporaryDirectory = new File(baseDir, tableName);
    temporaryDirectory.mkdirs();

    String segmentFileLocation = SegmentCompletionUtils.generateSegmentFileName(segmentName);
    String segmentLocation = SCHEME + temporaryDirectory + "/" + segmentFileLocation;
    String extraSegmentFileLocation = SegmentCompletionUtils.generateSegmentFileName(segmentName);
    String extraSegmentLocation = SCHEME + temporaryDirectory + "/" + extraSegmentFileLocation;
    String otherSegmentNotToBeDeleted = "segmentShouldStay";
    FileUtils.write(new File(temporaryDirectory, segmentFileLocation), "temporary file contents");
    FileUtils.write(new File(temporaryDirectory, extraSegmentFileLocation), "temporary file contents");
    FileUtils.write(new File(temporaryDirectory, otherSegmentNotToBeDeleted), "temporary file contents");
    FileUtils.write(new File(temporaryDirectory, segmentName), "temporary file contents");
    Assert.assertTrue(realtimeSegmentManager.commitSegmentFile(tableName,
        new CommittingSegmentDescriptor(segmentName, 100, 0, segmentLocation)));
    Assert.assertTrue(new File(temporaryDirectory, otherSegmentNotToBeDeleted).exists());
    Assert.assertFalse(new File(temporaryDirectory, extraSegmentLocation).exists());
  }

  ////////////////////////////////////////////////////////////////////////////
  // Fake makers
  ///////////////////////////////////////////////////////////////////////////

  private TableConfig makeTableConfig(String tableName, int nReplicas, String bootstrapHosts,
      String serverTenant) {
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartition()).thenReturn(Integer.toString(nReplicas));
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(nReplicas);
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    Map<String, String> streamConfigMap = new HashMap<>(1);
    String streamType = "kafka";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    String topic = "aTopic";
    String consumerFactoryClass = KafkaConsumerFactory.class.getName();
    String decoderClass = KafkaAvroMessageDecoder.class.getName();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "100000");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        "simple");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    final String bootstrapHostConfigKey = KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST);
    streamConfigMap.put(bootstrapHostConfigKey, bootstrapHosts);

    IndexingConfig mockIndexConfig = mock(IndexingConfig.class);
    when(mockIndexConfig.getStreamConfigs()).thenReturn(streamConfigMap);

    when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexConfig);
    TenantConfig mockTenantConfig = mock(TenantConfig.class);
    when(mockTenantConfig.getServer()).thenReturn(serverTenant);
    when(mockTableConfig.getTenantConfig()).thenReturn(mockTenantConfig);

    return mockTableConfig;
  }

  private static Map<String, String> getStreamConfigs() {
    Map<String, String> streamPropMap = new HashMap<>(1);
    String streamType = "kafka";
    streamPropMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    String topic = "aTopic";
    streamPropMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamPropMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        "simple");
    streamPropMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    streamPropMap.put(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), "host:1234");
    return streamPropMap;
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Fake classes
  /////////////////////////////////////////////////////////////////////////////////

  static class FakePinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {

    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    private List<String> _existingLLCSegments = new ArrayList<>(1);

    public List<String> _paths = new ArrayList<>(16);
    public List<ZNRecord> _records = new ArrayList<>(16);
    public Map<String, LLCRealtimeSegmentZKMetadata> _metadataMap = new HashMap<>(4);
    private FakeStreamPartitionAssignmentGenerator _partitionAssignmentGenerator;

    public static final long _largestOffsetToReturn = Integer.MAX_VALUE;
    public static final long _smallestOffsetToReturn = 0;

    public int _nCallsToUpdateHelix = 0;
    public int _nCallsToCreateNewSegmentMetadata = 0;
    public IdealState _tableIdealState;
    public String _currentTable;

    public static final String CRC = "5680988776500";
    public static final Interval INTERVAL = new Interval(3000, 4000);
    public static final String SEGMENT_VERSION = SegmentVersion.v1.toString();
    public static final int NUM_DOCS = 5099775;
    public static boolean IS_LEADER = true;
    public static boolean IS_CONNECTED = true;
    public boolean tooSoonToCorrect = false;

    public int _version;

    private SegmentMetadataImpl segmentMetadata;

    private TableConfigStore _tableConfigStore;

    protected FakePinotLLCRealtimeSegmentManager(List<String> existingLLCSegments) {

      super(null, clusterName, null, null, null, CONTROLLER_CONF, new ControllerMetrics(new MetricsRegistry()));
      try {
        TableConfigCache mockCache = mock(TableConfigCache.class);
        TableConfig mockTableConfig = mock(TableConfig.class);
        IndexingConfig mockIndexingConfig = mock(IndexingConfig.class);
        when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexingConfig);
        when(mockIndexingConfig.getStreamConfigs()).thenReturn(getStreamConfigs());
        when(mockCache.getTableConfig(anyString())).thenReturn(mockTableConfig);

        Field tableConfigCacheField = PinotLLCRealtimeSegmentManager.class.getDeclaredField("_tableConfigCache");
        tableConfigCacheField.setAccessible(true);
        tableConfigCacheField.set(this, mockCache);

        HelixManager mockHelixManager = mock(HelixManager.class);
        _partitionAssignmentGenerator = new FakeStreamPartitionAssignmentGenerator(mockHelixManager);
        Field partitionAssignmentGeneratorField =
            PinotLLCRealtimeSegmentManager.class.getDeclaredField("_streamPartitionAssignmentGenerator");
        partitionAssignmentGeneratorField.setAccessible(true);
        partitionAssignmentGeneratorField.set(this, _partitionAssignmentGenerator);
      } catch (Exception e) {
        Utils.rethrowException(e);
      }

      if (existingLLCSegments != null) {
        _existingLLCSegments = existingLLCSegments;
      }
      CONTROLLER_CONF.setControllerVipHost("vip");
      CONTROLLER_CONF.setControllerPort("9000");
      CONTROLLER_CONF.setDataDir(baseDir.toString());

      _version = 0;

      _tableConfigStore = new TableConfigStore();
    }

    void addTableToStore(String tableName, TableConfig tableConfig, int nStreamPartitions) {
      _tableConfigStore.addTable(tableName, tableConfig, nStreamPartitions);
    }

    void removeTableFromStore(String tableName) {
      _tableConfigStore.removeTable(tableName);
    }

    @Override
    protected TableConfig getRealtimeTableConfig(String realtimeTableName) {
      return _tableConfigStore.getTableConfig(realtimeTableName);
    }

    @Override
    protected boolean isTooSoonToCorrect(String tableNameWithType, String segmentId, long now) {
      return tooSoonToCorrect;
    }

    @Override
    protected boolean writeSegmentToPropertyStore(String nodePath, ZNRecord znRecord, final String realtimeTableName,
        int expectedVersion) {
      List<String> paths = new ArrayList<>();
      List<ZNRecord> records = new ArrayList<>();
      paths.add(nodePath);
      records.add(znRecord);
      if (expectedVersion != _version) {
        return false;
      } else {
        writeSegmentsToPropertyStore(paths, records, realtimeTableName);
        return true;
      }
    }

    @Override
    protected boolean writeSegmentToPropertyStore(String nodePath, ZNRecord znRecord, final String realtimeTableName) {
      List<String> paths = new ArrayList<>();
      List<ZNRecord> records = new ArrayList<>();
      paths.add(nodePath);
      records.add(znRecord);
      writeSegmentsToPropertyStore(paths, records, realtimeTableName);
      return true;
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records, String realtimeTableName) {
      _paths.addAll(paths);
      _records.addAll(records);
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        ZNRecord znRecord = records.get(i);
        String segmentId = getSegmentNameFromPath(path);
        _existingLLCSegments.add(segmentId);
        _metadataMap.put(segmentId, new LLCRealtimeSegmentZKMetadata(znRecord));
      }
    }

    private String getSegmentNameFromPath(String path) {
      return path.substring(path.lastIndexOf("/") + 1);
    }

    @Override
    protected List<String> getAllSegments(String realtimeTableName) {
      return Lists.newArrayList(_metadataMap.keySet());
    }

    @Override
    protected LLCRealtimeSegmentZKMetadata getSegmentMetadata(String realtimeTableName, String segmentName) {
      return _metadataMap.get(segmentName);
    }

    @Override
    protected List<String> getExistingSegments(String realtimeTableName) {
      return _existingLLCSegments;
    }

    @Override
    protected void updateIdealStateOnSegmentCompletion(@Nonnull final String tableNameWithType,
        @Nonnull final String currentSegmentId, @Nonnull final String newSegmentId,
        @Nonnull final PartitionAssignment partitionAssignment) {
      _nCallsToUpdateHelix++;
      super.updateIdealStateOnSegmentCompletion(_tableIdealState, currentSegmentId, newSegmentId, partitionAssignment);
    }

    @Override
    protected boolean createNewSegmentMetadataZNRecord(TableConfig realtimeTableConfig,
        LLCSegmentName committingSegmentName, LLCSegmentName newLLCSegmentName, PartitionAssignment partitionAssignment,
        CommittingSegmentDescriptor committingSegmentDescriptor, boolean isNewTableSetup) {
      _nCallsToCreateNewSegmentMetadata++;
      return super.createNewSegmentMetadataZNRecord(realtimeTableConfig, committingSegmentName, newLLCSegmentName,
          partitionAssignment, committingSegmentDescriptor, isNewTableSetup);
    }

    @Override
    protected boolean updateOldSegmentMetadataZNRecord(String realtimeTableName,
        LLCSegmentName committingLLCSegmentName, long nextOffset) {
      return super.updateOldSegmentMetadataZNRecord(realtimeTableName, committingLLCSegmentName, nextOffset);
    }

    @Override
    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName,
        Stat stat) {
      if (_metadataMap.containsKey(segmentName)) {
        LLCRealtimeSegmentZKMetadata oldMetadata = _metadataMap.get(segmentName);

        LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
        metadata.setSegmentName(oldMetadata.getSegmentName());
        metadata.setDownloadUrl(oldMetadata.getDownloadUrl());
        metadata.setNumReplicas(oldMetadata.getNumReplicas());
        metadata.setEndOffset(oldMetadata.getEndOffset());
        metadata.setStatus(oldMetadata.getStatus());
        return metadata;
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
      verify(segmentMetadata, times(1)).getColumnMetadataMap();
      verifyNoMoreInteractions(segmentMetadata);
    }

    @Override
    protected long getPartitionOffset(StreamConfig streamConfig, final OffsetCriteria offsetCriteria,
        int partitionId) {
      if (offsetCriteria.isLargest()) {
        return _largestOffsetToReturn;
      } else {
        return _smallestOffsetToReturn;
      }
    }

    // package-private
    long getSmallestStreamOffset() {
      return _smallestOffsetToReturn;
    }

    //package-private
    long getLargestStreamOffset() {
      return _largestOffsetToReturn;
    }

    @Override
    protected boolean isLeader() {
      return IS_LEADER;
    }

    @Override
    protected boolean isConnected() {
      return IS_CONNECTED;
    }

    @Override
    protected IdealState getTableIdealState(String realtimeTableName) {
      return _tableIdealState;
    }

    @Override
    protected void setTableIdealState(String realtimeTableName, IdealState idealState) {
      _tableIdealState = idealState;
    }

    @Override
    public void setupNewTable(TableConfig tableConfig, IdealState emptyIdealState) throws InvalidConfigException {
      _currentTable = tableConfig.getTableName();
      super.setupNewTable(tableConfig, emptyIdealState);
    }

    @Override
    protected int getPartitionCount(StreamConfig metadata) {
      return _tableConfigStore.getNStreamPartitions(_currentTable);
    }
  }

  static class FakePinotLLCRealtimeSegmentManagerII extends FakePinotLLCRealtimeSegmentManager {

    final static int SCENARIO_1_ZK_VERSION_NUM_HAS_CHANGE = 1;
    final static int SCENARIO_2_METADATA_STATUS_HAS_CHANGE = 2;

    private int _scenario;

    FakePinotLLCRealtimeSegmentManagerII(List<String> existingLLCSegments, int scenario) {
      super(existingLLCSegments);
      _scenario = scenario;
    }

    @Override
    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName,
        Stat stat) {
      LLCRealtimeSegmentZKMetadata metadata = super.getRealtimeSegmentZKMetadata(realtimeTableName, segmentName, stat);
      switch (_scenario) {
        case SCENARIO_1_ZK_VERSION_NUM_HAS_CHANGE:
          // Mock another controller has already updated the segment metadata, which makes the version number self increase.
          stat.setVersion(_version + 1);
          break;
        case SCENARIO_2_METADATA_STATUS_HAS_CHANGE:
          // Mock another controller has updated the status of the old segment metadata.
          metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
          break;
      }
      return metadata;
    }
  }

  private static class FakeStreamPartitionAssignmentGenerator extends StreamPartitionAssignmentGenerator {

    private List<String> _consumingInstances;

    public FakeStreamPartitionAssignmentGenerator(HelixManager helixManager) {
      super(helixManager);
      _consumingInstances = new ArrayList<>();
    }

    @Override
    protected List<String> getConsumingTaggedInstances(TableConfig tableConfig) {
      return _consumingInstances;
    }

    void setConsumingInstances(List<String> consumingInstances) {
      _consumingInstances = consumingInstances;
    }
  }

  static class TableConfigStore {
    private Map<String, TableConfig> _tableConfigsStore;
    private Map<String, Integer> _nPartitionsStore;

    TableConfigStore() {
      _tableConfigsStore = new HashMap<>(1);
      _nPartitionsStore = new HashMap<>(1);
    }

    void addTable(String tableName, TableConfig tableConfig, int nStreamPartitions) {
      _tableConfigsStore.put(tableName, tableConfig);
      _nPartitionsStore.put(tableName, nStreamPartitions);
    }

    void removeTable(String tableName) {
      _tableConfigsStore.remove(tableName);
      _nPartitionsStore.remove(tableName);
    }

    TableConfig getTableConfig(String tableName) {
      return _tableConfigsStore.get(tableName);
    }

    int getNStreamPartitions(String tableName) {
      return _nPartitionsStore.get(tableName);
    }

    List<String> getAllRealtimeTablesWithServerTenant(String serverTenant) {
      List<String> realtimeTablesWithServerTenant = new ArrayList<>();
      for (Map.Entry<String, TableConfig> entry : _tableConfigsStore.entrySet()) {
        if (entry.getValue().getTenantConfig().getServer().equals(serverTenant)) {
          realtimeTablesWithServerTenant.add(entry.getKey());
        }
      }
      return realtimeTablesWithServerTenant;
    }
  }
}
