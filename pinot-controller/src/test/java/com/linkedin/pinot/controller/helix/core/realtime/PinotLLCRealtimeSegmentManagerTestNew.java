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

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.io.Files;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.StreamConsumptionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.partition.IdealStateBuilderUtil;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.PartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.resources.LLCSegmentCompletionHandlers;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.realtime.partition.StreamPartitionAssignmentStrategyEnum;
import com.linkedin.pinot.controller.util.SegmentCompletionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
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
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.data.Stat;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class PinotLLCRealtimeSegmentManagerTestNew {
  private static final String clusterName = "testCluster";
  private static final String DUMMY_HOST = "dummyHost:1234";
  private static final String KAFKA_OFFSET = "testDummy";
  private static final String DEFAULT_SERVER_TENANT = "freeTenant";
  private static final StreamPartitionAssignmentStrategyEnum DEFAULT_STREAM_ASSIGNMENT_STRATEGY = null;
  private static final String SCHEME = LLCSegmentCompletionHandlers.getScheme();
  private String[] serverNames;
  private static File baseDir;

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

  @Test
  public void testValidateLLC() throws InvalidConfigException {
    // New table case
    testValidateLLCNewTable();

    // Partition count increase
    testValidateLLCPartitionIncrease();

    // Repair errors in segment commit
    testValidateLLCRepair();
  }

  /**
   * Method which tests segment completion
   * We do not care for partition increases in this method
   * Instances increasing will be accounted for
   */
  @Test
  public void testSegmentCompletion() throws InvalidConfigException {

    String tableName = "testSegmentCompletion_REALTIME";
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int nReplicas = 2;
    TableConfig tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    List<String> instances = getInstanceList(6);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);
    IdealState idealState;
    String currentSegmentName = "currentSegment";
    String newSegmentName = "newSegment";
    PartitionAssignment partitionAssignment = new PartitionAssignment(tableName);
    int nPartitions = 4;
    boolean exceptionExpected = true;
    Random random = new Random();

    // empty ideal state
    idealState = idealStateBuilder.build();
    try {
      segmentManager.updateIdealStateOnSegmentCompletion(idealState, currentSegmentName, newSegmentName,
          partitionAssignment);
    } catch (NullPointerException e) {
      Assert.assertTrue(exceptionExpected);
    }

    // empty partition assignment
    clearAndSetupInitialSegments(idealStateBuilder, segmentManager, tableConfig, nPartitions);
    try {
      segmentManager.updateIdealStateOnSegmentCompletion(idealState, currentSegmentName, newSegmentName,
          partitionAssignment);
    } catch (NullPointerException e) {
      Assert.assertTrue(exceptionExpected);
    }

    partitionAssignment =
        segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions);

    // current segment in CONSUMING
    int randomPartition = random.nextInt(nPartitions);
    Map<String, LLCSegmentName> partitionToLatestSegments =
        segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    LLCSegmentName llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomPartition));
    LLCSegmentName newLlcSegmentName =
        new LLCSegmentName(rawTableName, randomPartition, llcSegmentName.getSequenceNumber() + 1,
            System.currentTimeMillis());
    verifySegmentAssignmentOnCompletion(idealState, llcSegmentName.getSegmentName(), newLlcSegmentName.getSegmentName(),
        randomPartition, partitionAssignment, segmentManager);

    // current segment in OFFLINE
    randomPartition = random.nextInt(nPartitions);
    partitionToLatestSegments = segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomPartition));
    idealState = idealStateBuilder.transitionToState(llcSegmentName.getSegmentName(), "OFFLINE").build();
    newLlcSegmentName = new LLCSegmentName(rawTableName, randomPartition, llcSegmentName.getSequenceNumber() + 1,
        System.currentTimeMillis());
    verifySegmentAssignmentOnCompletion(idealState, llcSegmentName.getSegmentName(), newLlcSegmentName.getSegmentName(),
        randomPartition, partitionAssignment, segmentManager);

    // current segment in ONLINE
    randomPartition = random.nextInt(nPartitions);
    partitionToLatestSegments = segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomPartition));
    idealState = idealStateBuilder.transitionToState(llcSegmentName.getSegmentName(), "ONLINE").build();
    newLlcSegmentName = new LLCSegmentName(rawTableName, randomPartition, llcSegmentName.getSequenceNumber() + 1,
        System.currentTimeMillis());
    verifySegmentAssignmentOnCompletion(idealState, llcSegmentName.getSegmentName(), newLlcSegmentName.getSegmentName(),
        randomPartition, partitionAssignment, segmentManager);

    // partition assignment changed, not same as ideal state
    instances = getInstanceList(8);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    partitionAssignment =
        segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions);
    randomPartition = random.nextInt(nPartitions);
    partitionToLatestSegments = segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomPartition));
    newLlcSegmentName = new LLCSegmentName(rawTableName, randomPartition, llcSegmentName.getSequenceNumber() + 1,
        System.currentTimeMillis());
    verifySegmentAssignmentOnCompletion(idealState, llcSegmentName.getSegmentName(), newLlcSegmentName.getSegmentName(),
        randomPartition, partitionAssignment, segmentManager);
  }

  private void verifySegmentAssignmentOnCompletion(IdealState idealState, String currentSegmentName,
      String newSegmentName, int partition, PartitionAssignment partitionAssignment,
      FakePinotLLCRealtimeSegmentManager segmentManager) {

    Set<String> prevInstances = idealState.getInstanceSet(currentSegmentName);
    segmentManager.updateIdealStateOnSegmentCompletion(idealState, currentSegmentName, newSegmentName,
        partitionAssignment);
    Set<String> currInstances = idealState.getInstanceSet(currentSegmentName);
    Assert.assertEquals(prevInstances.size(), currInstances.size());
    Assert.assertTrue(prevInstances.containsAll(currInstances));
    Set<String> newSegmentInstances = idealState.getInstanceSet(newSegmentName);
    List<String> expectedNewInstances = partitionAssignment.getInstancesListForPartition(String.valueOf(partition));
    Assert.assertEquals(newSegmentInstances.size(), expectedNewInstances.size());
    Assert.assertTrue(newSegmentInstances.containsAll(expectedNewInstances));
  }

  /**
   * Test cases for new table being created, and initial segments setup that follows
   */
  private void testValidateLLCNewTable() {
    String tableName = "validateThisTable_REALTIME";
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    boolean exceptionExpected;
    List<String> instances = getInstanceList(1);

    // noop path - insufficient instances
    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    exceptionExpected = true;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    // bad stream configs
    tableConfig =
        makeTableConfig(tableName, nReplicas, null, null, DEFAULT_SERVER_TENANT, DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    instances = getInstanceList(3);
    exceptionExpected = true;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    // noop path - 0 partitions
    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.build();
    nPartitions = 0;
    exceptionExpected = false;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    // noop path - ideal state disabled
    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.disableIdealState().build();
    nPartitions = 4;
    exceptionExpected = false;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    // happy paths - new table config with nPartitions and sufficient instances
    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    exceptionExpected = false;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    idealState = idealStateBuilder.build();
    nPartitions = 8;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    idealState = idealStateBuilder.build();
    nPartitions = 8;
    instances = getInstanceList(10);
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);

    idealState = idealStateBuilder.build();
    nPartitions = 12;
    validateLLCNewTable(tableConfig, idealState, nPartitions, nReplicas, instances, exceptionExpected);
  }

  private IdealState validateLLCNewTable(TableConfig tableConfig, IdealState idealState, int nPartitions, int nReplicas,
      List<String> instances, boolean exceptionExpected) {
    IdealState updatedIdealState = null;
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    try {
      updatedIdealState = segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
      Assert.assertFalse(exceptionExpected);
      Map<String, LLCSegmentName> partitionToLatestSegments =
          segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(updatedIdealState);
      Map<String, Map<String, String>> mapFields = updatedIdealState.getRecord().getMapFields();
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
          Assert.assertEquals(metadata.getStartOffset(), -1L);
          Assert.assertEquals(path, "/SEGMENTS/" + tableConfig.getTableName() + "/" + segmentName);
          LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
          Assert.assertEquals(llcSegmentName.getPartitionId(), partition);
          Assert.assertEquals(llcSegmentName.getTableName(), TableNameBuilder.extractRawTableName(tableConfig.getTableName()));
          Assert.assertEquals(metadata.getNumReplicas(), nReplicas);
        }
      }
    } catch (Exception e) {
      Assert.assertTrue(exceptionExpected);
    }
    return updatedIdealState;
  }

  /**
   * Test cases for the scenario where stream partitions increase, and the validation manager is attempting to create segments for new partitions
   * This test assumes that all other factors remain same (no error conditions/inconsistencies in metadata and ideal state)
   */
  private void testValidateLLCPartitionIncrease() {

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    String tableName = "validateThisTable_REALTIME";
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    boolean exceptionExpected;
    List<String> instances = getInstanceList(5);

    // empty to 4 partitions
    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    idealState = idealStateBuilder.build();
    nPartitions = 4;
    exceptionExpected = false;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // only initial segments present CONSUMING - metadata INPROGRESS - increase numPartitions
    nPartitions = 6;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // 2 partitions advanced a seq number
    PartitionAssignment partitionAssignment =
        segmentManager._partitionAssignmentGenerator.getPartitionAssignmentFromIdealState(tableConfig, idealState);
    for (int p = 0; p < 2; p++) {
      String segmentName = idealStateBuilder.getSegment(p, 0);
      advanceASeqForPartition(idealState, segmentManager, partitionAssignment, segmentName, p, 1, 100, tableName);
    }
    idealState = idealStateBuilder.build();
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // increase num partitions
    nPartitions = 10;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // keep num partitions same - noop
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // keep num partitions same, but bad instances - error
    instances = getInstanceList(1);
    exceptionExpected = true;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // increase num partitions, but bad instances - error
    nPartitions = 12;
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);

    // increase num partitions, but disabled ideal state - noop
    idealState = idealStateBuilder.disableIdealState().build();
    exceptionExpected = false;
    instances = getInstanceList(6);
    validateLLCPartitionsIncrease(segmentManager, idealState, tableConfig, nPartitions, nReplicas, instances,
        exceptionExpected);
  }

  private void advanceASeqForPartition(IdealState idealState, FakePinotLLCRealtimeSegmentManager segmentManager,
      PartitionAssignment partitionAssignment, String segmentName, int partition, int nextSeqNum, long nextOffset,
      String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    segmentManager.updateOldSegmentMetadataZNRecord(tableName, llcSegmentName, nextOffset);
    LLCSegmentName newLlcSegmentName =
        new LLCSegmentName(rawTableName, partition, nextSeqNum, System.currentTimeMillis());
    segmentManager.createNewSegmentMetadataZNRecord(tableName, newLlcSegmentName, nextOffset, partitionAssignment);
    segmentManager.updateIdealStateOnSegmentCompletion(idealState, segmentName, newLlcSegmentName.getSegmentName(),
        partitionAssignment);
  }

  private void validateLLCPartitionsIncrease(FakePinotLLCRealtimeSegmentManager segmentManager, IdealState idealState,
      TableConfig tableConfig, int nPartitions, int nReplicas, List<String> instances, boolean exceptionExpected) {
    try {
      Map<String, Map<String, String>> oldMapFields = new HashMap<>(idealState.getRecord().getMapFields());
      Map<String, LLCRealtimeSegmentZKMetadata> oldMetadataMap = new HashMap<>(segmentManager._metadataMap);
      segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
      IdealState updatedIdealState = segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
      Assert.assertFalse(exceptionExpected);
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
      // each new partition should have exactly 1 new segment in CONSUMING state, and metadata in IN_PROGRESS state
      for (Integer partitionId : newPartitions) {
        List<String> allSegmentsForPartition = partitionToAllSegmentsMap.get(partitionId);
        if (idealState.isEnabled()) {
          Assert.assertEquals(allSegmentsForPartition.size(), 1);
          for (String segment : allSegmentsForPartition) {
            Map<String, String> instanceStateMap = updatedMapFields.get(segment);
            Assert.assertEquals(instanceStateMap.size(), nReplicas);
            for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
              Assert.assertEquals(entry.getValue(), "CONSUMING");
            }
            LLCRealtimeSegmentZKMetadata newPartitionMetadata = updatedMetadataMap.get(segment);
            Assert.assertNotNull(newPartitionMetadata);
            Assert.assertEquals(newPartitionMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
          }
        } else {
          Assert.assertEquals(allSegmentsForPartition.size(), 0);
        }
      }
    } catch (Exception e) {
      Assert.assertTrue(exceptionExpected);
    }
  }

  private void testValidateLLCRepair() throws InvalidConfigException {

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);
    String tableName = "repairThisTable_REALTIME";
    int nReplicas = 2;
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    TableConfig tableConfig;
    IdealState idealState;
    int nPartitions;
    List<String> instances = getInstanceList(5);

    tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    nPartitions = 4;
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    PartitionAssignment expectedPartitionAssignment =
        segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions);

    // set up a happy path - segment is present in ideal state, is CONSUMING, metadata says IN_PROGRESS
    idealState = clearAndSetupHappyPathIdealState(idealStateBuilder, segmentManager, tableConfig, nPartitions);

    // randomly introduce an error condition and assert that we repair it
    long seed = new Random().nextLong();
    System.out.println("Random seed for validate llc repairs " + seed);
    Random random = new Random(seed);

    for (int run = 0; run < 200; run++) {

      boolean consuming = true;
      boolean inProgress = true;
      if (random.nextBoolean()) {
        segmentManager.tooSoonToCorrect = true;
      }
      if (!random.nextBoolean()) { // not present in ideal state
        // 1. very first metadata IN_PROGRESS, but segment missing - repair: create segment CONSUMING
        // 2. seq number matured, latest metadata IN_PROGRESS, but segment missing - repair: create segment CONSUMING, prev segment transition to ONLINE
        if (random.nextBoolean()) {
          // 1. very first seq number of this partition
          clearAndSetupInitialSegments(idealStateBuilder, segmentManager, tableConfig, nPartitions);

          int randomlySelectedPartition = random.nextInt(nPartitions);
          Map<String, LLCSegmentName> partitionToLatestSegments =
              segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
          LLCSegmentName llcSegmentName = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
          idealStateBuilder.removeSegment(llcSegmentName.getSegmentName());

          Assert.assertNull(idealState.getRecord().getMapFields().get(llcSegmentName.getSegmentName()));
          nPartitions = expectedPartitionAssignment.getNumPartitions();
          Map<String, Map<String, String>> oldMapFields = idealState.getRecord().getMapFields();
          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          if (segmentManager.tooSoonToCorrect) {
            // validate nothing changed and try again with disabled
            Assert.assertEquals(oldMapFields, idealState.getRecord().getMapFields());
            segmentManager.tooSoonToCorrect = false;
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          }
          // specific verifyRepairs - verify that new segment gets created in ideal state with CONSUMING
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(llcSegmentName.getSegmentName()));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, llcSegmentName.getSegmentName(), null));
          // general verifyRepairs
          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        } else {
          int randomlySelectedPartition = random.nextInt(nPartitions);

          String rawTableName = TableNameBuilder.extractRawTableName(tableName);
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
          segmentManager.createNewSegmentMetadataZNRecord(tableName, newLlcSegmentName,
              latestMetadata.getStartOffset() + 100, expectedPartitionAssignment);

          Assert.assertNull(idealState.getRecord().getMapFields().get(newLlcSegmentName.getSegmentName()));
          nPartitions = expectedPartitionAssignment.getNumPartitions();
          Map<String, Map<String, String>> oldMapFields = idealState.getRecord().getMapFields();
          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          if (segmentManager.tooSoonToCorrect) {
            // validate nothing changed and try again with disabled
            Assert.assertEquals(oldMapFields, idealState.getRecord().getMapFields());
            segmentManager.tooSoonToCorrect = false;
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          }
          // verify that new segment gets created in ideal state with CONSUMING and old segment ONLINE
          Assert.assertNotNull(idealState.getRecord().getMapFields().get(newLlcSegmentName.getSegmentName()));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null));
          Assert.assertNotNull(
              segmentManager.getRealtimeSegmentZKMetadata(tableName, newLlcSegmentName.getSegmentName(), null));
          // general verifyRepairs
          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        }
      } else { // present in ideal state
        // 1. metadata IN_PROGRESS, segment CONSUMING - happy path
        // 2. metadata IN_PROGRESS/DONE, segment OFFLINE - repair: create new metadata and new segment
        // 3. metadata DONE, segment CONSUMING - repair: transition segment to ONLINE, new metadata, new segment
        if (!random.nextBoolean()) {
          consuming = false;
        }
        if (!random.nextBoolean()) {
          inProgress = false;
        }

        if (consuming) {
          if (inProgress) { // 1. happy path
            Map<String, Map<String, String>> oldIdealState = new HashMap<>(idealState.getRecord().getMapFields());
            nPartitions = expectedPartitionAssignment.getNumPartitions();

            Map<String, Map<String, String>> oldMapFields = idealState.getRecord().getMapFields();
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            if (segmentManager.tooSoonToCorrect) {
              // validate nothing changed and try again with disabled
              Assert.assertEquals(oldMapFields, idealState.getRecord().getMapFields());
              segmentManager.tooSoonToCorrect = false;
              segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            }
            // verify that nothing changed
            Assert.assertEquals(oldIdealState, idealState.getRecord().getMapFields());
            // general verifyRepairs
            verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
          } else { // 3. DONE + CONSUMING
            int randomlySelectedPartition = random.nextInt(nPartitions);
            Map<String, LLCSegmentName> partitionToLatestSegments =
                segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
            LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
            LLCRealtimeSegmentZKMetadata latestMetadata =
                segmentManager.getRealtimeSegmentZKMetadata(tableName, latestSegment.getSegmentName(), null);
            segmentManager.updateOldSegmentMetadataZNRecord(tableName, latestSegment,
                latestMetadata.getStartOffset() + 100);

            nPartitions = expectedPartitionAssignment.getNumPartitions();
            Map<String, Map<String, String>> oldMapFields = idealState.getRecord().getMapFields();
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            if (segmentManager.tooSoonToCorrect) {
              // validate nothing changed and try again with disabled
              Assert.assertEquals(oldMapFields, idealState.getRecord().getMapFields());
              segmentManager.tooSoonToCorrect = false;
              segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
            }

            // general verifyRepairs
            verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
          }
        } else { // 2. OFFLINE
          int randomlySelectedPartition = random.nextInt(nPartitions);
          Map<String, LLCSegmentName> partitionToLatestSegments =
              segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
          LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(randomlySelectedPartition));
          idealState = idealStateBuilder.transitionToState(latestSegment.getSegmentName(), "OFFLINE").build();


          nPartitions = expectedPartitionAssignment.getNumPartitions();
          Map<String, Map<String, String>> oldMapFields = idealState.getRecord().getMapFields();
          segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          if (segmentManager.tooSoonToCorrect) {
            // validate nothing changed and try again with disabled
            Assert.assertEquals(oldMapFields, idealState.getRecord().getMapFields());
            segmentManager.tooSoonToCorrect = false;
            segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
          }
          verifyRepairs(tableConfig, idealState, expectedPartitionAssignment, segmentManager, oldMapFields);
        }
      }

      // randomly change instances used, or increase partition count
      int randomExternalChanges = random.nextInt(20);
      int[] numInstancesSet = new int[]{4, 6, 8, 10};
      // randomly change instances and hence set a new expected partition assignment
      if (randomExternalChanges == 0) {
        int numInstances = numInstancesSet[random.nextInt(numInstancesSet.length)];
        instances = getInstanceList(numInstances);
        segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
        expectedPartitionAssignment =
            segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions);
      } else if (randomExternalChanges == 1) {
        // randomly increase partitions and hence set a new expected partition assignment
        expectedPartitionAssignment =
            segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions + 1);
      } else if (randomExternalChanges == 2) {
        // both and hence set a new expected partition assignment
        int numInstances = numInstancesSet[random.nextInt(numInstancesSet.length)];
        instances = getInstanceList(numInstances);
        segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
        expectedPartitionAssignment =
            segmentManager._partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions + 1);
      }
    }
  }

  private void verifyRepairs(TableConfig tableConfig, IdealState idealState,
      PartitionAssignment expectedPartitionAssignment, FakePinotLLCRealtimeSegmentManager segmentManager,
      Map<String, Map<String, String>> oldMapFields) {

    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // latest metadata for each partition
    Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> latestMetadata =
        segmentManager.getLatestMetadata(tableConfig.getTableName());

    // latest segment in ideal state for each partition
    Map<String, LLCSegmentName> partitionToLatestSegments =
        segmentManager._partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);

    // Assert that we have a latest metadata and segment for each partition
    List<String> latestSegmentNames = new ArrayList<>();
    for (int p = 0; p < expectedPartitionAssignment.getNumPartitions(); p++) {
      MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata> llcRealtimeSegmentZKMetadata = latestMetadata.get(p);
      LLCSegmentName latestLlcSegment = partitionToLatestSegments.get(String.valueOf(p));
      Assert.assertNotNull(llcRealtimeSegmentZKMetadata);
      Assert.assertNotNull(llcRealtimeSegmentZKMetadata.peekFirst());
      Assert.assertNotNull(latestLlcSegment);
      Assert.assertEquals(latestLlcSegment.getSegmentName(), llcRealtimeSegmentZKMetadata.peekFirst().getSegmentName());
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
    String tableName = tableConfig.getTableName();
    IdealState idealState = idealStateBuilder.clear().build();
    segmentManager._metadataMap.clear();

    segmentManager.validateLLCSegments(tableConfig, idealState, nPartitions);
    PartitionAssignment partitionAssignment =
        segmentManager._partitionAssignmentGenerator.getPartitionAssignmentFromIdealState(tableConfig, idealState);
    for (int p = 0; p < nPartitions; p++) {
      String segmentName = idealStateBuilder.getSegment(p, 0);
      advanceASeqForPartition(idealState, segmentManager, partitionAssignment, segmentName, p, 1, 100, tableName);
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
  public void testPreExistingSegments() {
    LLCSegmentName existingSegmentName = new LLCSegmentName("someTable", 1, 31, 12355L);
    String[] existingSegs = {existingSegmentName.getSegmentName()};
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(Arrays.asList(existingSegs));

    final String rtTableName = "testPreExistingLLCSegments_REALTIME";

    TableConfig tableConfig = makeTableConfig(rtTableName, 3, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    segmentManager.addTableToStore(rtTableName, tableConfig, 8);
    IdealState idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, 10);
    try {
      segmentManager.setupNewTable(tableConfig, idealState);
      Assert.fail("Did not get expected exception when setting up new table with existing segments in ");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  // Make sure that if we are either not leader or we are disconnected, we do not process metadata commit.
  @Test
  public void testCommittingSegmentIfDisconnected() {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);

    final String tableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    List<String> instances = getInstanceList(nInstances);
    final long memoryUsedBytes = 1000;
    TableConfig tableConfig = makeTableConfig(tableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    segmentManager.addTableToStore(tableName, tableConfig, nPartitions);

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(tableName, nReplicas);
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
    boolean status =
        segmentManager.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
            memoryUsedBytes);
    Assert.assertFalse(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 0);  // Idealstate not updated
    Assert.assertEquals(segmentManager._paths.size(), 0);   // propertystore not updated
    segmentManager.IS_CONNECTED = true;
    segmentManager.IS_LEADER = false;
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
        memoryUsedBytes);
    Assert.assertFalse(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 0);  // Idealstate not updated
    Assert.assertEquals(segmentManager._paths.size(), 0);   // propertystore not updated
    segmentManager.IS_LEADER = true;
    status = segmentManager.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
        memoryUsedBytes);
    Assert.assertTrue(status);
    Assert.assertEquals(segmentManager._nCallsToUpdateHelix, 1);  // Idealstate updated
    Assert.assertEquals(segmentManager._paths.size(), 2);   // propertystore updated
  }

  @Test
  public void testCommittingSegment() {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(null);

    final String rtTableName = "table_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(rtTableName);
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    List<String> instances = getInstanceList(nInstances);
    final long memoryUsed = 1000;
    TableConfig tableConfig = makeTableConfig(rtTableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, nReplicas);

    segmentManager.addTableToStore(rtTableName, tableConfig, nPartitions);
    segmentManager._partitionAssignmentGenerator.setConsumingInstances(instances);
    segmentManager.setupNewTable(tableConfig, idealState);

    // Now commit the first segment of partition 6.
    final int committingPartition = 6;
    final long nextOffset = 3425666L;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        new LLCRealtimeSegmentZKMetadata(segmentManager._records.get(committingPartition));
    segmentManager._paths.clear();
    segmentManager._records.clear();
    boolean status =
        segmentManager.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
            memoryUsed);
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
    Assert.assertEquals(newSegmentName.getSequenceNumber(), oldSegmentName.getSequenceNumber() + 1);
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
  public void testCommitSegmentWhenControllerWentThroughGC() {

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
    final long memoryUsed = 1000;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        new LLCRealtimeSegmentZKMetadata(segmentManager2._records.get(committingPartition));

    boolean status =
        segmentManager1.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
            memoryUsed);
    Assert.assertTrue(status);  // Committing segment metadata succeeded.

    status = segmentManager2.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
        memoryUsed);
    Assert.assertFalse(status); // Committing segment metadata failed.

    status = segmentManager3.commitSegmentMetadata(rawTableName, committingSegmentMetadata.getSegmentName(), nextOffset,
        memoryUsed);
    Assert.assertFalse(status); // Committing segment metadata failed.
  }

  private void setupSegmentManager(FakePinotLLCRealtimeSegmentManager segmentManager, String rtTableName) {
    final int nInstances = 6;
    final int nPartitions = 16;
    final int nReplicas = 3;
    List<String> instances = getInstanceList(nInstances);
    TableConfig tableConfig = makeTableConfig(rtTableName, nReplicas, KAFKA_OFFSET, DUMMY_HOST, DEFAULT_SERVER_TENANT,
        DEFAULT_STREAM_ASSIGNMENT_STRATEGY);
    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(rtTableName, nReplicas);

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
    Assert.assertTrue(realtimeSegmentManager.commitSegmentFile(tableName, segmentLocation, segmentName));
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
    Assert.assertTrue(realtimeSegmentManager.commitSegmentFile(tableName, segmentLocation, segmentName));
    Assert.assertTrue(new File(temporaryDirectory, otherSegmentNotToBeDeleted).exists());
    Assert.assertFalse(new File(temporaryDirectory, extraSegmentLocation).exists());
  }

  @Test
  public void testUpdateFlushThresholdForSegmentMetadata() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(Collections.<String>emptyList());

    PartitionAssignment partitionAssignment = new PartitionAssignment("fakeTable_REALTIME");
    // 4 partitions assigned to 4 servers, 4 replicas => the segments should have 250k rows each (1M / 4)
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for (int replicaId = 1; replicaId <= 4; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + replicaId);
      }

      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 250k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 250000);
    }

    // 4 partitions assigned to 4 servers, 2 partitions/server => the segments should have 500k rows each (1M / 2)
    partitionAssignment.getPartitionToInstances().clear();
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for (int replicaId = 1; replicaId <= 2; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + ((replicaId + segmentId) % 4));
      }

      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 500k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }

    // 4 partitions assigned to 4 servers, 1 partition/server => the segments should have 1M rows each (1M / 1)
    partitionAssignment.getPartitionToInstances().clear();
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();
      instances.add("Server_1.2.3.4_123" + segmentId);
      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 1M rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 1000000);
    }

    // Assign another partition to all servers => the servers should have 500k rows each (1M / 2)
    List<String> instances = new ArrayList<>();
    for (int replicaId = 1; replicaId <= 4; ++replicaId) {
      instances.add("Server_1.2.3.4_123" + replicaId);
    }
    partitionAssignment.addPartition("5", instances);

    // Check that each segment has 500k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      realtimeSegmentManager.updateFlushThresholdForSegmentMetadata(metadata, partitionAssignment, 1000000);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }
  }


  ////////////////////////////////////////////////////////////////////////////
  // Fake makers
  ///////////////////////////////////////////////////////////////////////////
  private StreamMetadata makeKafkaStreamMetadata(String topicName, String autoOffsetReset, String bootstrapHosts) {
    StreamMetadata streamMetadata = mock(StreamMetadata.class);
    Map<String, String> consumerPropertiesMap = new HashMap<>();
    consumerPropertiesMap.put(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET, autoOffsetReset);
    consumerPropertiesMap.put(CommonConstants.Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE, "simple");
    consumerPropertiesMap.put(CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST, bootstrapHosts);
    when(streamMetadata.getKafkaConsumerProperties()).thenReturn(consumerPropertiesMap);
    when(streamMetadata.getKafkaTopicName()).thenReturn(topicName);
    when(streamMetadata.getBootstrapHosts()).thenReturn(bootstrapHosts);
    when(streamMetadata.getConsumerFactoryName()).thenReturn(SimpleConsumerFactory.class.getName());
    return streamMetadata;
  }

  private TableConfig makeTableConfig(String tableName, int nReplicas, String autoOffsetReset, String bootstrapHosts,
      String serverTenant, StreamPartitionAssignmentStrategyEnum strategy) {
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartition()).thenReturn(Integer.toString(nReplicas));
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(nReplicas);
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    Map<String, String> streamConfigMap = new HashMap<>(1);

    streamConfigMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE), "simple");
    streamConfigMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET), autoOffsetReset);

    final String bootstrapHostConfigKey = CommonConstants.Helix.DataSource.STREAM_PREFIX + "."
        + CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST;
    streamConfigMap.put(bootstrapHostConfigKey, bootstrapHosts);

    IndexingConfig mockIndexConfig = mock(IndexingConfig.class);
    when(mockIndexConfig.getStreamConfigs()).thenReturn(streamConfigMap);
    if (strategy != null) {
      StreamConsumptionConfig mockStreamConsumptionConfig = mock(StreamConsumptionConfig.class);
      when(mockStreamConsumptionConfig.getStreamPartitionAssignmentStrategy()).thenReturn(strategy.toString());
      when(mockIndexConfig.getStreamConsumptionConfig()).thenReturn(mockStreamConsumptionConfig);
    } else {
      when(mockIndexConfig.getStreamConsumptionConfig()).thenReturn(null);
    }

    when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexConfig);
    TenantConfig mockTenantConfig = mock(TenantConfig.class);
    when(mockTenantConfig.getServer()).thenReturn(serverTenant);
    when(mockTableConfig.getTenantConfig()).thenReturn(mockTenantConfig);

    return mockTableConfig;
  }

  private static Map<String, String> getStreamConfigs() {
    Map<String, String> streamPropMap = new HashMap<>(1);
    streamPropMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE), "simple");
    streamPropMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET), "smallest");
    streamPropMap.put(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
        CommonConstants.Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST), "host:1234");
    return streamPropMap;
  }


  private String makeFakeSegmentName(int id) {
    return new LLCSegmentName("fakeTable_REALTIME", id, 0, 1234L).getSegmentName();
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Fake classes
  /////////////////////////////////////////////////////////////////////////////////

  static class FakePinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {

    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    private List<String> _existingLLCSegments = new ArrayList<>(1);

    public String _realtimeTableName;
    public Map<String, List<String>> _idealStateEntries = new HashMap<>(1);
    public List<String> _paths = new ArrayList<>(16);
    public List<ZNRecord> _records = new ArrayList<>(16);
    public String _startOffset;
    public boolean _createNew;
    public int _nReplicas;
    public Map<String, LLCRealtimeSegmentZKMetadata> _metadataMap = new HashMap<>(4);
    private FakePartitionAssignmentGenerator _partitionAssignmentGenerator;
    public PartitionAssignment _currentTablePartitionAssignment;

    public List<String> _newInstances;
    public List<String> _oldSegmentNameStr = new ArrayList<>(16);
    public List<String> _newSegmentNameStr = new ArrayList<>(16);

    public long _kafkaLargestOffsetToReturn;
    public long _kafkaSmallestOffsetToReturn;

    public List<ZNRecord> _existingSegmentMetadata;

    public int _nCallsToUpdateHelix = 0;
    public int _nCallsToCreateNewSegmentMetadata = 0;
    public IdealState _tableIdealState;
    public List<String> _currentInstanceList;
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
        _partitionAssignmentGenerator = new FakePartitionAssignmentGenerator(mockHelixManager);
        Field partitionAssignmentGeneratorField =
            PinotLLCRealtimeSegmentManager.class.getDeclaredField("_partitionAssignmentGenerator");
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

    void addTableToStore(String tableName, TableConfig tableConfig, int nKafkaPartitions) {
      _tableConfigStore.addTable(tableName, tableConfig, nKafkaPartitions);
    }

    void removeTableFromStore(String tableName) {
      _tableConfigStore.removeTable(tableName);
    }

    @Override
    protected List<String> getRealtimeTablesWithServerTenant(String serverTenantName) {
      return _tableConfigStore.getAllRealtimeTablesWithServerTenant(serverTenantName);
    }

    @Override
    protected TableConfig getRealtimeTableConfig(String realtimeTableName) {
      return _tableConfigStore.getTableConfig(realtimeTableName);
    }

    @Override
    protected boolean isTooSoonToCorrect(String tableNameWithType, String segmentId) {
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
    protected List<LLCRealtimeSegmentZKMetadata> getAllSegmentMetadata(String tableNameWithType) {
      return Lists.newArrayList(_metadataMap.values());
    }

    @Override
    protected void setupInitialSegments(TableConfig tableConfig, StreamMetadata streamMetadata,
        PartitionAssignment partitionAssignment, IdealState idealState, boolean create, int flushSize) {
      _realtimeTableName = tableConfig.getTableName();
      _currentTablePartitionAssignment = partitionAssignment;
      _startOffset = streamMetadata.getKafkaConsumerProperties()
          .get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
      super.setupInitialSegments(tableConfig, streamMetadata, partitionAssignment, idealState, create, flushSize);
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
    protected void updateIdealState(IdealState idealState, String realtimeTableName,
        Map<String, List<String>> idealStateEntries, boolean create, int nReplicas) {
      _realtimeTableName = realtimeTableName;
      _idealStateEntries = idealStateEntries;
      _nReplicas = nReplicas;
      _createNew = create;
      for (Map.Entry<String, List<String>> entry : idealStateEntries.entrySet()) {
        _idealStateEntries.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    protected boolean createNewSegmentMetadataZNRecord(String realtimeTableName, LLCSegmentName newLLCSegmentName,
        long nextOffset, PartitionAssignment partitionAssignment) {
      _nCallsToCreateNewSegmentMetadata++;
      return super.createNewSegmentMetadataZNRecord(realtimeTableName, newLLCSegmentName, nextOffset,
          partitionAssignment);
    }

    @Override
    protected boolean updateOldSegmentMetadataZNRecord(String realtimeTableName,
        LLCSegmentName committingLLCSegmentName, long nextOffset) {
      return super.updateOldSegmentMetadataZNRecord(realtimeTableName, committingLLCSegmentName, nextOffset);
    }

    protected void updateIdealState(final String realtimeTableName, final List<String> newInstances,
        final String oldSegmentNameStr, final String newSegmentNameStr) {
      _realtimeTableName = realtimeTableName;
      _newInstances = newInstances;
      _oldSegmentNameStr.add(oldSegmentNameStr);
      _newSegmentNameStr.add(newSegmentNameStr);
      _idealStateEntries.put(newSegmentNameStr, newInstances);
      _nCallsToUpdateHelix++;
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
    protected long getKafkaPartitionOffset(StreamMetadata streamMetadata, final String offsetCriteria,
        int partitionId) {
      if (offsetCriteria.equals("largest")) {
        return _kafkaLargestOffsetToReturn;
      } else {
        return _kafkaSmallestOffsetToReturn;
      }
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
    protected void setTableIdealState(String realtimeTableName, IdealState idealState) {
      _tableIdealState = idealState;
    }

    @Override
    public void setupNewTable(TableConfig tableConfig, IdealState emptyIdealState) {
      _currentTable = tableConfig.getTableName();
      super.setupNewTable(tableConfig, emptyIdealState);
    }

    @Override
    protected List<String> getInstances(String tenantName) {
      return _currentInstanceList;
    }

    @Override
    protected int getKafkaPartitionCount(StreamMetadata metadata) {
      return _tableConfigStore.getNKafkaPartitions(_currentTable);
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

  private static class FakePartitionAssignmentGenerator extends PartitionAssignmentGenerator {

    private List<String> _consumingInstances;

    public FakePartitionAssignmentGenerator(HelixManager helixManager) {
      super(helixManager);
      _consumingInstances = new ArrayList<>();
    }

    @Override
    protected List<String> getConsumingTaggedInstances(RealtimeTagConfig realtimeTagConfig) {
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

    void addTable(String tableName, TableConfig tableConfig, int nKafkaPartitions) {
      _tableConfigsStore.put(tableName, tableConfig);
      _nPartitionsStore.put(tableName, nKafkaPartitions);
    }

    void removeTable(String tableName) {
      _tableConfigsStore.remove(tableName);
      _nPartitionsStore.remove(tableName);
    }

    TableConfig getTableConfig(String tableName) {
      return _tableConfigsStore.get(tableName);
    }

    int getNKafkaPartitions(String tableName) {
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
