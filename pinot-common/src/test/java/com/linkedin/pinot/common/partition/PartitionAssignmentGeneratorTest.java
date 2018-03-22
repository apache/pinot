package com.linkedin.pinot.common.partition;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class PartitionAssignmentGeneratorTest {

  private String tableName = "aTableName_REALTIME";
  private TestPartitionAssignmentGenerator _partitionAssignmentGenerator;

  private String[] consumingServerNames;

  private List<String> getConsumingInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= consumingServerNames.length);
    String[] instanceArray = Arrays.copyOf(consumingServerNames, nServers);
    return Lists.newArrayList(instanceArray);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    _partitionAssignmentGenerator = new TestPartitionAssignmentGenerator(null);

    final int maxInstances = 20;
    consumingServerNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  /**
   * Given an ideal state, constructs the partition assignment for the table
   */
  @Test
  public void testGetPartitionAssignmentFromIdealState() {

    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);
    IdealState idealState = new IdealState(tableName);

    int numPartitions = 0;
    List<LLCSegmentName> segmentsToCheck = new ArrayList<>();

    // empty ideal state
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // 3 partitions 2 replicas, all on 0th seq number (6 segments)
    LLCSegmentName llcSegment_0_0 = new LLCSegmentName(tableName, 0, 0, System.currentTimeMillis());
    Map<String, String> instanceStateMap_0_0 = new HashMap<>(2);
    instanceStateMap_0_0.put("s1", "CONSUMING");
    instanceStateMap_0_0.put("s2", "CONSUMING");
    LLCSegmentName llcSegment_1_0 = new LLCSegmentName(tableName, 1, 0, System.currentTimeMillis());
    Map<String, String> instanceStateMap_1_0 = new HashMap<>(2);
    instanceStateMap_1_0.put("s3", "CONSUMING");
    instanceStateMap_1_0.put("s4", "CONSUMING");
    LLCSegmentName llcSegment_2_0 = new LLCSegmentName(tableName, 2, 0, System.currentTimeMillis());
    Map<String, String> instanceStateMap_2_0 = new HashMap<>(2);
    instanceStateMap_2_0.put("s1", "CONSUMING");
    instanceStateMap_2_0.put("s2", "CONSUMING");
    idealState.setInstanceStateMap(llcSegment_0_0.getSegmentName(), instanceStateMap_0_0);
    idealState.setInstanceStateMap(llcSegment_1_0.getSegmentName(), instanceStateMap_1_0);
    idealState.setInstanceStateMap(llcSegment_2_0.getSegmentName(), instanceStateMap_2_0);
    numPartitions = 3;
    segmentsToCheck = Lists.newArrayList(llcSegment_0_0, llcSegment_1_0, llcSegment_2_0);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // 3 partitions 2 replicas, 2 on 0th seq number 1 on 1st seq number (8 segments)
    LLCSegmentName llcSegment_0_1 = new LLCSegmentName(tableName, 0, 1, System.currentTimeMillis());
    instanceStateMap_0_0 = new HashMap<>(2);
    instanceStateMap_0_0.put("s1", "ONLINE");
    instanceStateMap_0_0.put("s2", "ONLINE");
    Map<String, String> instanceStateMap_0_1 = new HashMap<>(2);
    instanceStateMap_0_1.put("s1", "CONSUMING");
    instanceStateMap_0_1.put("s2", "CONSUMING");
    idealState.setInstanceStateMap(llcSegment_0_0.getSegmentName(), instanceStateMap_0_0);
    idealState.setInstanceStateMap(llcSegment_0_1.getSegmentName(), instanceStateMap_0_1);
    segmentsToCheck = Lists.newArrayList(llcSegment_0_1, llcSegment_1_0, llcSegment_2_0);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // 3 partitions 2 replicas, all on 1st seg num (12 segments)
    LLCSegmentName llcSegment_1_1 = new LLCSegmentName(tableName, 1, 1, System.currentTimeMillis());
    instanceStateMap_1_0 = new HashMap<>(2);
    instanceStateMap_1_0.put("s3", "ONLINE");
    instanceStateMap_1_0.put("s4", "ONLINE");
    Map<String, String> instanceStateMap_1_1 = new HashMap<>(2);
    instanceStateMap_1_1.put("s3", "CONSUMING");
    instanceStateMap_1_1.put("s4", "CONSUMING");
    idealState.setInstanceStateMap(llcSegment_1_0.getSegmentName(), instanceStateMap_1_0);
    idealState.setInstanceStateMap(llcSegment_1_1.getSegmentName(), instanceStateMap_1_1);
    LLCSegmentName llcSegment_2_1 = new LLCSegmentName(tableName, 2, 1, System.currentTimeMillis());
    instanceStateMap_2_0 = new HashMap<>(2);
    instanceStateMap_2_0.put("s1", "ONLINE");
    instanceStateMap_2_0.put("s2", "ONLINE");
    Map<String, String> instanceStateMap_2_1 = new HashMap<>(2);
    instanceStateMap_2_1.put("s1", "CONSUMING");
    instanceStateMap_2_1.put("s2", "CONSUMING");
    idealState.setInstanceStateMap(llcSegment_2_0.getSegmentName(), instanceStateMap_2_0);
    idealState.setInstanceStateMap(llcSegment_2_1.getSegmentName(), instanceStateMap_2_1);
    segmentsToCheck = Lists.newArrayList(llcSegment_0_1, llcSegment_1_1, llcSegment_2_1);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // 3 partitions 2 replicas, seq 0 has moved to COMPLETED servers, seq 1 on CONSUMING instances
    instanceStateMap_0_0 = new HashMap<>(2);
    instanceStateMap_0_0.put("s1_completed", "ONLINE");
    instanceStateMap_0_0.put("s2_completed", "ONLINE");
    idealState.setInstanceStateMap(llcSegment_0_0.getSegmentName(), instanceStateMap_0_0);
    instanceStateMap_1_0 = new HashMap<>(2);
    instanceStateMap_1_0.put("s3_completed", "ONLINE");
    instanceStateMap_1_0.put("s4_completed", "ONLINE");
    idealState.setInstanceStateMap(llcSegment_1_0.getSegmentName(), instanceStateMap_1_0);
    instanceStateMap_2_0 = new HashMap<>(2);
    instanceStateMap_2_0.put("s1_completed", "ONLINE");
    instanceStateMap_2_0.put("s2_completed", "ONLINE");
    idealState.setInstanceStateMap(llcSegment_2_0.getSegmentName(), instanceStateMap_2_0);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // status of latest segments OFFLINE/ERROR
    instanceStateMap_2_1 = new HashMap<>(2);
    instanceStateMap_2_1.put("s1", "ERROR");
    instanceStateMap_2_1.put("s2", "ERROR");
    idealState.setInstanceStateMap(llcSegment_2_1.getSegmentName(), instanceStateMap_2_1);
    instanceStateMap_1_1 = new HashMap<>(2);
    instanceStateMap_1_1.put("s3", "OFFLINE");
    instanceStateMap_1_1.put("s4", "OFFLINE");
    idealState.setInstanceStateMap(llcSegment_1_1.getSegmentName(), instanceStateMap_1_1);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // all non llc segments
    idealState.getRecord().getMapFields().clear();
    String aNonLLCSegment = "randomName";
    Map<String, String> instanceStateMap = new HashMap<>(1);
    instanceStateMap.put("s1", "CONSUMING");
    instanceStateMap.put("s2", "CONSUMING");
    idealState.setInstanceStateMap(aNonLLCSegment, instanceStateMap);
    numPartitions = 0;
    segmentsToCheck.clear();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);

    // some non llc segments, some llc segments
    idealState.setInstanceStateMap(llcSegment_0_0.getSegmentName(), instanceStateMap_0_0);
    numPartitions = 1;
    segmentsToCheck = Lists.newArrayList(llcSegment_0_0);
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions, segmentsToCheck);
  }

  private void verifyPartitionAssignmentFromIdealState(TableConfig tableConfig, IdealState idealState,
      int numPartitions, List<LLCSegmentName> segmentsToCheck) {
    PartitionAssignment partitionAssignmentFromIdealState =
        _partitionAssignmentGenerator.getPartitionAssignmentFromIdealState(tableConfig, idealState);
    Assert.assertEquals(tableConfig.getTableName(), partitionAssignmentFromIdealState.getTableName());
    Assert.assertEquals(partitionAssignmentFromIdealState.getNumPartitions(), numPartitions);
    // check that latest segments are honoring partition assignment
    for (LLCSegmentName llcSegmentName : segmentsToCheck) {
      String segmentName = llcSegmentName.getSegmentName();
      int partitionId = llcSegmentName.getPartitionId();
      Set<String> idealStateInstances = idealState.getInstanceStateMap(segmentName).keySet();
      List<String> partitionAssignmentInstances =
          partitionAssignmentFromIdealState.getInstancesListForPartition(String.valueOf(partitionId));
      Assert.assertEquals(idealStateInstances.size(), partitionAssignmentInstances.size());
      Assert.assertTrue(idealStateInstances.containsAll(partitionAssignmentInstances));
    }
  }

  @Test
  public void testGeneratePartitionAssignment() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(2);
    when(tableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    int numPartitions = 0;
    List<String> consumingInstanceList = getConsumingInstanceList(0);
    PartitionAssignment previousPartitionAssignment = new PartitionAssignment(tableName);
    boolean exception;
    boolean unchanged = false;

    // 0 consuming instances - error not enough instances
    exception = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 1 consuming instance - error not enough instances
    consumingInstanceList = getConsumingInstanceList(1);
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 0 partitions - 3 consuming instances - empty partition assignment
    consumingInstanceList = getConsumingInstanceList(3);
    _partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    exception = false;
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 partitions - 3 consuming instances - should use 3
    numPartitions = 3;
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 partitions - 6 consuming instances - increased instances, should use 6
    consumingInstanceList = getConsumingInstanceList(6);
    _partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 partitions - 12 consuming instances - increased beyond MAX allowed, should use 10 (or whatever is the MAX)
    consumingInstanceList = getConsumingInstanceList(12);
    _partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // increase in partitions - 4
    numPartitions = 4;
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList, previousPartitionAssignment,
        exception, unchanged);

    // increase in partitions - 8 (numPartitions * numReplicas = 8*2 = 16. but should still use only 10 instances)
    numPartitions = 8;
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // same - shouldn't change
    unchanged = true;
    verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList, previousPartitionAssignment,
        exception, unchanged);
  }

  private PartitionAssignment verifyGeneratePartitionAssignment(TableConfig tableConfig, int numPartitions,
      List<String> consumingInstanceList, PartitionAssignment previousPartitionAssignment, boolean exception,
      boolean unchanged) {
    PartitionAssignment partitionAssignment;
    try {
      partitionAssignment = _partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, numPartitions);
      Assert.assertFalse(exception);
      verify(partitionAssignment, numPartitions, consumingInstanceList, unchanged, previousPartitionAssignment);
    } catch (Exception e) {
      Assert.assertTrue(exception);
      partitionAssignment = previousPartitionAssignment;
    }

    return partitionAssignment;
  }

  @Test
  public void testRegeneratePartitionAssignment() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(2);
    when(tableConfig.getValidationConfig()).thenReturn(mockValidationConfig);

    IdealState idealState = new IdealState(tableName);
    int numPartitions = 0;
    List<String> consumingInstanceList = getConsumingInstanceList(0);
    PartitionAssignment previousPartitionAssignment = new PartitionAssignment(tableName);
    boolean exception;
    boolean unchanged = false;

    // empty instances - error not enough instances
    exception = true;
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 1 instance - error not enough instances
    consumingInstanceList = getConsumingInstanceList(1);
    _partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 instances - empty ideal state
    consumingInstanceList = getConsumingInstanceList(3);
    exception = false;
    _partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // no change
    unchanged = true;
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 instances - 4 partitions
    unchanged = false;
    numPartitions = 4;
    LLCSegmentName llcSegmentName_0_0 = new LLCSegmentName(tableName, 0, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_1_0 = new LLCSegmentName(tableName, 1, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_2_0 = new LLCSegmentName(tableName, 2, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_3_0 = new LLCSegmentName(tableName, 3, 0, System.currentTimeMillis());
    idealState.setInstanceStateMap(llcSegmentName_0_0.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_1_0.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_2_0.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_3_0.getSegmentName(), new HashMap<String, String>(1));
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // no change
    unchanged = true;
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // 3 instances - 4 partitions - some partitions advanced in seq number
    unchanged = false;
    LLCSegmentName llcSegmentName_1_1 = new LLCSegmentName(tableName, 1, 1, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_2_1 = new LLCSegmentName(tableName, 2, 1, System.currentTimeMillis());
    idealState.setInstanceStateMap(llcSegmentName_2_1.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_1_1.getSegmentName(), new HashMap<String, String>(1));
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);


    // no change
    unchanged = true;
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // instances increased to 12 - use only MAX allowed
    unchanged = false;
    consumingInstanceList = getConsumingInstanceList(12);
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // no change
    unchanged = true;
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // partitions in ideal state increased
    unchanged = false;
    numPartitions = 7;
    LLCSegmentName llcSegmentName_4_0 = new LLCSegmentName(tableName, 4, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_5_0 = new LLCSegmentName(tableName, 5, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName_6_0 = new LLCSegmentName(tableName, 6, 0, System.currentTimeMillis());
    idealState.setInstanceStateMap(llcSegmentName_4_0.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_5_0.getSegmentName(), new HashMap<String, String>(1));
    idealState.setInstanceStateMap(llcSegmentName_6_0.getSegmentName(), new HashMap<String, String>(1));
    previousPartitionAssignment = verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

    // some hlc segments in ideal state
    unchanged = true;
    idealState.setInstanceStateMap("randomSegmentName", new HashMap<String, String>(1));
    verifyRegeneratePartitionAssignment(tableConfig, idealState, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exception, unchanged);

  }

  private PartitionAssignment verifyRegeneratePartitionAssignment(TableConfig tableConfig, IdealState idealState,
      int numPartitions, List<String> consumingInstanceList, PartitionAssignment previousPartitionAssignment,
      boolean exception, boolean unchanged) {
    PartitionAssignment partitionAssignment;
    try {
      partitionAssignment = _partitionAssignmentGenerator.regeneratePartitionAssignment(tableConfig, idealState);
      Assert.assertFalse(exception);
      verify(partitionAssignment, numPartitions, consumingInstanceList, unchanged, previousPartitionAssignment);
    } catch (Exception e) {
      Assert.assertTrue(exception);
      partitionAssignment = previousPartitionAssignment;
    }

    return partitionAssignment;
  }

  private void verify(PartitionAssignment partitionAssignment, int numPartitions, List<String> consumingInstanceList,
      boolean unchanged, PartitionAssignment previousPartitionAssignment) {
    Assert.assertEquals(partitionAssignment.getTableName(), tableName);

    // check num partitions equal
    Assert.assertEquals(partitionAssignment.getNumPartitions(), numPartitions);

    List<String> instancesUsed = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : partitionAssignment.getPartitionToInstances().entrySet()) {
      for (String instance : entry.getValue()) {
        if (!instancesUsed.contains(instance)) {
          instancesUsed.add(instance);
        }
      }
    }

    // check all instances belong to the super set
    Assert.assertTrue(consumingInstanceList.containsAll(instancesUsed));

    // count number of instances used, should be min(MAX_ALLOWED and provided)
    int allowedNumInstances = _partitionAssignmentGenerator.getNumInstancesToUse(consumingInstanceList.size());
    Assert.assertTrue(instancesUsed.size() <= allowedNumInstances);

    // verify strategy is uniform
    int serverId = 0;
    for (Map.Entry<String, List<String>> entry : partitionAssignment.getPartitionToInstances().entrySet()) {
      for (String instance : entry.getValue()) {
        Assert.assertTrue(instance.equals(instancesUsed.get(serverId++)));
        if (serverId == instancesUsed.size()) {
          serverId = 0;
        }
      }
    }

    // if nothing changed, should be same as before
    if (unchanged) {
      Assert.assertEquals(partitionAssignment, previousPartitionAssignment);
    }
  }

  private class TestPartitionAssignmentGenerator extends PartitionAssignmentGenerator {

    private List<String> _consumingInstances;

    public TestPartitionAssignmentGenerator(HelixManager helixManager) {
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
}