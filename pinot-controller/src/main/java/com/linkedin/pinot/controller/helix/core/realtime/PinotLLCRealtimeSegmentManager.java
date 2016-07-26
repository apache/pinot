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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  private static final String KAFKA_PARTITIONS_PATH = "/KAFKA_PARTITIONS";

  private static PinotLLCRealtimeSegmentManager INSTANCE = null;

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;
  private boolean _amILeader = false;

  public static synchronized void create(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created");
    }
    INSTANCE = new PinotLLCRealtimeSegmentManager(helixAdmin, clusterName, helixManager, propertyStore, helixResourceManager);
  }

  private String makeKafkaPartitionPath(String realtimeTableName) {
    return KAFKA_PARTITIONS_PATH + "/" + realtimeTableName;
  }

  protected PinotLLCRealtimeSegmentManager(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    _helixAdmin = helixAdmin;
    _helixManager = helixManager;
    _propertyStore = propertyStore;
    _helixResourceManager = helixResourceManager;
    _clusterName = clusterName;
  }

  public static PinotLLCRealtimeSegmentManager getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Not yet created");
    }
    return INSTANCE;
  }

  // TODO Hook this up for leadership transfer
  public void onBecomeLeader() {
    if (isLeader()) {
      if (!_amILeader) {
        // We were not leader before, now we are.
        _amILeader = true;
        LOGGER.info("Became leader");
        // Scanning tables to check for incomplete table additions is optional if we make table addtition operations
        // idempotent.The user can retry the failed operation and it will work.
        //
        // Go through all partitions of all tables that have LL configured, and check that they have as many
        // segments in CONSUMING state in Idealstate as there are kafka partitions.
        completeCommittingSegments();
      } else {
        // We already had leadership, nothing to do.
        LOGGER.info("Already leader. Duplicate notification");
      }
    } else {
      _amILeader = false;
      LOGGER.info("Lost leadership");
    }
  }

  private boolean isLeader() {
    return _helixManager.isLeader();
  }

  /*
   * Use helix balancer to balance the kafka partitions amongst the realtime nodes (for a table).
   * The topic name is being used as a dummy helix resource name. We do not read or write to zk in this
   * method.
   */
  public void setupHelixEntries(final String topicName, final String realtimeTableName, int nPartitions,
      final List<String> instanceNames, int nReplicas, long startOffset, IdealState idealState, boolean create) {
    if (nReplicas > instanceNames.size()) {
      throw new RuntimeException("Replicas requested(" + nReplicas + ") cannot fit within number of instances(" +
          instanceNames.size() + ") for table " + realtimeTableName + " topic " + topicName);
    }
    /*
     Due to a bug in auto-rebalance (https://issues.apache.org/jira/browse/HELIX-631)
     we do the kafka partition allocation with local code in this class.
    {
      final String resourceName = topicName;

      List<String> partitions = new ArrayList<>(nPartitions);
      for (int i = 0; i < nPartitions; i++) {
        partitions.add(Integer.toString(i));
      }

      LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
      states.put("OFFLINE", 0);
      states.put("ONLINE", nReplicas);

      AutoRebalanceStrategy strategy = new AutoRebalanceStrategy(resourceName, partitions, states);
      znRecord = strategy.computePartitionAssignment(instanceNames, new HashMap<String, Map<String, String>>(0), instanceNames);
      znRecord.setMapFields(new HashMap<String, Map<String, String>>(0));
    }
    */

    // Allocate kafka partitions across server instances.
    ZNRecord znRecord = new ZNRecord(topicName);
    int serverId = 0;
    for (int p = 0; p < nPartitions; p++) {
      List<String> instances = new ArrayList<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instances.add(instanceNames.get(serverId++));
        if (serverId == instanceNames.size()) {
          serverId = 0;
        }
      }
      znRecord.setListField(Integer.toString(p), instances);
    }
    writeKafkaPartitionAssignemnt(realtimeTableName, znRecord);
    setupInitialSegments(realtimeTableName, znRecord, startOffset, idealState, create);
  }

  protected void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
    final String path = makeKafkaPartitionPath(realtimeTableName);
    _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  protected ZNRecord getKafkaPartitionAssignment(final String realtimeTableName) {
    final String path = makeKafkaPartitionPath(realtimeTableName);
    return _propertyStore.get(path, null, AccessOption.PERSISTENT);
  }

  protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, long startOffset,
      IdealState idealState, boolean create) {
    List<String> currentSegments = getExistingSegments(realtimeTableName);
    // Make sure that there are no low-level segments existing.
    if (currentSegments != null) {
      for (String segment : currentSegments) {
        if (!SegmentName.isHighLevelConsumerSegmentName(segment)) {
          // For now, we don't support changing of kafka partitions, or otherwise re-creating the low-level
          // realtime segments for any other reason.
          throw new RuntimeException("Low-level segments already exist for table " + realtimeTableName);
        }
      }
    }
    // Map of segment names to the server-instances that hold the segment.
    final Map<String, List<String>> idealStateEntries = new HashMap<String, List<String>>(4);
    final Map<String, List<String>> partitionMap = partitionAssignment.getListFields();
    final int nPartitions = partitionMap.size();

    // Create one segment entry in PROPERTYSTORE for each kafka partition.
    // Any of these may already be there, so bail out clean if they are already present.
    List<String> paths = new ArrayList<>(nPartitions);
    List<ZNRecord> records = new ArrayList<>(nPartitions);
    final long now = System.currentTimeMillis();
    final int seqNum = 0; // Initial seq number for the segments
    for (int i = 0; i < nPartitions; i++) {
      final List instances = partitionMap.get(Integer.toString(i));
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
      LLCSegmentName llcSegmentName = new LLCSegmentName(rawTableName, i, seqNum, now);
      final String segName = llcSegmentName.getSegmentName();

      metadata.setCreationTime(now);
      metadata.setStartOffset(startOffset);
      metadata.setNumReplicas(instances.size());
      metadata.setTableName(realtimeTableName);
      metadata.setSegmentName(segName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

      ZNRecord record = metadata.toZNRecord();
      final String znodePath = PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName + "/" + segName;
      paths.add(znodePath);
      records.add(record);
      idealStateEntries.put(segName, instances);
    }

    writeSegmentsToPropertyStore(paths, records);
    LOGGER.info("Added {} segments to propertyStore for table {}", paths.size(), realtimeTableName);

    updateHelixIdealState(idealState, realtimeTableName, idealStateEntries, create);
  }

  // Update the helix idealstate when a new table is added.
  protected void updateHelixIdealState(final IdealState idealState, String realtimeTableName, final Map<String, List<String>> idealStateEntries,
      boolean create) {
    if (create) {
      addLLCRealtimeSegmentsInIdealState(idealState, idealStateEntries);
      _helixAdmin.addResource(_clusterName, realtimeTableName, idealState);
    } else {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
        @Override
        public IdealState apply(IdealState idealState) {
          return addLLCRealtimeSegmentsInIdealState(idealState, idealStateEntries);
        }
      }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
    }
  }

  // Update the helix state when an old segment commits and a new one is to be started.
  protected void updateHelixIdealState(final String realtimeTableName, final List<String> newInstances,
      final String oldSegmentNameStr, final String newSegmentNameStr) {
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        return updateForNewRealtimeSegment(idealState, newInstances, oldSegmentNameStr, newSegmentNameStr);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
  }

  protected static IdealState updateForNewRealtimeSegment(IdealState idealState,
      final List<String> newInstances, final String oldSegmentNameStr, final String newSegmentNameStr) {
    if (oldSegmentNameStr != null) {
      // Update the old ones to be ONLINE
      Set<String>  oldInstances = idealState.getInstanceSet(oldSegmentNameStr);
      for (String instance : oldInstances) {
        idealState.setPartitionState(oldSegmentNameStr, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
      }
    }

    // We may have (for whatever reason) a different instance list in the idealstate for the new segment.
    // If so, clear it, and then set the instance state for the set of instances that we know should be there.
    Map<String, String> stateMap = idealState.getInstanceStateMap(newSegmentNameStr);
    if (stateMap != null) {
      stateMap.clear();
    }
    for (String instance : newInstances) {
      idealState.setPartitionState(newSegmentNameStr, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    }

    return idealState;
  }

  private IdealState addLLCRealtimeSegmentsInIdealState(final IdealState idealState, Map<String, List<String>> idealStateEntries) {
    for (Map.Entry<String, List<String>> entry : idealStateEntries.entrySet()) {
      final String segmentId = entry.getKey();
      final Map<String, String> stateMap = idealState.getInstanceStateMap(segmentId);
      if (stateMap != null) {
        // Replace the segment if it already exists
        stateMap.clear();
      }
      for (String instanceName : entry.getValue()) {
        idealState.setPartitionState(segmentId, instanceName, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
      }
    }
    return idealState;
  }

  protected List<String> getExistingSegments(String realtimeTableName) {
    return  _propertyStore.getChildNames(PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName, AccessOption.PERSISTENT);
  }

  protected List<ZNRecord> getExistingSegmentMetadata(String realtimeTableName) {
    return _propertyStore.getChildren(PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName, null, 0);

  }

  protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records) {
    _propertyStore.setChildren(paths, records, AccessOption.PERSISTENT);
  }

  protected List<String> getAllRealtimeTables() {
    return _helixResourceManager.getAllRealtimeTables();
  }

  protected IdealState getTableIdealState(String realtimeTableName) {
    return HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
  }

  /**
   * This method is invoked after the realtime segment is uploaded but before a response is sent to the server.
   * It updates the propertystore segment metadata from IN_PROGRESS to DONE, and also creates new propertystore
   * records for new segments, and puts them in idealstate in CONSUMING state.
   *
   * @param rawTableName Raw table name
   * @param committingSegmentNameStr Committing segment name
   * @param nextOffset The offset with which the next segment should start.
   * @return
   */
  // TODO Change this to be tableName rather than realtimeTableName
  public boolean commitSegment(String rawTableName, final String committingSegmentNameStr, long nextOffset) {
    final long now = System.currentTimeMillis();
    final String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(rawTableName);

    final LLCRealtimeSegmentZKMetadata oldSegMetadata = getRealtimeSegmentZKMetadata(realtimeTableName,
        committingSegmentNameStr);
    final LLCSegmentName oldSegmentName = new LLCSegmentName(committingSegmentNameStr);
    final int partitionId = oldSegmentName.getPartitionId();
    final int oldSeqNum = oldSegmentName.getSequenceNumber();
    oldSegMetadata.setEndOffset(nextOffset);
    oldSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    oldSegMetadata.setEndTime(now);
    final ZNRecord oldZnRecord = oldSegMetadata.toZNRecord();
    final String oldZnodePath = PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName + "/" + committingSegmentNameStr;

    final ZNRecord partitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
    List<String> newInstances = partitionAssignment.getListField(Integer.toString(partitionId));
    final Map<String, List<String>> idealStateEntries = new HashMap<>(1);

    // Construct segment metadata and idealstate for the new segment
    final int newSeqNum = oldSeqNum + 1;
    final long newStartOffset = nextOffset;
    LLCSegmentName newHolder = new LLCSegmentName(oldSegmentName.getTableName(), partitionId, newSeqNum, now);
    final String newSegmentNameStr = newHolder.getSegmentName();
    final LLCRealtimeSegmentZKMetadata newSegMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(System.currentTimeMillis());
    newSegMetadata.setStartOffset(newStartOffset);
    newSegMetadata.setNumReplicas(newInstances.size());
    newSegMetadata.setTableName(realtimeTableName);
    newSegMetadata.setSegmentName(newSegmentNameStr);
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    final ZNRecord newZnRecord = newSegMetadata.toZNRecord();
    final String newZnodePath = PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName + "/" + newSegmentNameStr;

    idealStateEntries.put(newSegmentNameStr, newInstances);

    List<String> paths = new ArrayList<>(2);
    paths.add(oldZnodePath);
    paths.add(newZnodePath);
    List<ZNRecord> records = new ArrayList<>(2);
    records.add(oldZnRecord);
    records.add(newZnRecord);
    /*
     * Update zookeeper in two steps.
     *
     * Step 1: Update PROPERTYSTORE to change the segment metadata for old segment and add a new one for new segment
     * Step 2: Update IDEALSTATES to include the new segment in the idealstate for the table in CONSUMING state, and change
     *         the old segment to ONLINE state.
     *
     * The controller may fail between these two steps, so when a new controller takes over as leader, it needs to
     * check whether there are any recent segments in PROPERTYSTORE that are not accounted for in idealState. If so,
     * it should create the new segments in idealState.
     *
     * If the controller fails after step-2, we are fine because the idealState has the new segments.
     * If the controller fails before step-1, the server will see this as an upload failure, and will re-try.
     */
    writeSegmentsToPropertyStore(paths, records);

    // TODO Introduce a controller failure here for integration testing

    updateHelixIdealState(realtimeTableName, newInstances, committingSegmentNameStr, newSegmentNameStr);
    return true;
  }

  public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName) {
    return new LLCRealtimeSegmentZKMetadata(_propertyStore.get(
          ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName), null, AccessOption.PERSISTENT));
  }

  private void completeCommittingSegments() {
    for (String realtimeTableName : getAllRealtimeTables()) {
      completeCommittingSegments(realtimeTableName);
    }
  }

  protected void completeCommittingSegments(String realtimeTableName) {
    IdealState idealState = getTableIdealState(realtimeTableName);
    Set<String> segmentNamesIS = idealState.getPartitionSet();
    List<ZNRecord> segmentMetadataList = getExistingSegmentMetadata(realtimeTableName);
    if (segmentMetadataList == null || segmentMetadataList.isEmpty()) {
      return;
    }
    final List<LLCSegmentName> segmentNames = new ArrayList<>(segmentMetadataList.size());

    for (ZNRecord segment : segmentMetadataList) {
      if (SegmentName.isLowLevelConsumerSegmentName(segment.getId())) {
        segmentNames.add(new LLCSegmentName(segment.getId()));
      }
    }

    if (segmentNames.isEmpty()) {
      return;
    }

    Collections.sort(segmentNames, Collections.reverseOrder());

    int curPartition = segmentNames.get(0).getPartitionId();  // Current kafka partition we are working on.
    final int nSegments = segmentNames.size();

    /*
     * We only need to look at the most recent segment in the kafka partition. If that segment is also present
     * in the idealstate, we are good.
     * Otherwise, we need to add that segment to the idealstate:
     * - We find the current instance assignment for that partition and update idealstate accordingly.
     * NOTE: It may be that the kafka assignment of instances has changed for this partition. In that case,
     * we need to also modify the numPartitions field in the segment metadata.
     * TODO Modify numPartitions field in segment metadata and re-write it in propertystore.
     * The numPartitions field in the metadata is used by SegmentCompletionManager
     */
    for (int i = 0; i < nSegments; i++) {
      final LLCSegmentName segmentName = segmentNames.get(i);
      if (segmentName.getPartitionId() == curPartition) {
        final String curSegmentNameStr = segmentName.getSegmentName();
        if (!segmentNamesIS.contains(curSegmentNameStr)) {
          LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", realtimeTableName, curPartition,
              curSegmentNameStr);

          final ZNRecord partitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
          List<String> newInstances = partitionAssignment.getListField(Integer.toString(curPartition));
          LOGGER.info("{}: Assigning segment {} to {}", realtimeTableName, curSegmentNameStr, newInstances);
          // TODO Re-write num-partitions in metadata if needed.

          String prevSegmentNameStr = null;
          // If there was a prev segment in the same partition, then we need to fix it to be ONLINE.
          if (i < nSegments-1) {
            LLCSegmentName prevSegmentName = segmentNames.get(i+1);
            if (prevSegmentName.getPartitionId() == segmentName.getPartitionId()) {
              prevSegmentNameStr = prevSegmentName.getSegmentName();
            }
          }
          updateHelixIdealState(realtimeTableName, newInstances, prevSegmentNameStr, curSegmentNameStr);
          // Skip all other segments in this partition.
        }
        curPartition--;
      }
      if (curPartition < 0) {
        break;
      }
    }
  }
}
