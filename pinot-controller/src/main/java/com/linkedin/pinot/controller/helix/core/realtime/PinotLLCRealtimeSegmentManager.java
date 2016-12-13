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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaSimpleConsumerFactoryImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import javax.annotation.Nullable;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  private static final int KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS = 10000;
  protected static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments

  private static PinotLLCRealtimeSegmentManager INSTANCE = null;

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;
  private boolean _amILeader = false;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;

  public static synchronized void create(PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    create(helixResourceManager.getHelixAdmin(), helixResourceManager.getHelixClusterName(),
        helixResourceManager.getHelixZkManager(), helixResourceManager.getPropertyStore(), helixResourceManager,
        controllerConf, controllerMetrics);
  }

  private static synchronized void create(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager,
      ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created");
    }
    INSTANCE = new PinotLLCRealtimeSegmentManager(helixAdmin, clusterName, helixManager, propertyStore,
        helixResourceManager, controllerConf, controllerMetrics);
    SegmentCompletionManager.create(helixManager, INSTANCE, controllerConf, controllerMetrics);
  }

  public void start() {
    _helixManager.addControllerListener(new ControllerChangeListener() {
      @Override
      public void onControllerChange(NotificationContext changeContext) {
        onBecomeLeader();
      }
    });
  }

  protected PinotLLCRealtimeSegmentManager(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager,
      ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    _helixAdmin = helixAdmin;
    _helixManager = helixManager;
    _propertyStore = propertyStore;
    _helixResourceManager = helixResourceManager;
    _clusterName = clusterName;
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
  }

  public static PinotLLCRealtimeSegmentManager getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Not yet created");
    }
    return INSTANCE;
  }

  private void onBecomeLeader() {
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
      final List<String> instanceNames, int nReplicas, String initialOffset, String bootstrapHosts, IdealState
      idealState, boolean create) {
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
    ZNRecord znRecord = generatePartitionAssignment(topicName, nPartitions, instanceNames, nReplicas);
    writeKafkaPartitionAssignemnt(realtimeTableName, znRecord);
    setupInitialSegments(realtimeTableName, znRecord, topicName, initialOffset, bootstrapHosts, idealState, create,
        nReplicas);
  }

  // Remove all trace of LLC for this table.
  public void cleanupLLC(final String realtimeTableName) {
    // Start by removing the kafka partition assigment znode. This will prevent any new segments being created.
    ZKMetadataProvider.removeKafkaPartitionAssignmentFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Removed Kafka partition assignemnt (if any) record for {}", realtimeTableName);
    // If there are any completions in the pipeline we let them commit.
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    // Remove segments from propertystore.
    final List<String> segmentNames = getExistingSegments(realtimeTableName);
    final List<String> znodesToDelete = new ArrayList<>(segmentNames.size());
    for (String segmentName : segmentNames) {
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        final String znodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName);
        znodesToDelete.add(znodePath);
      }
    }
    _propertyStore.remove(znodesToDelete, AccessOption.PERSISTENT);
    LOGGER.info("Removed {} LLC segments from PROPERTYSTORE for {}", znodesToDelete.size(), realtimeTableName);

    // Remove segments from IdealState. The segments in the idealstate should be the same as that in propertystore,
    // but it is better to take what idealstate reports and remove the llc segments from there.
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        Set<String> allSegments = idealState.getPartitionSet();
        List<String> segmentsToRemove = new ArrayList<String>(allSegments.size());
        int removedCount = 0;
        for (String segmentName : allSegments) {
          if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
            segmentsToRemove.add(segmentName);
            removedCount++;
          }
        }
        LOGGER.info("Attempting to remove {} LLC segments from IDEALSTATE for {}", removedCount, realtimeTableName);
        for (String segmentName : segmentsToRemove) {
          idealState.getPartitionSet().remove(segmentName);
        }
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
    LOGGER.info("Removed LLC segments from IDEALSTATE (if any) for {}", realtimeTableName);
  }

  protected void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
    final String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(realtimeTableName);
    _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  public ZNRecord getKafkaPartitionAssignment(final String realtimeTableName) {
    final String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(realtimeTableName);
    return _propertyStore.get(path, null, AccessOption.PERSISTENT);
  }

  protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, String topicName, String
      initialOffset, String bootstrapHosts, IdealState idealState, boolean create, int nReplicas) {
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
    final Map<String, List<String>> partitionToServersMap = partitionAssignment.getListFields();
    final int nPartitions = partitionToServersMap.size();

    // Create one segment entry in PROPERTYSTORE for each kafka partition.
    // Any of these may already be there, so bail out clean if they are already present.
    List<String> paths = new ArrayList<>(nPartitions);
    List<ZNRecord> records = new ArrayList<>(nPartitions);
    final long now = System.currentTimeMillis();
    final int seqNum = STARTING_SEQUENCE_NUMBER;

    for (int i = 0; i < nPartitions; i++) {
      final List<String> instances = partitionToServersMap.get(Integer.toString(i));
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      final String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
      LLCSegmentName llcSegmentName = new LLCSegmentName(rawTableName, i, seqNum, now);
      final String segName = llcSegmentName.getSegmentName();

      metadata.setCreationTime(now);

      final long startOffset = getPartitionOffset(topicName, bootstrapHosts, initialOffset, i);
      LOGGER.info("Setting start offset for segment {} to {}", segName, startOffset);
      metadata.setStartOffset(startOffset);
      metadata.setEndOffset(Long.MAX_VALUE);

      metadata.setNumReplicas(instances.size());
      metadata.setTableName(rawTableName);
      metadata.setSegmentName(segName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

      ZNRecord record = metadata.toZNRecord();
      final String znodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segName);
      paths.add(znodePath);
      records.add(record);
      idealStateEntries.put(segName, instances);
    }

    writeSegmentsToPropertyStore(paths, records);
    LOGGER.info("Added {} segments to propertyStore for table {}", paths.size(), realtimeTableName);

    updateHelixIdealState(idealState, realtimeTableName, idealStateEntries, create, nReplicas);
  }

  // Update the helix idealstate when a new table is added.
  protected void updateHelixIdealState(final IdealState idealState, String realtimeTableName, final Map<String, List<String>> idealStateEntries,
      boolean create, final int nReplicas) {
    if (create) {
      addLLCRealtimeSegmentsInIdealState(idealState, idealStateEntries);
      _helixAdmin.addResource(_clusterName, realtimeTableName, idealState);
    } else {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
        @Override
        public IdealState apply(IdealState idealState) {
          idealState.setReplicas(Integer.toString(nReplicas));
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
    String propStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    return  _propertyStore.getChildNames(propStorePath, AccessOption.PERSISTENT);
  }

  protected List<ZNRecord> getExistingSegmentMetadata(String realtimeTableName) {
    String propStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    return _propertyStore.getChildren(propStorePath, null, 0);

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
    oldSegMetadata.setDownloadUrl(
        ControllerConf.constructDownloadUrl(rawTableName, committingSegmentNameStr, _controllerConf.generateVipUrl()));
    // Pull segment metadata from incoming segment and set it in zk segment metadata
    SegmentMetadataImpl segmentMetadata = extractSegmentMetadata(rawTableName, committingSegmentNameStr);
    oldSegMetadata.setCrc(Long.valueOf(segmentMetadata.getCrc()));
    oldSegMetadata.setStartTime(segmentMetadata.getTimeInterval().getStartMillis());
    oldSegMetadata.setEndTime(segmentMetadata.getTimeInterval().getEndMillis());
    oldSegMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    oldSegMetadata.setIndexVersion(segmentMetadata.getVersion());
    oldSegMetadata.setTotalRawDocs(segmentMetadata.getTotalRawDocs());

    final ZNRecord oldZnRecord = oldSegMetadata.toZNRecord();
    final String oldZnodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, committingSegmentNameStr);

    final ZNRecord partitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
    // If an LLC table is dropped (or cleaned up), we will get null here. In that case we should not be
    // creating a new segment
    if (partitionAssignment == null) {
      LOGGER.warn("Kafka partition assignment not found for {}", realtimeTableName);
      throw new RuntimeException("Kafka partition assigment not found. Not committing segment");
    }
    List<String> newInstances = partitionAssignment.getListField(Integer.toString(partitionId));

    // Construct segment metadata and idealstate for the new segment
    final int newSeqNum = oldSeqNum + 1;
    final long newStartOffset = nextOffset;
    LLCSegmentName newHolder = new LLCSegmentName(oldSegmentName.getTableName(), partitionId, newSeqNum, now);
    final String newSegmentNameStr = newHolder.getSegmentName();
    final ZNRecord newZnRecord =
        makeZnRecordForNewSegment(rawTableName, newInstances.size(), newStartOffset, newSegmentNameStr);
    final String newZnodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, newSegmentNameStr);

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

  /**
   * Extract the segment metadata file from the tar-zipped segment file that is expected to be in the
   * directory for the table.
   * Segment tar-zipped file path: DATADIR/rawTableName/segmentName
   * We extract the metadata into a file into a file in the same level,as in: DATADIR/rawTableName/segmentName.metadata
   * @param rawTableName Name of the table (not including the REALTIME extension)
   * @param segmentNameStr Name of the segment
   * @return SegmentMetadataImpl if it is able to extract the metadata file from the tar-zipped segment file.
   */
  protected SegmentMetadataImpl extractSegmentMetadata(final String rawTableName, final String segmentNameStr) {
    final String baseDir = StringUtil.join("/", _controllerConf.getDataDir(), rawTableName);
    final String segFileName = StringUtil.join("/", baseDir, segmentNameStr);
    final File segFile = new File(segFileName);
    SegmentMetadataImpl segmentMetadata;
    Path metadataPath = null;
    try {
      InputStream is = TarGzCompressionUtils
          .unTarOneFile(new FileInputStream(segFile), V1Constants.MetadataKeys.METADATA_FILE_NAME);
      metadataPath = FileSystems.getDefault().getPath(baseDir, segmentNameStr + ".metadata");
      Files.copy(is, metadataPath);
      segmentMetadata = new SegmentMetadataImpl(new File(metadataPath.toString()));
    } catch (Exception e) {
      throw new RuntimeException("Exception extacting and reading segment metadata for " + segmentNameStr, e);
    } finally {
      if (metadataPath != null) {
        FileUtils.deleteQuietly(new File(metadataPath.toString()));
      }
    }
    return segmentMetadata;
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

  protected long getKafkaPartitionOffset(KafkaStreamMetadata kafkaStreamMetadata, final String offsetCriteria,
      int partitionId) {
    final String topicName = kafkaStreamMetadata.getKafkaTopicName();
    final String bootstrapHosts = kafkaStreamMetadata.getBootstrapHosts();

    return getPartitionOffset(topicName, bootstrapHosts, offsetCriteria, partitionId);
  }

  private long getPartitionOffset(final String topicName, final String bootstrapHosts, final String offsetCriteria, int partitionId) {
    SimpleConsumerWrapper kafkaConsumer = SimpleConsumerWrapper.forPartitionConsumption(
        new KafkaSimpleConsumerFactoryImpl(), bootstrapHosts, "dummyClientId", topicName, partitionId,
        KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS);
    final long startOffset;
    try {
      startOffset = kafkaConsumer.fetchPartitionOffset(offsetCriteria, KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS);
    } catch (TimeoutException e) {
      LOGGER.warn("Timed out when fetching partition offsets for topic {} partition {}", topicName, partitionId);
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(kafkaConsumer);
    }
    return startOffset;
  }

  /**
   * Create a consuming segment for the kafka partitions that are missing one.
   *
   * @param realtimeTableName is the name of the realtime table (e.g. "table_REALTIME")
   * @param nonConsumingPartitions is a list of integers (kafka partitions that do not have a consuming segment)
   * @param llcSegments is a list of segment names in the ideal state as was observed last.
   */
  public void createConsumingSegment(String realtimeTableName, List<Integer> nonConsumingPartitions,
      List<String> llcSegments, AbstractTableConfig tableConfig) {
    final KafkaStreamMetadata kafkaStreamMetadata = new KafkaStreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    List<LLCSegmentName> segmentNames = new ArrayList<>(llcSegments.size());
    for (String segmentId: llcSegments) {
      segmentNames.add(new LLCSegmentName(segmentId));
    }

    Collections.sort(segmentNames, Collections.reverseOrder());

    Collections.sort(nonConsumingPartitions, Collections.reverseOrder());

    final ZNRecord partitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
    final int nReplicas = partitionAssignment.getListField("0").size(); // Number of replicas of partition 0

    final int nSegments = segmentNames.size();
    // The segment names are sorted in reverse order, so we will have all the segments of the highest partition
    // first, and then the next partition, and so on. In each group, the highest sequence number will appear first.
    // The nonConsumingPartitions list is also sorted in reverse order.
    // In the loop below, we set 'curPartition' to be the partition that we are trying to find the highest sequence
    // number for. If we find one, we create a CONSUMING segment with sequence X+1. If we don't find any partition,we
    // create a CONSUMING segment with STARTING_SEQUENCE_NUMER.
    // If we are done creating a CONSUMING segment for a partition, we remove it from the list of nonConsumingPartitions
    // and set curPartition to the next partition in the list.
    int segmentIndex = 0;
    while (segmentIndex < nSegments && !nonConsumingPartitions.isEmpty()) {
      final LLCSegmentName segmentName = segmentNames.get(segmentIndex);
      final int segmentPartitionId = segmentName.getPartitionId();
      final int curPartition = nonConsumingPartitions.get(0);
      try {
        if (segmentPartitionId > curPartition) {
          // The partition we need to look for next is not this one, and could be down below in the list of segments.
          // We need to skip all the segments for higher partitions until we get to curPartition (or lower).
          // Be sure not to remove 'curPartition' from nonConsumingPartitions.
          segmentIndex++;
          continue;
        } else if (segmentPartitionId < curPartition) {
          // We went past a partition that has no LLC segments. We can create one with STARTING_SEQUENCE_NUMBER
          int seqNum = STARTING_SEQUENCE_NUMBER;
          List<String> instances = partitionAssignment.getListField(Integer.toString(curPartition));
          LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", realtimeTableName, curPartition, seqNum);
          String consumerStartOffsetSpec = kafkaStreamMetadata.getKafkaConsumerProperties().get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
          long startOffset = getKafkaPartitionOffset(kafkaStreamMetadata, consumerStartOffsetSpec, curPartition);
          LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, realtimeTableName, curPartition);

          createSegment(realtimeTableName, nReplicas, curPartition, seqNum, instances, startOffset);
        } else {
          // We hit the segment with the highest sequence number for 'curPartition'. Create a consuming segment.
          int nextSeqNum = segmentName.getSequenceNumber() + 1;
          List<String> instances = partitionAssignment.getListField(Integer.toString(curPartition));
          LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", realtimeTableName, curPartition, nextSeqNum);
          long startOffset = getKafkaPartitionOffset(kafkaStreamMetadata, "smallest", curPartition);
          LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, realtimeTableName, curPartition);
          final LLCRealtimeSegmentZKMetadata oldSegMetadata = getRealtimeSegmentZKMetadata(realtimeTableName,
              segmentName.getSegmentName());
          if (startOffset < oldSegMetadata.getEndOffset()) {
            startOffset = oldSegMetadata.getEndOffset();
            LOGGER.info("Choosing newer kafka offset {} for table {} for partition {}", startOffset, realtimeTableName, curPartition);
          }
          createSegment(realtimeTableName, nReplicas, curPartition, nextSeqNum, instances, startOffset);
          segmentIndex++;
        }
      } catch (Exception e) {
        LOGGER.error("Exception creating CONSUMING segment for {} partition {}", realtimeTableName, curPartition, e);
      }
      // We land here only if we have created a CONSUMING segment for 'curPartition'. Remove it from the list, and go on
      // to the next partition (if there is one);
      nonConsumingPartitions.remove(0);
    }
  }

  private void createSegment(String realtimeTableName, int numReplicas, int partitionId, int seqNum,
      List<String> serverInstances, long startOffset) {
    LOGGER.info("Attempting to auto-create a segment for partition {} of table {}", partitionId, realtimeTableName);
    final List<String> propStorePaths = new ArrayList<>(1);
    final List<ZNRecord> propStoreEntries = new ArrayList<>(1);
    long now = System.currentTimeMillis();
    final String tableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    LLCSegmentName newSegmentName = new LLCSegmentName(tableName, partitionId, seqNum, now);
    final String newSegmentNameStr = newSegmentName.getSegmentName();
    final ZNRecord newZnRecord = makeZnRecordForNewSegment(realtimeTableName, numReplicas, startOffset,
        newSegmentNameStr);
    final String newZnodePath = ZKMetadataProvider
        .constructPropertyStorePathForSegment(realtimeTableName, newSegmentNameStr);
    propStorePaths.add(newZnodePath);
    propStoreEntries.add(newZnRecord);

    writeSegmentsToPropertyStore(propStorePaths, propStoreEntries);

    updateHelixIdealState(realtimeTableName, serverInstances, null, newSegmentNameStr);

    LOGGER.info("Successful auto-create of CONSUMING segment {}", newSegmentNameStr);
  }

  private ZNRecord makeZnRecordForNewSegment(String realtimeTableName, int numReplicas, long startOffset,
      String newSegmentNameStr) {
    final LLCRealtimeSegmentZKMetadata newSegMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(System.currentTimeMillis());
    newSegMetadata.setStartOffset(startOffset);
    newSegMetadata.setNumReplicas(numReplicas);
    newSegMetadata.setTableName(realtimeTableName);
    newSegMetadata.setSegmentName(newSegmentNameStr);
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return newSegMetadata.toZNRecord();
  }

  /**
   * An instance is reporting that it has stopped consuming a kafka topic due to some error.
   * Mark the state of the segment to be OFFLINE in idealstate.
   * When all replicas of this segment are marked offline, the ValidationManager, in its next
   * run, will auto-create a new segment with the appropriate offset.
   * See {@link #createConsumingSegment(String, List, List, AbstractTableConfig)}
  */
  public void segmentStoppedConsuming(final LLCSegmentName segmentName, final String instance) {
    String rawTableName = segmentName.getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(rawTableName);
    final String segmentNameStr = segmentName.getSegmentName();
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        idealState.setPartitionState(segmentNameStr, instance,
            CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.OFFLINE);
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentNameStr);
        LOGGER.info("Attempting to mark {} offline. Current map:{}", segmentNameStr, instanceStateMap.toString());
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
    LOGGER.info("Successfully marked {} offline for instance {} since it stopped consuming", segmentNameStr, instance);
  }

  /**
   * Update the kafka partitions as necessary to accommodate changes in number of replicas, number of tenants or
   * number of kafka partitions. As new segments are assigned, they will obey the new kafka partition assignment.
   *
   * @param realtimeTableName name of the realtime table
   * @param tableConfig tableConfig from propertystore
   */
  public void updateKafkaPartitionsIfNecessary(String realtimeTableName, AbstractTableConfig tableConfig) {
    final ZNRecord partitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
    final Map<String, List<String>> partitionToServersMap = partitionAssignment.getListFields();
    final KafkaStreamMetadata kafkaStreamMetadata = new KafkaStreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());

    final String realtimeServerTenantName =
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tableConfig.getTenantConfig().getServer());
    final List<String> currentInstances = getInstances(realtimeServerTenantName);

    // Previous partition count is what we find in the Kafka partition assignment znode.
    // Get the current partition count from Kafka.
    final int prevPartitionCount = partitionToServersMap.size();
    int currentPartitionCount = -1;
    try {
      currentPartitionCount = getKafkaPartitionCount(kafkaStreamMetadata);
    } catch (Exception e) {
      LOGGER.warn("Could not get partition count for {}. Leaving kafka partition count at {}", realtimeTableName, currentPartitionCount);
      return;
    }

    // Previous instance set is what we find in the Kafka partition assignment znode (values of the map entries)
    final Set<String> prevInstances = new HashSet<>(currentInstances.size());
    for (List<String> servers : partitionToServersMap.values()) {
      prevInstances.addAll(servers);
    }

    final int prevReplicaCount = partitionToServersMap.entrySet().iterator().next().getValue().size();
    final int currentReplicaCount = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());

    boolean updateKafkaAssignment = false;

    if (!prevInstances.equals(new HashSet<String>(currentInstances))) {
      LOGGER.info("Detected change in instances for table {}", realtimeTableName);
      updateKafkaAssignment = true;
    }

    if (prevPartitionCount != currentPartitionCount) {
      LOGGER.info("Detected change in Kafka partition count for table {} from {} to {}", realtimeTableName, prevPartitionCount, currentPartitionCount);
      updateKafkaAssignment = true;
    }

    if (prevReplicaCount != currentReplicaCount) {
      LOGGER.info("Detected change in per-partition replica count for table {} from {} to {}", realtimeTableName, prevReplicaCount, currentReplicaCount);
      updateKafkaAssignment = true;
    }

    if (!updateKafkaAssignment) {
      LOGGER.info("Not updating Kafka partition assignment for table {}");
      return;
    }

    // Generate new kafka partition assignment and update the znode
    if (currentInstances.size() < currentReplicaCount) {
      LOGGER.error("Cannot have {} replicas in {} instances for {}.Not updating partition assignment", currentReplicaCount, currentInstances.size(), realtimeTableName);
      return;
    }
    ZNRecord newPartitionAssignment = generatePartitionAssignment(kafkaStreamMetadata.getKafkaTopicName(), currentPartitionCount, currentInstances, currentReplicaCount);
    writeKafkaPartitionAssignemnt(realtimeTableName, newPartitionAssignment);
    LOGGER.info("Successfully updated Kafka partition assignment for table {}");
  }

  /*
   * Generate partition assignment. An example znode for 8 kafka partitions and and 6 realtime servers looks as below
   * in zookeeper.
   * {
     "id":"KafkaTopicName"
     ,"simpleFields":{
     }
     ,"listFields":{
       "0":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
       ,"1":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
       ,"2":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
       ,"3":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
       ,"4":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
       ,"5":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
       ,"6":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
       ,"7":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
     }
     ,"mapFields":{
     }
   }
   */
  private ZNRecord generatePartitionAssignment(String topicName, int nPartitions, List<String> instanceNames,
      int nReplicas) {
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
    return znRecord;
  }

  protected int getKafkaPartitionCount(KafkaStreamMetadata kafkaStreamMetadata) {
    return PinotTableIdealStateBuilder.getPartitionCount(kafkaStreamMetadata);
  }

  protected List<String> getInstances(String tenantName) {
    return _helixAdmin.getInstancesInClusterWithTag(_clusterName, tenantName);
  }
}
