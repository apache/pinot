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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.PartitionAssignmentGenerator;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.events.MetadataEventNotifierFactory;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdateManager;
import com.linkedin.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdater;
import com.linkedin.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdaterParams;
import com.linkedin.pinot.controller.util.SegmentCompletionUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.core.realtime.segment.ConsumingSegmentAssignmentStrategy;
import com.linkedin.pinot.core.realtime.segment.RealtimeSegmentAssignmentStrategy;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumer;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.IntRange;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  private static final int KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS = 10000;
  protected static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments
  protected static final long END_OFFSET_FOR_CONSUMING_SEGMENTS = Long.MAX_VALUE;
  private static final int NUM_LOCKS = 4;

  private static final String METADATA_TEMP_DIR_SUFFIX = ".metadata.tmp";
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";
  // TODO: make this configurable with default set to 10
  /**
   * After step 1 of segment completion is done,
   * this is the max time until which step 3 is allowed to complete.
   * See {@link PinotLLCRealtimeSegmentManager#commitSegmentMetadata(String, String, long, SegmentCompletionProtocol.Request.Params)} for explanation of steps 1 2 3
   * This includes any backoffs and retries for the steps 2 and 3
   * The segment will be eligible for repairs by the validation manager, if the time  exceeds this value
   */
  private static int MAX_SEGMENT_COMPLETION_TIME_MINS = 10;

  private static PinotLLCRealtimeSegmentManager INSTANCE = null;

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;
  private boolean _amILeader = false;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;
  private final Lock[] _idealstateUpdateLocks;
  private final TableConfigCache _tableConfigCache;
  private final PartitionAssignmentGenerator _partitionAssignmentGenerator;
  private final FlushThresholdUpdateManager _flushThresholdUpdateManager;

  public boolean getIsSplitCommitEnabled() {
    return _controllerConf.getAcceptSplitCommit();
  }

  public String getControllerVipUrl() {
    return _controllerConf.generateVipUrl();
  }

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
    _helixManager.addControllerListener(changeContext -> onBecomeLeader());
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
    _idealstateUpdateLocks = new Lock[NUM_LOCKS];
    for (int i = 0; i < NUM_LOCKS; i++) {
      _idealstateUpdateLocks[i] = new ReentrantLock();
    }
    _tableConfigCache = new TableConfigCache(_propertyStore);
    _partitionAssignmentGenerator = new PartitionAssignmentGenerator(_helixManager);
    _flushThresholdUpdateManager = new FlushThresholdUpdateManager();
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
      } else {
        // We already had leadership, nothing to do.
        LOGGER.info("Already leader. Duplicate notification");
      }
    } else {
      _amILeader = false;
      LOGGER.info("Lost leadership");
    }
  }

  protected boolean isLeader() {
    return _helixManager.isLeader();
  }

  protected boolean isConnected() {
    return _helixManager.isConnected();
  }

  /**
   *
   * @param tableConfig
   * @param emptyIdealState may contain HLC segments if both HLC and LLC are configured
   */
  public void setupNewTable(TableConfig tableConfig, IdealState emptyIdealState) throws InvalidConfigException {
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    int partitionCount = getKafkaPartitionCount(streamMetadata);
    List<String> currentSegments = getExistingSegments(tableConfig.getTableName());
    // Make sure that there are no low-level segments existing.
    if (currentSegments != null) {
      for (String segment : currentSegments) {
        if (!SegmentName.isHighLevelConsumerSegmentName(segment)) {
          // For now, we don't support changing of kafka partitions, or otherwise re-creating the low-level
          // realtime segments for any other reason.
          throw new RuntimeException("Low-level segments already exist for table " + tableConfig.getTableType());
        }
      }
    }
    IdealState idealState = setupTable(tableConfig, emptyIdealState, partitionCount);
    setTableIdealState(tableConfig.getTableName(), idealState);
  }

  // Remove all trace of LLC for this table.
  public void cleanupLLC(final String realtimeTableName) {
    // Start by removing the kafka partition assigment znode. This will prevent any new segments being created.
    ZKMetadataProvider.removeKafkaPartitionAssignmentFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Removed Kafka partition assignment (if any) record for {}", realtimeTableName);
    // If there are any completions in the pipeline we let them commit.
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
    final List<String> segmentsToRemove = new ArrayList<String>();
    Set<String> allSegments = idealState.getPartitionSet();
    int removeCount = 0;
    for (String segmentName : allSegments) {
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        segmentsToRemove.add(segmentName);
        removeCount++;
      }
    }
    LOGGER.info("Attempting to remove {} LLC segments of table {}", removeCount, realtimeTableName);

    _helixResourceManager.deleteSegments(realtimeTableName, segmentsToRemove);
  }

  private SegmentPartitionMetadata getPartitionMetadataFromTableConfig(String tableName, int numPartitions, int partitionId) {
    Map<String, ColumnPartitionMetadata> partitionMetadataMap = new HashMap<>();
    if (_propertyStore == null) {
      return null;
    }
    TableConfig tableConfig = getRealtimeTableConfig(tableName);
    SegmentPartitionMetadata partitionMetadata = null;
    SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (partitionConfig != null && partitionConfig.getColumnPartitionMap() != null
        && partitionConfig.getColumnPartitionMap().size() > 0) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = partitionConfig.getColumnPartitionMap();
      for (Map.Entry<String, ColumnPartitionConfig> entry : columnPartitionMap.entrySet()) {
        String column = entry.getKey();
        ColumnPartitionConfig columnPartitionConfig = entry.getValue();
        partitionMetadataMap.put(column, new ColumnPartitionMetadata(columnPartitionConfig.getFunctionName(),
            numPartitions, Collections.singletonList(new IntRange(partitionId))));
      }
      partitionMetadata = new SegmentPartitionMetadata(partitionMetadataMap);
    }
    return partitionMetadata;
  }

  /**
   * Given a segment metadata, build the finalized version of segment partition metadata. This partition metadata will
   * be written as a part of SegmentZKMetadata.
   * @param segmentMetadata Segment metadata
   * @return
   */
  private SegmentPartitionMetadata getPartitionMetadataFromSegmentMetadata(SegmentMetadataImpl segmentMetadata) {
    Map<String, ColumnPartitionMetadata> partitionMetadataMap = new HashMap<>();
    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      String columnName = entry.getKey();
      ColumnMetadata columnMetadata = entry.getValue();
      // Check if the column metadata contains the partition information
      if (columnMetadata.getPartitionFunction() != null && columnMetadata.getPartitionRanges() != null) {
        partitionMetadataMap.put(columnName,
            new ColumnPartitionMetadata(columnMetadata.getPartitionFunction().toString(),
                columnMetadata.getNumPartitions(), columnMetadata.getPartitionRanges()));
      }
    }
    return new SegmentPartitionMetadata(partitionMetadataMap);
  }

  protected List<String> getExistingSegments(String realtimeTableName) {
    String propStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    return  _propertyStore.getChildNames(propStorePath, AccessOption.PERSISTENT);
  }

  protected boolean writeSegmentToPropertyStore(String znodePath, ZNRecord znRecord, final String realtimeTableName,
      int expectedVersion) {
    boolean success = _propertyStore.set(znodePath, znRecord, expectedVersion, AccessOption.PERSISTENT);
    if (!success) {
      LOGGER.error("Failed to write segment to property store at {} for table {}. Expected zookeeper version number: {}",
          znodePath, realtimeTableName, expectedVersion);
      return false;
    }
    return success;
  }

  protected boolean writeSegmentToPropertyStore(String znodePath, ZNRecord znRecord, final String realtimeTableName) {
    boolean success = _propertyStore.set(znodePath, znRecord, AccessOption.PERSISTENT);
    if (!success) {
      LOGGER.error("Failed to write segment to property store at {} for table {}.", znodePath, realtimeTableName);
    }
    return success;
  }

  protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records, final String realtimeTableName) {
    try {
      _propertyStore.setChildren(paths, records, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Failed to update idealstate for table {} for paths {}", realtimeTableName, paths, e);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1);
      throw e;
    }
  }

  protected IdealState getTableIdealState(String realtimeTableName) {
    return HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
  }

  protected void setTableIdealState(String realtimeTableName, IdealState idealState) {
    _helixAdmin.setResourceIdealState(_clusterName, realtimeTableName, idealState);
  }

  public boolean commitSegmentFile(String tableName, String segmentLocation, String segmentName) {
    File segmentFile = convertURIToSegmentLocation(segmentLocation);

    File baseDir = new File(_controllerConf.getDataDir());
    File tableDir = new File(baseDir, tableName);
    File fileToMoveTo = new File(tableDir, segmentName);

    if (!isConnected() || !isLeader()) {
      // We can potentially log a different value than what we saw ....
      LOGGER.warn("Lost leadership while committing segment file {}, {} for table {}: isLeader={}, isConnected={}",
          segmentName, segmentLocation, tableName, isLeader(), isConnected());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_NOT_LEADER, 1L);
      return false;
    }

    try {
      com.linkedin.pinot.common.utils.FileUtils.moveFileWithOverwrite(segmentFile, fileToMoveTo);
    } catch (Exception e) {
      LOGGER.error("Could not move {} to {}", segmentFile, segmentName, e);
      return false;
    }
    for (File file : tableDir.listFiles()) {
      if (file.getName().startsWith(SegmentCompletionUtils.getSegmentNamePrefix(segmentName))) {
        LOGGER.warn("Deleting " + file);
        FileUtils.deleteQuietly(file);
      }
    }
    return true;
  }

  private static File convertURIToSegmentLocation(String segmentLocation) {
    try {
      URI uri = new URI(segmentLocation);
      return new File(uri.getPath());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not convert URI " + segmentLocation + " to segment location", e);
    }
  }

  /**
   * This method is invoked after the realtime segment is uploaded but before a response is sent to the server.
   * It updates the propertystore segment metadata from IN_PROGRESS to DONE, and also creates new propertystore
   * records for new segments, and puts them in idealstate in CONSUMING state.
   *
   * @param rawTableName Raw table name
   * @param reqParams
   * @return boolean
   */
  public boolean commitSegmentMetadata(String rawTableName, SegmentCompletionProtocol.Request.Params reqParams) {
    final String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TableConfig tableConfig = getRealtimeTableConfig(realtimeTableName);
    if (tableConfig == null) {
      LOGGER.warn("Did not find table config for table {}", realtimeTableName);
      return false;
    }

    /*
     * Update zookeeper in 3 steps.
     *
     * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE
     * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
     * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
     */

    final String committingSegmentNameStr = reqParams.getSegmentName();
    final long nextOffset = reqParams.getOffset();
    final LLCSegmentName committingLLCSegmentName = new LLCSegmentName(committingSegmentNameStr);
    final int partitionId = committingLLCSegmentName.getPartitionId();
    final int newSeqNum = committingLLCSegmentName.getSequenceNumber() + 1;
    final long now = System.currentTimeMillis();

    LLCSegmentName newLLCSegmentName = new LLCSegmentName(committingLLCSegmentName.getTableName(), partitionId, newSeqNum, now);
    String newSegmentNameStr = newLLCSegmentName.getSegmentName();

    IdealState idealState = getTableIdealState(realtimeTableName);
    Preconditions.checkState(idealState.getInstanceStateMap(committingSegmentNameStr)
        .containsValue(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE));
    int numPartitions = _partitionAssignmentGenerator.getNumPartitionsFromIdealState(idealState);

    PartitionAssignment partitionAssignment;
    try {
      partitionAssignment = _partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, numPartitions);
    } catch (InvalidConfigException e) {
      LOGGER.error("Exception when generating partition assignment for table {} and numPartitions {}",
          realtimeTableName, numPartitions, e);
      return false;
    }

    // If an LLC table is dropped (or cleaned up), we will get null here. In that case we should not be creating a new segment
    if (partitionAssignment == null) {
      LOGGER.warn("Kafka partition assignment not found for {}", realtimeTableName);
      throw new RuntimeException("Kafka partition assignment not found. Not committing segment");
    }

    // Step-1
    boolean success = updateOldSegmentMetadataZNRecord(realtimeTableName, committingLLCSegmentName, nextOffset);
    if (!success) {
      return false;
    }

    // Step-2
    success = createNewSegmentMetadataZNRecord(tableConfig, committingLLCSegmentName, newLLCSegmentName, nextOffset,
        partitionAssignment, reqParams.getSegmentSizeBytes());
    if (!success) {
      return false;
    }

    // Step-3
    // TODO Introduce a controller failure here for integration testing

    // When multiple segments of the same table complete around the same time it is possible that
    // the idealstate udpate fails due to contention. We serialize the updates to the idealstate
    // to reduce this contention. We may still contend with RetentionManager, or other updates
    // to idealstate from other controllers, but then we have the retry mechanism to get around that.
    // hash code can be negative, so make sure we are getting a positive lock index
    int lockIndex = (realtimeTableName.hashCode() & Integer.MAX_VALUE) % NUM_LOCKS;
    Lock lock = _idealstateUpdateLocks[lockIndex];
    try {
      lock.lock();
      updateIdealStateOnSegmentCompletion(realtimeTableName, committingSegmentNameStr, newSegmentNameStr, partitionAssignment);
      LOGGER.info("Changed {} to ONLINE and created {} in CONSUMING", committingSegmentNameStr, newSegmentNameStr);
    } finally {
      lock.unlock();
    }

    // Trigger the metadata event notifier
    notifyOnSegmentFlush(realtimeTableName);

    return true;
  }


  /**
   * Update segment metadata of committing segment
   * @param realtimeTableName - table name for which segment is being committed
   * @param committingLLCSegmentName - name of the segment being committed
   * @param nextOffset - the end offset for this committing segment
   * @return
   */
  protected boolean updateOldSegmentMetadataZNRecord(String realtimeTableName, LLCSegmentName committingLLCSegmentName,
      long nextOffset) {

    String committingSegmentNameStr = committingLLCSegmentName.getSegmentName();
    Stat stat = new Stat();
    final LLCRealtimeSegmentZKMetadata committingSegmentMetadata = getRealtimeSegmentZKMetadata(realtimeTableName,
        committingSegmentNameStr, stat);

    if (committingSegmentMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.IN_PROGRESS) {
      LOGGER.warn("Status of segment metadata {} has already been changed by other controller for table {}: Status={}",
          committingSegmentNameStr, realtimeTableName, committingSegmentMetadata.getStatus());
      return false;
    }

    // TODO: set number of rows to end consumption in new segment metadata, based on memory used and number of rows from old segment
    committingSegmentMetadata.setEndOffset(nextOffset);
    committingSegmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    committingSegmentMetadata.setDownloadUrl(
        ControllerConf.constructDownloadUrl(rawTableName, committingSegmentNameStr, _controllerConf.generateVipUrl()));
    // Pull segment metadata from incoming segment and set it in zk segment metadata
    SegmentMetadataImpl segmentMetadata = extractSegmentMetadata(rawTableName, committingSegmentNameStr);
    committingSegmentMetadata.setCrc(Long.valueOf(segmentMetadata.getCrc()));
    committingSegmentMetadata.setStartTime(segmentMetadata.getTimeInterval().getStartMillis());
    committingSegmentMetadata.setEndTime(segmentMetadata.getTimeInterval().getEndMillis());
    committingSegmentMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    committingSegmentMetadata.setIndexVersion(segmentMetadata.getVersion());
    committingSegmentMetadata.setTotalRawDocs(segmentMetadata.getTotalRawDocs());
    committingSegmentMetadata.setPartitionMetadata(getPartitionMetadataFromSegmentMetadata(segmentMetadata));

    final ZNRecord oldZnRecord = committingSegmentMetadata.toZNRecord();
    final String oldZnodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, committingSegmentNameStr);

    if (!isConnected() || !isLeader()) {
      // We can potentially log a different value than what we saw ....
      LOGGER.warn("Lost leadership while committing segment metadata for {} for table {}: isLeader={}, isConnected={}",
          committingSegmentNameStr, realtimeTableName, isLeader(), isConnected());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_NOT_LEADER, 1L);
      return false;
    }
    boolean success = writeSegmentToPropertyStore(oldZnodePath, oldZnRecord, realtimeTableName, stat.getVersion());
    if (!success) {
      LOGGER.warn("Fail to write old segment to property store for {} for table {}: isLeader={}, isConnected={}",
          committingSegmentNameStr, realtimeTableName, isLeader(), isConnected());
    }
    return success;
  }

  /**
   * Creates segment metadata for next sequence number from the segment just committed
   * @param realtimeTableConfig - table config of the segment for which new metadata is being created
   * @param newLLCSegmentName - new segment name
   * @param nextOffset - start offset for new segment
   * @param partitionAssignment - stream partition assignment for this table
   * @param committingSegmentSizeBytes - size of the committing segment
   * @return
   */
  protected boolean createNewSegmentMetadataZNRecord(TableConfig realtimeTableConfig,
      LLCSegmentName committingLLCSegmentName, LLCSegmentName newLLCSegmentName, long nextOffset,
      PartitionAssignment partitionAssignment, long committingSegmentSizeBytes) {

    String realtimeTableName = realtimeTableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);

    LLCRealtimeSegmentZKMetadata committingSegmentMetadata = null;
    if (committingLLCSegmentName != null) {
      committingSegmentMetadata =
          getRealtimeSegmentZKMetadata(realtimeTableName, committingLLCSegmentName.getSegmentName(), null);
    }

    int partitionId = newLLCSegmentName.getPartitionId();
    int numReplicas = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId)).size();
    ZNRecord newZnRecord = makeZnRecordForNewSegment(rawTableName, numReplicas, nextOffset, newLLCSegmentName,
        partitionAssignment.getNumPartitions());
    final LLCRealtimeSegmentZKMetadata newSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata(newZnRecord);

    FlushThresholdUpdater flushThresholdUpdater =
        _flushThresholdUpdateManager.getFlushThresholdUpdater(realtimeTableConfig);
    FlushThresholdUpdaterParams params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentSizeBytes(committingSegmentSizeBytes);
    params.setPartitionAssignment(partitionAssignment);
    params.setCommittingSegmentZkMetadata(committingSegmentMetadata);
    flushThresholdUpdater.updateFlushThreshold(newSegmentZKMetadata, params);

    newZnRecord = newSegmentZKMetadata.toZNRecord();

    final String newSegmentNameStr = newLLCSegmentName.getSegmentName();
    final String newZnodePath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, newSegmentNameStr);

    if (!isConnected() || !isLeader()) {
      // We can potentially log a different value than what we saw ....
      LOGGER.warn("Lost leadership while committing new segment metadata for {} for table {}: isLeader={}, isConnected={}",
          newSegmentNameStr, rawTableName, isLeader(), isConnected());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_NOT_LEADER, 1L);
      return false;
    }

    boolean success = writeSegmentToPropertyStore(newZnodePath, newZnRecord, realtimeTableName);
    if (!success) {
      LOGGER.warn("Fail to write new segment to property store for {} for table {}: isLeader={}, isConnected={}",
          newSegmentNameStr, rawTableName, isLeader(), isConnected());
    }
    return success;
  }

  /**
   * Helper function to return cached table config.
   *
   * @param tableName name of the table
   * @return table configuration that reflects the most recent version
   */
  protected TableConfig getRealtimeTableConfig(String tableName) {
    TableConfig tableConfig;
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    try {
      tableConfig = _tableConfigCache.getTableConfig(tableNameWithType);
    } catch (ExecutionException e) {
      LOGGER.warn("Exception happened while loading the table config ({}) from the property store to the cache.",
          tableNameWithType, e);
      return null;
    }
    return tableConfig;
  }


  public long getCommitTimeoutMS(String tableName) {
    long commitTimeoutMS = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
    if (_propertyStore == null) {
      return commitTimeoutMS;
    }
    TableConfig tableConfig = getRealtimeTableConfig(tableName);
    final Map<String, String> streamConfigs = tableConfig.getIndexingConfig().getStreamConfigs();
    if (streamConfigs != null && streamConfigs.containsKey(
        CommonConstants.Helix.DataSource.Realtime.SEGMENT_COMMIT_TIMEOUT_SECONDS)) {
      final String commitTimeoutSecondsStr =
          streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.SEGMENT_COMMIT_TIMEOUT_SECONDS);
      try {
        return TimeUnit.MILLISECONDS.convert(Integer.parseInt(commitTimeoutSecondsStr), TimeUnit.SECONDS);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse flush size of {}", commitTimeoutSecondsStr, e);
        return commitTimeoutMS;
      }
    }
    return commitTimeoutMS;
  }

  /**
   * Extract the segment metadata files from the tar-zipped segment file that is expected to be in the directory for the
   * table.
   * <p>Segment tar-zipped file path: DATADIR/rawTableName/segmentName.
   * <p>We extract the metadata.properties and creation.meta into a temporary metadata directory:
   * DATADIR/rawTableName/segmentName.metadata.tmp, and load metadata from there.
   *
   * @param rawTableName Name of the table (not including the REALTIME extension)
   * @param segmentNameStr Name of the segment
   * @return SegmentMetadataImpl if it is able to extract the metadata file from the tar-zipped segment file.
   */
  protected SegmentMetadataImpl extractSegmentMetadata(final String rawTableName, final String segmentNameStr) {
    String baseDirStr = StringUtil.join("/", _controllerConf.getDataDir(), rawTableName);
    String segFileStr = StringUtil.join("/", baseDirStr, segmentNameStr);
    String tempMetadataDirStr = StringUtil.join("/", baseDirStr, segmentNameStr + METADATA_TEMP_DIR_SUFFIX);
    File tempMetadataDir = new File(tempMetadataDirStr);

    try {
      Preconditions.checkState(tempMetadataDir.mkdirs(), "Failed to create directory: %s", tempMetadataDirStr);

      // Extract metadata.properties
      InputStream metadataPropertiesInputStream =
          TarGzCompressionUtils.unTarOneFile(new FileInputStream(new File(segFileStr)),
              V1Constants.MetadataKeys.METADATA_FILE_NAME);
      Preconditions.checkNotNull(metadataPropertiesInputStream, "%s does not exist",
          V1Constants.MetadataKeys.METADATA_FILE_NAME);
      Path metadataPropertiesPath =
          FileSystems.getDefault().getPath(tempMetadataDirStr, V1Constants.MetadataKeys.METADATA_FILE_NAME);
      Files.copy(metadataPropertiesInputStream, metadataPropertiesPath);

      // Extract creation.meta
      InputStream creationMetaInputStream =
          TarGzCompressionUtils.unTarOneFile(new FileInputStream(new File(segFileStr)),
              V1Constants.SEGMENT_CREATION_META);
      Preconditions.checkNotNull(creationMetaInputStream, "%s does not exist", V1Constants.SEGMENT_CREATION_META);
      Path creationMetaPath = FileSystems.getDefault().getPath(tempMetadataDirStr, V1Constants.SEGMENT_CREATION_META);
      Files.copy(creationMetaInputStream, creationMetaPath);

      // Load segment metadata
      return new SegmentMetadataImpl(tempMetadataDir);
    } catch (Exception e) {
      throw new RuntimeException("Exception extracting and reading segment metadata for " + segmentNameStr, e);
    } finally {
      FileUtils.deleteQuietly(tempMetadataDir);
    }
  }

  public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName, Stat stat) {
    ZNRecord znRecord = _propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName), stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      LOGGER.error("Segment metadata not found for table {}, segment {}. (can happen during table drop)", realtimeTableName, segmentName);
      throw new RuntimeException("Segment metadata not found for table " + realtimeTableName + " segment " + segmentName);
    }
    return new LLCRealtimeSegmentZKMetadata(znRecord);
  }

  protected long getKafkaPartitionOffset(StreamMetadata streamMetadata, final String offsetCriteria,
      int partitionId) {
    return getPartitionOffset(offsetCriteria, partitionId, streamMetadata);
  }

  private long getPartitionOffset(final String offsetCriteria, int partitionId,
      StreamMetadata streamMetadata) {
    KafkaOffsetFetcher kafkaOffsetFetcher = new KafkaOffsetFetcher(offsetCriteria, partitionId, streamMetadata);
    try {
      RetryPolicies.fixedDelayRetryPolicy(3, 1000L).attempt(kafkaOffsetFetcher);
      return kafkaOffsetFetcher.getOffset();
    } catch (Exception e) {
      Exception fetcherException = kafkaOffsetFetcher.getException();
      LOGGER.error("Could not get offset for topic {} partition {}, criteria {}",
          streamMetadata.getKafkaTopicName(), partitionId, offsetCriteria, fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }


  private long getBetterStartOffsetIfNeeded(final String realtimeTableName, final int partition,
      final LLCSegmentName latestSegment, final long oldestOffsetInKafka, final int nextSeqNum) {
    final LLCRealtimeSegmentZKMetadata oldSegMetadata =
        getRealtimeSegmentZKMetadata(realtimeTableName, latestSegment.getSegmentName(), null);
    CommonConstants.Segment.Realtime.Status status = oldSegMetadata.getStatus();
    long segmentStartOffset = oldestOffsetInKafka;
    final long prevSegStartOffset = oldSegMetadata.getStartOffset();  // Offset at which the prev segment intended to start consuming
    if (status.equals(CommonConstants.Segment.Realtime.Status.IN_PROGRESS)) {
      if (oldestOffsetInKafka <= prevSegStartOffset) {
        // We still have the same start offset available, re-use it.
        segmentStartOffset = prevSegStartOffset;
        LOGGER.info("Choosing previous segment start offset {} for table {} for partition {}, sequence {}",
            oldestOffsetInKafka,
            realtimeTableName, partition, nextSeqNum);
      } else {
        // There is data loss.
        LOGGER.warn("Data lost from kafka offset {} to {} for table {} partition {} sequence {}",
            prevSegStartOffset, oldestOffsetInKafka, realtimeTableName, partition, nextSeqNum);
        // Start from the earliest offset in kafka
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_KAFKA_DATA_LOSS, 1);
      }
    } else {
      // Status must be DONE, so we have a valid end-offset for the previous segment
      final long prevSegEndOffset = oldSegMetadata.getEndOffset();  // Will be 0 if the prev segment was not completed.
      if (oldestOffsetInKafka < prevSegEndOffset) {
        // We don't want to create a segment that overlaps in data with the prev segment. We know that the previous
        // segment's end offset is available in Kafka, so use that.
        segmentStartOffset = prevSegEndOffset;
        LOGGER.info("Choosing newer kafka offset {} for table {} for partition {}, sequence {}", oldestOffsetInKafka,
            realtimeTableName, partition, nextSeqNum);
      } else if (oldestOffsetInKafka > prevSegEndOffset) {
        // Kafka's oldest offset is greater than the end offset of the prev segment, so there is data loss.
        LOGGER.warn("Data lost from kafka offset {} to {} for table {} partition {} sequence {}", prevSegEndOffset,
            oldestOffsetInKafka, realtimeTableName, partition, nextSeqNum);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_KAFKA_DATA_LOSS, 1);
      } else {
        // The two happen to be equal. A rarity, so log it.
        LOGGER.info("Kafka earliest offset {} is the same as new segment start offset", oldestOffsetInKafka);
      }
    }
    return segmentStartOffset;
  }

  private ZNRecord makeZnRecordForNewSegment(String realtimeTableName, int numReplicas, long startOffset,
      LLCSegmentName newSegmentName, int numPartitions) {
    final LLCRealtimeSegmentZKMetadata newSegMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(System.currentTimeMillis());
    newSegMetadata.setStartOffset(startOffset);
    newSegMetadata.setEndOffset(END_OFFSET_FOR_CONSUMING_SEGMENTS);
    newSegMetadata.setNumReplicas(numReplicas);
    newSegMetadata.setTableName(realtimeTableName);
    newSegMetadata.setSegmentName(newSegmentName.getSegmentName());
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

    // Add the partition metadata if available.
    SegmentPartitionMetadata partitionMetadata = getPartitionMetadataFromTableConfig(realtimeTableName,
        numPartitions, newSegmentName.getPartitionId());
    if (partitionMetadata != null) {
      newSegMetadata.setPartitionMetadata(partitionMetadata);
    }

    return newSegMetadata.toZNRecord();
  }

  /**
   * An instance is reporting that it has stopped consuming a kafka topic due to some error.
   * Mark the state of the segment to be OFFLINE in idealstate.
   * When all replicas of this segment are marked offline, the ValidationManager, in its next
   * run, will auto-create a new segment with the appropriate offset.
   */
  public void segmentStoppedConsuming(final LLCSegmentName segmentName, final String instance) {
    String rawTableName = segmentName.getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    final String segmentNameStr = segmentName.getSegmentName();
    try {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
        @Override
        public IdealState apply(IdealState idealState) {
          idealState.setPartitionState(segmentNameStr, instance,
              CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.OFFLINE);
          Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentNameStr);
          LOGGER.info("Attempting to mark {} offline. Current map:{}", segmentNameStr, instanceStateMap.toString());
          return idealState;
        }
      }, RetryPolicies.exponentialBackoffRetryPolicy(10, 500L, 1.2f));
    } catch (Exception e) {
      LOGGER.error("Failed to update idealstate for table {} instance {} segment {}", realtimeTableName, instance,
          segmentNameStr, e);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1);
      throw e;
    }
    LOGGER.info("Successfully marked {} offline for instance {} since it stopped consuming", segmentNameStr, instance);
  }

  /**
   * Helper method to trigger metadata event notifier
   * @param tableName a table name
   */
  private void notifyOnSegmentFlush(String tableName) {
    final MetadataEventNotifierFactory metadataEventNotifierFactory =
        MetadataEventNotifierFactory.loadFactory(_controllerConf.subset(METADATA_EVENT_NOTIFIER_PREFIX));
    final TableConfig tableConfig = getRealtimeTableConfig(tableName);
    metadataEventNotifierFactory.create().notifyOnSegmentFlush(tableConfig);
  }

  protected int getKafkaPartitionCount(StreamMetadata streamMetadata) {
    return PinotTableIdealStateBuilder.getPartitionCount(streamMetadata);
  }

  /**
   * Given a table name, returns a list of metadata for all segments of that table from the property store
   * @param tableNameWithType
   * @return
   */
  @VisibleForTesting
  protected List<LLCRealtimeSegmentZKMetadata> getAllSegmentMetadata(String tableNameWithType) {
    return ZKMetadataProvider.getLLCRealtimeSegmentZKMetadataListForTable(_helixManager.getHelixPropertyStore(),
        tableNameWithType);
  }

  /**
   * Gets latest 2 metadata. We need only the 2 latest metadata for each partition in order to perform repairs
    * @param tableNameWithType
   * @return
   */
  @VisibleForTesting
  protected Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> getLatestMetadata(String tableNameWithType) {
    List<LLCRealtimeSegmentZKMetadata> metadatas = getAllSegmentMetadata(tableNameWithType);

    Comparator<LLCRealtimeSegmentZKMetadata> comparator = (o1, o2) -> {
      LLCSegmentName s1 = new LLCSegmentName(o1.getSegmentName());
      LLCSegmentName s2 = new LLCSegmentName(o2.getSegmentName());
      return s2.compareTo(s1);
    };

    Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> partitionToLatestSegments = new HashMap<>();

    for (LLCRealtimeSegmentZKMetadata metadata : metadatas) {
      LLCSegmentName segmentName = new LLCSegmentName(metadata.getSegmentName());
      final int partitionId = segmentName.getPartitionId();
      MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata> latestSegments = partitionToLatestSegments.get(partitionId);
      if (latestSegments == null) {
        latestSegments = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(2).create();
        partitionToLatestSegments.put(partitionId, latestSegments);
      }
      latestSegments.offer(metadata);
    }

    return partitionToLatestSegments;
  }

  /**
   * Validates llc segments in ideal state and repairs them if necessary
   *
   * During segment commit, we update zookeeper in 3 steps
   * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE
   * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
   * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
   *
   * The controller may fail between these three steps.
   * So when validation manager runs, it needs to check the following:
   *
   * If it fails between step-1 and step-2:
   * Check whether there are any segments in the PROPERTYSTORE with status DONE, but no new segment in status IN_PROGRESS,
   * and hence the status of the segment in the IDEALSTATE is still CONSUMING
   *
   * If it fails between step-2 and-3:
   * Check whether there are any segments in PROPERTYSTORE with status IN_PROGRESS, that are not accounted for in idealState.
   * If so, it should create the new segments in idealState.
   *
   * If the controller fails after step-3, we are fine because the idealState has the new segments.
   * If the controller fails before step-1, the server will see this as an upload failure, and will re-try.
   * @param tableConfig
   *
   * TODO: We need to find a place to detect and update a gauge for nonConsumingPartitionsCount for a table, and reset it to 0 at the end of validateLLC
   */
  public void validateLLCSegments(final TableConfig tableConfig) {
    final String tableNameWithType = tableConfig.getTableName();
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    final int partitionCount = getKafkaPartitionCount(streamMetadata);
    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        return validateLLCSegments(tableConfig, idealState, partitionCount);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f), true);
  }

  /**
   * Updates ideal state after completion of a realtime segment
   * @param tableNameWithType
   * @param currentSegmentId
   * @param newSegmentId
   * @param partitionAssignment
   */
  @VisibleForTesting
  protected void updateIdealStateOnSegmentCompletion(@Nonnull final String tableNameWithType,
      @Nonnull final String currentSegmentId, @Nonnull final String newSegmentId,
      @Nonnull final PartitionAssignment partitionAssignment) {

    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        return updateIdealStateOnSegmentCompletion(idealState, currentSegmentId, newSegmentId, partitionAssignment);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
  }

  /**
   * Sets up a new table's segments metadata and returns the ideal state setup with initial segments
   * @param tableConfig
   * @param idealState
   * @param partitionCount
   * @return
   */
  private IdealState setupTable(TableConfig tableConfig, IdealState idealState, int partitionCount)
      throws InvalidConfigException {
    final String tableNameWithType = tableConfig.getTableName();
    if (!idealState.isEnabled()) {
      LOGGER.info("Skipping validation for disabled table {}", tableNameWithType);
      return idealState;
    }
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    final long now = getCurrentTimeMs();

    PartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, partitionCount);

    Set<Integer> newPartitions = new HashSet<>(partitionCount);
    for (int partition = 0; partition < partitionCount; partition++) {
      newPartitions.add(partition);
    }

    Set<String> consumingSegments =
        setupNewPartitions(tableConfig, streamMetadata, partitionAssignment, newPartitions, now);

    RealtimeSegmentAssignmentStrategy segmentAssignmentStrategy = new ConsumingSegmentAssignmentStrategy();
    Map<String, List<String>> assignments = segmentAssignmentStrategy.assign(consumingSegments, partitionAssignment);

    updateIdealState(idealState, null, consumingSegments, assignments);
    return idealState;
  }

  /*
   *  A segment commit takes 3 modifications to zookeeper:
   *  - Change old segment metadata (mark it DONE, and then other things)
   *  - Add new metadata
   *  - Update idealstate to change oldsegment to ONLINE and new segment to CONSUMING
   *
   * A controller failure before/during the first step, or after the last step, does not bother us.
   * However, a controller failure after the first step (but before the successful completion of last step)
   * will result in leaving zookeeper in an inconsistent state. We have logic to trigger a periodic scan
   * of the segments, and repair those in this intermediate stage of segment completion.
   *
   * Now that there are two threads that may try to create new segments, we need to be careful.
   *
   * It may happen that the segment completion thread has just done step-1, but meanwhile the periodic
   * validator triggers, and mistakes this segment to be in incomplete state.
   *
   * We check the segment's metadata to see if that is old enough for repair. If it is fairly new, we
   * leave it as it is, to be fixed the next time repair job triggers.
   */
  @VisibleForTesting
  protected boolean isTooSoonToCorrect(String tableNameWithType, String segmentId, long now) {
    Stat stat = new Stat();
    LLCRealtimeSegmentZKMetadata metadata = getRealtimeSegmentZKMetadata(tableNameWithType, segmentId, stat);
    long metadataUpdateTime = stat.getMtime();
    if (now < metadataUpdateTime + TimeUnit.MILLISECONDS.convert(MAX_SEGMENT_COMPLETION_TIME_MINS, TimeUnit.MINUTES)) {
      // too soon to correct
      return true;
    }
    return false;
  }


  private boolean isAllInstancesOffline(Map<String, String> instanceStateMap) {
    for (String state : instanceStateMap.values()) {
      if (!state.equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE)) {
        return false;
      }
    }
    return true;
  }

  /*
   * Validate LLC segments of a table.
   *
   * Iterates over latest metadata for each partition and checks for following scenarios and repairs them:
   * 1) Segment present in ideal state
   * a) metadata status is IN_PROGRESS, segment state is CONSUMING - happy path
   * b) metadata status is IN_PROGRESS, segment state is OFFLINE - create new metadata and new CONSUMING segment
   * c) metadata status is DONE, segment state is OFFLINE - create new metadata and new CONSUMING segment
   * d) metadata status is DONE, segment state is CONSUMING - create new metadata and new CONSUMINg segment
   * 2) Segment is absent from ideal state - add new segment to ideal state
   *
   * Also checks if it is too soon to correct (could be in the process of committing segment)
   * If new partitions are detected, gets them started
   * If new instances are detected, uses them for new assignments
   *
   * So, the method may:
   * - Add or modify one or more segment metadata znodes
   * - Update the idealstate, which may fail if some other process updated the idealstate.
   *
   * In case idealstate update fails, then we need to start over.
   * TODO: split this method into multiple smaller methods
   */
  @VisibleForTesting
  protected IdealState validateLLCSegments(final TableConfig tableConfig, IdealState idealState,
      final int partitionCount)  {
    final String tableNameWithType = tableConfig.getTableName();
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    if (!idealState.isEnabled()) {
      LOGGER.info("Skipping validation for disabled table {}", tableNameWithType);
      return idealState;
    }
    final long now = getCurrentTimeMs();

    // Get the metadata for the latest 2 segments of each partition
    Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> latestMetadata =
        getLatestMetadata(tableNameWithType);

    // Find partitions for which there is no metadata at all. These are new partitions that we need to start consuming.
    Set<Integer> newPartitions = new HashSet<>(partitionCount);
    for (int partition = 0; partition < partitionCount; partition++) {
      if (!latestMetadata.containsKey(partition)) {
        LOGGER.info("Found partition {} with no segments", partition);
        newPartitions.add(partition);
      }
    }

    PartitionAssignment partitionAssignment;
    boolean skipNewPartitions = false;
    try {
      partitionAssignment = _partitionAssignmentGenerator.generatePartitionAssignment(tableConfig, partitionCount);
    } catch (InvalidConfigException e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.PARTITION_ASSIGNMENT_GENERATION_ERROR,
          1L);
      LOGGER.warn(
          "Could not generate partition assignment. Fetching partition assignment from ideal state for repair of table {}",
          tableNameWithType);
      partitionAssignment = _partitionAssignmentGenerator.getPartitionAssignmentFromIdealState(tableConfig, idealState);
      skipNewPartitions = true;
    }

    Set<String> onlineSegments = new HashSet<>(); // collect all segment names which should be updated to ONLINE state
    Set<String> consumingSegments = new HashSet<>(); // collect all segment names which should be created in CONSUMING state

    // Walk over all partitions that we have metadata for, and repair any partitions necessary.
    // Possible things to repair:
    // 1. The latest metadata is in DONE state, but the idealstate says segment is CONSUMING:
    //    a. Create metadata for next segment and find hosts to assign it to.
    //    b. update current segment in idealstate to ONLINE
    //    c. add new segment in idealstate to CONSUMING on the hosts.
    // 2. The latest metadata is IN_PROGRESS, but segment is not there in idealstate.
    //    a. change prev segment to ONLINE in idealstate
    //    b. add latest segment to CONSUMING in idealstate.
    // 3. All instances of a segment are in OFFLINE state.
    //    a. Create a new segment (with the next seq number)
    //       and restart consumption from the same offset (if possible) or a newer offset (if realtime stream does not have the same offset).
    //       In latter case, report data loss.
    for (Map.Entry<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> entry : latestMetadata.entrySet()) {
      int partition = entry.getKey();
      LLCRealtimeSegmentZKMetadata metadata = entry.getValue().pollFirst();
      final String segmentId = metadata.getSegmentName();
      final LLCSegmentName segmentName = new LLCSegmentName(segmentId);

      Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
      if (mapFields.containsKey(segmentId)) {
        // Latest segment of metadata is in idealstate.
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        if (instanceStateMap.values().contains(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
          if (metadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {

            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done either.
            if (isTooSoonToCorrect(tableNameWithType, segmentId, now)) {
              LOGGER.info("Skipping correction of segment {} (too soon to correct)", segmentId);
              continue;
            }
            LOGGER.info("{}:Repairing segment for partition {}. "
                    + "Old segment metadata {} has status DONE, but segments are still in CONSUMING state in ideal STATE",
                tableNameWithType, partition, segmentId);

            final int newSeqNum = segmentName.getSequenceNumber() + 1;
            LLCSegmentName newLLCSegmentName =
                new LLCSegmentName(segmentName.getTableName(), partition, newSeqNum, now);

            LOGGER.info("{}: Creating new segment metadata for {}", tableNameWithType,
                newLLCSegmentName.getSegmentName());

            createNewSegmentMetadataZNRecord(tableConfig, segmentName, newLLCSegmentName, metadata.getEndOffset(),
                partitionAssignment, 0);

            onlineSegments.add(segmentId);
            consumingSegments.add(newLLCSegmentName.getSegmentName());
          }
          // else, the metadata should be IN_PROGRESS, which is the right state for a consuming segment.
        } else if (isAllInstancesOffline(instanceStateMap)) {
          // An in-progress segment marked itself offline for some reason the server could not consume.
          // No instances are consuming, so create a new consuming segment.
          int nextSeqNum = segmentName.getSequenceNumber() + 1;
          LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", tableNameWithType, partition,
              nextSeqNum);
          // To begin with, set startOffset to the oldest available offset in kafka. Fix it to be the one we want,
          // depending on what the prev segment had.
          long startOffset = getKafkaPartitionOffset(streamMetadata, "smallest", partition);
          LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, tableNameWithType, partition);
          startOffset =
              getBetterStartOffsetIfNeeded(tableNameWithType, partition, segmentName, startOffset, nextSeqNum);
          LLCSegmentName newLLCSegmentName =
              new LLCSegmentName(segmentName.getTableName(), partition, nextSeqNum, now);

          createNewSegmentMetadataZNRecord(tableConfig, segmentName, newLLCSegmentName, startOffset,
              partitionAssignment, 0);

          consumingSegments.add(newLLCSegmentName.getSegmentName());
        }
        // else It can be in ONLINE state, in which case there is no need to repair the segment.
      } else {
        // idealstate does not have an entry for the segment (but metadata is present)
        // controller has failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE)
        // and creating new segment metadata (new segment metadata state = IN_PROGRESS),
        // but before updating ideal state (new segment ideal missing from ideal state)
        if (isTooSoonToCorrect(tableNameWithType, segmentId, now)) {
          LOGGER.info("Skipping correction of segment {} (too soon to correct)", segmentId);
          continue;
        }

        Preconditions.checkArgument(metadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.IN_PROGRESS));
        LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", tableNameWithType,
            partition, segmentId);

        // If there was a prev segment in the same partition, then we need to fix it to be ONLINE.
        LLCRealtimeSegmentZKMetadata prevMetadata = entry.getValue().pollLast();

        if (prevMetadata == null && skipNewPartitions) {
          continue;
        }
        if (prevMetadata != null) {
          onlineSegments.add(prevMetadata.getSegmentName());
        }
        consumingSegments.add(segmentId);
      }
    }

    if (!skipNewPartitions) {
      Set<String> newPartitionSegments =
          setupNewPartitions(tableConfig, streamMetadata, partitionAssignment, newPartitions, now);
      consumingSegments.addAll(newPartitionSegments);
    }

    RealtimeSegmentAssignmentStrategy segmentAssignmentStrategy = new ConsumingSegmentAssignmentStrategy();
    Map<String, List<String>> assignments;
    try {
      assignments =
          segmentAssignmentStrategy.assign(consumingSegments, partitionAssignment);
    } catch (InvalidConfigException e) {
      throw new IllegalStateException(
          "Caught exception when assigning segments using partition assignment for table " + tableNameWithType);
    }

    updateIdealState(idealState, onlineSegments, consumingSegments, assignments);
    return idealState;
  }

  /**
   * Updates the ideal state object
   * Adds the segments in consumingSegments to CONSUMING state using instances from assignments
   * Sets the segments in onlineSegments to ONLINE state
   * @param idealState
   * @param consumingSegments
   * @param onlineSegments
   * @param assignments
   */
  private void updateIdealState(IdealState idealState, Set<String> onlineSegments, Set<String> consumingSegments,
      Map<String, List<String>> assignments) {
    if (onlineSegments != null) {
      for (String segment : onlineSegments) {
        Set<String> oldInstances = idealState.getInstanceSet(segment);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(oldInstances));
        for (String instance : oldInstances) {
          idealState.setPartitionState(segment, instance,
              PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
        }
      }
    }

    if (consumingSegments != null) {
      for (String segment : consumingSegments) {
        List<String> newInstances = assignments.get(segment);
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segment);
        if (instanceStateMap != null) {
          instanceStateMap.clear();
        }
        for (String instance : newInstances) {
          idealState.setPartitionState(segment, instance,
              PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
        }
      }
    }
  }

  /**
   * Create metadata for new partitions
   * @param tableConfig
   * @param streamMetadata
   * @param partitionAssignment
   * @param newPartitions
   * @param now
   * @return set of newly created segment names
   */
  private Set<String> setupNewPartitions(TableConfig tableConfig, StreamMetadata streamMetadata,
      PartitionAssignment partitionAssignment, Set<Integer> newPartitions, long now) {

    String tableName = tableConfig.getTableName();
    Set<String> newSegmentNames = new HashSet<>(newPartitions.size());
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int nextSeqNum = STARTING_SEQUENCE_NUMBER;
    String consumerStartOffsetSpec = streamMetadata.getKafkaConsumerProperties()
        .get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
    for (int partition : newPartitions) {
      LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", tableName, partition,
          nextSeqNum);
      long startOffset = getKafkaPartitionOffset(streamMetadata, consumerStartOffsetSpec, partition);
      LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, tableName, partition);

      LLCSegmentName newLLCSegmentName = new LLCSegmentName(rawTableName, partition, nextSeqNum, now);
      createNewSegmentMetadataZNRecord(tableConfig, null, newLLCSegmentName, startOffset, partitionAssignment, 0);

      newSegmentNames.add(newLLCSegmentName.getSegmentName());
    }
    return newSegmentNames;
  }


  @VisibleForTesting
  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  protected IdealState updateIdealStateOnSegmentCompletion(@Nonnull IdealState idealState, @Nonnull String currentSegmentId,
      @Nonnull String newSegmentId, @Nonnull  PartitionAssignment partitionAssignment) {

    Map<String, List<String>> instanceAssignments = null;

    RealtimeSegmentAssignmentStrategy strategy = new ConsumingSegmentAssignmentStrategy();
    try {
      instanceAssignments = strategy.assign(Lists.newArrayList(newSegmentId), partitionAssignment);
    } catch (InvalidConfigException e) {
      _controllerMetrics.addMeteredTableValue(idealState.getResourceName(),
          ControllerMeter.CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_ERROR, 1L);
      throw new IllegalStateException("Caught exception when updating ideal state on segment completion", e);
    }

    List<String> newSegmentInstances = instanceAssignments.get(newSegmentId);
    Set<String> currentSegmentInstances = idealState.getInstanceSet(currentSegmentId);
    for (String instance : currentSegmentInstances) {
      idealState.setPartitionState(currentSegmentId, instance,
          PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    }

    // We may have (for whatever reason) a different instance list in the idealstate for the new segment.
    // If so, clear it, and then set the instance state for the set of instances that we know should be there.
    Map<String, String> stateMap = idealState.getInstanceStateMap(newSegmentId);
    if (stateMap != null) {
      stateMap.clear();
    }
    for (String instance : newSegmentInstances) {
      idealState.setPartitionState(newSegmentId, instance,
          PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
    }

    return idealState;
  }

  private static class KafkaOffsetFetcher implements Callable<Boolean> {
    private final String _topicName;
    private final String _offsetCriteria;
    private final int _partitionId;

    private Exception _exception = null;
    private long _offset = -1;
    private PinotStreamConsumerFactory _pinotStreamConsumerFactory;
    StreamMetadata _streamMetadata;


    private KafkaOffsetFetcher(final String offsetCriteria, int partitionId, StreamMetadata streamMetadata) {
      _offsetCriteria = offsetCriteria;
      _partitionId = partitionId;
      _pinotStreamConsumerFactory = PinotStreamConsumerFactory.create(streamMetadata);
      _streamMetadata = streamMetadata;
      _topicName = streamMetadata.getKafkaTopicName();
    }

    private long getOffset() {
      return _offset;
    }

    private Exception getException() {
      return _exception;
    }

    @Override
    public Boolean call() throws Exception {

      PinotStreamConsumer
          kafkaConsumer = _pinotStreamConsumerFactory.buildConsumer("dummyClientId", _partitionId, _streamMetadata);
      try {
        _offset = kafkaConsumer.fetchPartitionOffset(_offsetCriteria, KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS);
        if (_exception != null) {
          LOGGER.info("Successfully retrieved offset({}) for kafka topic {} partition {}", _offset, _topicName, _partitionId);
        }
        return Boolean.TRUE;
      } catch (SimpleConsumerWrapper.TransientConsumerException e) {
        LOGGER.warn("Temporary exception when fetching offset for topic {} partition {}:{}", _topicName, _partitionId, e.getMessage());
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        _exception = e;
        throw e;
      } finally {
        IOUtils.closeQuietly(kafkaConsumer);
      }
    }
  }
}
