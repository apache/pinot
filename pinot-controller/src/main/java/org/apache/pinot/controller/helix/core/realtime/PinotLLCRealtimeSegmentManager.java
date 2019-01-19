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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.partition.PartitionAssignment;
import org.apache.pinot.common.partition.StreamPartitionAssignmentGenerator;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerLeadershipManager;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdateManager;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdater;
import org.apache.pinot.controller.util.SegmentCompletionUtils;
import org.apache.pinot.core.realtime.segment.ConsumingSegmentAssignmentStrategy;
import org.apache.pinot.core.realtime.segment.RealtimeSegmentAssignmentStrategy;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.PartitionOffsetFetcher;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.filesystem.PinotFS;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  protected static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments
  protected static final long END_OFFSET_FOR_CONSUMING_SEGMENTS = Long.MAX_VALUE;

  private static final String METADATA_TEMP_DIR_SUFFIX = ".metadata.tmp";
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";

  // Max time to wait for all LLC segments to complete committing their metadata.
  private static final long MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS = 30_000L;

  // TODO: make this configurable with default set to 10
  /**
   * After step 1 of segment completion is done,
   * this is the max time until which step 3 is allowed to complete.
   * See {@link PinotLLCRealtimeSegmentManager#commitSegmentMetadata(String, CommittingSegmentDescriptor)} for explanation of steps 1 2 3
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
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;
  private final int _numIdealStateUpdateLocks;
  private final Lock[] _idealstateUpdateLocks;
  private final TableConfigCache _tableConfigCache;
  private final StreamPartitionAssignmentGenerator _streamPartitionAssignmentGenerator;
  private final FlushThresholdUpdateManager _flushThresholdUpdateManager;

  private volatile boolean _isStopping = false;
  private AtomicInteger _numCompletingSegments = new AtomicInteger(0);

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
      ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created");
    }
    INSTANCE =
        new PinotLLCRealtimeSegmentManager(helixAdmin, clusterName, helixManager, propertyStore, helixResourceManager,
            controllerConf, controllerMetrics);
    SegmentCompletionManager.create(helixManager, INSTANCE, controllerConf, controllerMetrics);
  }

  public void stop() {
    _isStopping = true;
    LOGGER.info("Awaiting segment metadata commits: maxWaitTimeMillis = {}", MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS);
    long millisToWait = MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS;

    // Busy-wait for all segments that are committing metadata to complete their operation.
    // Waiting
    while (_numCompletingSegments.get() > 0 && millisToWait > 0) {
      try {
        long thisWait = 1000;
        if (millisToWait < thisWait) {
          thisWait = millisToWait;
        }
        Thread.sleep(thisWait);
        millisToWait -= thisWait;
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted: Remaining wait time {} (out of {})", millisToWait, MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS);
        return;
      }
    }
    LOGGER.info("Wait completed: Number of completing segments = {}", _numCompletingSegments.get());
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
    _numIdealStateUpdateLocks = controllerConf.getRealtimeSegmentMetadataCommitNumLocks();
    _idealstateUpdateLocks = new Lock[_numIdealStateUpdateLocks];
    for (int i = 0; i < _numIdealStateUpdateLocks; i++) {
      _idealstateUpdateLocks[i] = new ReentrantLock();
    }
    _tableConfigCache = new TableConfigCache(_propertyStore);
    _streamPartitionAssignmentGenerator = new StreamPartitionAssignmentGenerator(_helixManager);
    _flushThresholdUpdateManager = new FlushThresholdUpdateManager();
  }

  public static PinotLLCRealtimeSegmentManager getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Not yet created");
    }
    return INSTANCE;
  }


  protected boolean isLeader() {
    return ControllerLeadershipManager.getInstance().isLeader();
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
    final StreamConfig streamConfig = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    int partitionCount = getPartitionCount(streamConfig);
    List<String> currentSegments = getExistingSegments(tableConfig.getTableName());
    // Make sure that there are no low-level segments existing.
    if (currentSegments != null) {
      for (String segment : currentSegments) {
        if (!SegmentName.isHighLevelConsumerSegmentName(segment)) {
          // For now, we don't support re-creating the low-level realtime segments
          throw new RuntimeException("Low-level segments already exist for table " + tableConfig.getTableType());
        }
      }
    }
    _flushThresholdUpdateManager.clearFlushThresholdUpdater(tableConfig);
    if (!isConnected()) {
      throw new RuntimeException(
          "Lost zk connection while setting up new table " + tableConfig.getTableName() + " isConnected="
              + isConnected());
    }
    IdealState idealState = setupTable(tableConfig, emptyIdealState, partitionCount);
    setTableIdealState(tableConfig.getTableName(), idealState);
  }

  // Remove all trace of LLC for this table.
  public void cleanupLLC(final String realtimeTableName) {
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

  private SegmentPartitionMetadata getPartitionMetadataFromTableConfig(String tableName, int numPartitions,
      int partitionId) {
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
        partitionMetadataMap.put(column,
            new ColumnPartitionMetadata(columnPartitionConfig.getFunctionName(), numPartitions,
                Collections.singleton(partitionId)));
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
      if (columnMetadata.getPartitionFunction() != null) {
        partitionMetadataMap.put(columnName,
            new ColumnPartitionMetadata(columnMetadata.getPartitionFunction().toString(),
                columnMetadata.getNumPartitions(), columnMetadata.getPartitions()));
      }
    }
    return new SegmentPartitionMetadata(partitionMetadataMap);
  }

  protected List<String> getExistingSegments(String realtimeTableName) {
    String propStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    return _propertyStore.getChildNames(propStorePath, AccessOption.PERSISTENT);
  }

  protected boolean writeSegmentToPropertyStore(String znodePath, ZNRecord znRecord, final String realtimeTableName,
      int expectedVersion) {
    boolean success = _propertyStore.set(znodePath, znRecord, expectedVersion, AccessOption.PERSISTENT);
    if (!success) {
      LOGGER.error(
          "Failed to write segment to property store at {} for table {}. Expected zookeeper version number: {}",
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

  protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records,
      final String realtimeTableName) {
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

  public boolean commitSegmentFile(String tableName, CommittingSegmentDescriptor committingSegmentDescriptor) {
    if (_isStopping) {
      LOGGER.info("Returning false since the controller is stopping");
      return false;
    }
    String segmentName = committingSegmentDescriptor.getSegmentName();
    String segmentLocation = committingSegmentDescriptor.getSegmentLocation();
    URI segmentFileURI = ControllerConf.getUriFromPath(segmentLocation);
    URI baseDirURI = ControllerConf.getUriFromPath(_controllerConf.getDataDir());
    URI tableDirURI = ControllerConf.getUriFromPath(StringUtil.join("/", _controllerConf.getDataDir(), tableName));
    URI uriToMoveTo = ControllerConf.getUriFromPath(StringUtil.join("/", tableDirURI.toString(), segmentName));
    PinotFS pinotFS = PinotFSFactory.create(baseDirURI.getScheme());

    if (!isConnected() || !isLeader()) {
      // We can potentially log a different value than what we saw ....
      LOGGER.warn("Lost leadership while committing segment file {}, {} for table {}: isLeader={}, isConnected={}",
          segmentName, segmentLocation, tableName, isLeader(), isConnected());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_NOT_LEADER, 1L);
      return false;
    }

    try {
      pinotFS.move(segmentFileURI, uriToMoveTo, true);
    } catch (Exception e) {
      LOGGER.error("Could not move {} to {}", segmentLocation, segmentName, e);
      return false;
    }

    try {
      for (String uri : pinotFS.listFiles(tableDirURI, true)) {
        if (uri.contains(SegmentCompletionUtils.getSegmentNamePrefix(segmentName))) {
          LOGGER.warn("Deleting " + uri);
          pinotFS.delete(new URI(uri), true);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Could not delete tmp segment files for {}", tableDirURI, e);
    }

    return true;
  }

  /**
   * This method is invoked after the realtime segment is uploaded but before a response is sent to the server.
   * It updates the propertystore segment metadata from IN_PROGRESS to DONE, and also creates new propertystore
   * records for new segments, and puts them in idealstate in CONSUMING state.
   *
   * @param rawTableName Raw table name
   * @param committingSegmentDescriptor
   * @return boolean
   */
  public boolean commitSegmentMetadata(String rawTableName, CommittingSegmentDescriptor committingSegmentDescriptor) {
    if (_isStopping) {
      LOGGER.info("Returning false since the controller is stopping");
      return false;
    }
    try {
      _numCompletingSegments.addAndGet(1);
      return commitSegmentMetadataInternal(rawTableName, committingSegmentDescriptor);
    } finally {
      _numCompletingSegments.addAndGet(-1);
    }
  }

  private boolean commitSegmentMetadataInternal(String rawTableName, CommittingSegmentDescriptor committingSegmentDescriptor) {

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

    final String committingSegmentNameStr = committingSegmentDescriptor.getSegmentName();
    final long nextOffset = committingSegmentDescriptor.getNextOffset();
    final LLCSegmentName committingLLCSegmentName = new LLCSegmentName(committingSegmentNameStr);
    final int partitionId = committingLLCSegmentName.getPartitionId();
    final int newSeqNum = committingLLCSegmentName.getSequenceNumber() + 1;
    final long now = System.currentTimeMillis();

    LLCSegmentName newLLCSegmentName =
        new LLCSegmentName(committingLLCSegmentName.getTableName(), partitionId, newSeqNum, now);
    String newSegmentNameStr = newLLCSegmentName.getSegmentName();

    IdealState idealState = getTableIdealState(realtimeTableName);
    Preconditions.checkState(idealState.getInstanceStateMap(committingSegmentNameStr)
        .containsValue(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE));
    int numPartitions = _streamPartitionAssignmentGenerator.getNumPartitionsFromIdealState(idealState);

    PartitionAssignment partitionAssignment;
    try {
      partitionAssignment =
          _streamPartitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, numPartitions);
    } catch (InvalidConfigException e) {
      LOGGER.error("Exception when generating partition assignment for table {} and numPartitions {}",
          realtimeTableName, numPartitions, e);
      return false;
    }

    // If an LLC table is dropped (or cleaned up), we will get null here. In that case we should not be creating a new segment
    if (partitionAssignment == null) {
      LOGGER.warn("Partition assignment not found for {}", realtimeTableName);
      throw new RuntimeException("Partition assignment not found. Not committing segment");
    }

    // Step-1
    boolean success = updateOldSegmentMetadataZNRecord(realtimeTableName, committingLLCSegmentName, nextOffset);
    if (!success) {
      return false;
    }

    // Step-2
    success =
        createNewSegmentMetadataZNRecord(tableConfig, committingLLCSegmentName, newLLCSegmentName, partitionAssignment,
            committingSegmentDescriptor, false);
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
    int lockIndex = (realtimeTableName.hashCode() & Integer.MAX_VALUE) % _numIdealStateUpdateLocks;
    Lock lock = _idealstateUpdateLocks[lockIndex];
    try {
      lock.lock();
      updateIdealStateOnSegmentCompletion(realtimeTableName, committingSegmentNameStr, newSegmentNameStr,
          partitionAssignment);
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
    final LLCRealtimeSegmentZKMetadata committingSegmentMetadata =
        getRealtimeSegmentZKMetadata(realtimeTableName, committingSegmentNameStr, stat);

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
    final String oldZnodePath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, committingSegmentNameStr);

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
   * @param realtimeTableConfig  table config of the segment for which new metadata is being created
   * @param committingLLCSegmentName
   * @param newLLCSegmentName  new segment name
   * @param partitionAssignment  stream partition assignment for this table
   * @param committingSegmentDescriptor
   * @param isNewTableSetup
   * @return
   */
  protected boolean createNewSegmentMetadataZNRecord(TableConfig realtimeTableConfig,
      LLCSegmentName committingLLCSegmentName, LLCSegmentName newLLCSegmentName,
      PartitionAssignment partitionAssignment, CommittingSegmentDescriptor committingSegmentDescriptor,
      boolean isNewTableSetup) {

    String realtimeTableName = realtimeTableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);

    LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata = null;
    if (committingLLCSegmentName != null) {
      committingSegmentZKMetadata =
          getRealtimeSegmentZKMetadata(realtimeTableName, committingLLCSegmentName.getSegmentName(), null);
    }

    int partitionId = newLLCSegmentName.getPartitionId();
    int numReplicas = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId)).size();
    ZNRecord newZnRecord =
        makeZnRecordForNewSegment(rawTableName, numReplicas, committingSegmentDescriptor.getNextOffset(),
            newLLCSegmentName, partitionAssignment.getNumPartitions());
    final LLCRealtimeSegmentZKMetadata newSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata(newZnRecord);

    FlushThresholdUpdater flushThresholdUpdater =
        _flushThresholdUpdateManager.getFlushThresholdUpdater(realtimeTableConfig);
    flushThresholdUpdater.updateFlushThreshold(newSegmentZKMetadata, committingSegmentZKMetadata,
        committingSegmentDescriptor, partitionAssignment);

    newZnRecord = newSegmentZKMetadata.toZNRecord();

    final String newSegmentNameStr = newLLCSegmentName.getSegmentName();
    final String newZnodePath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, newSegmentNameStr);

    if (!isNewTableSetup) {
      if (!isLeader() || !isConnected()) {
        // We can potentially log a different value than what we saw ....
        LOGGER.warn(
            "Lost leadership while committing new segment metadata for {} for table {}: isLeader={}, isConnected={}",
            newSegmentNameStr, rawTableName, isLeader(), isConnected());
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_NOT_LEADER, 1L);
        return false;
      }
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
    if (streamConfigs != null && streamConfigs.containsKey(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS)) {
      final String commitTimeoutSecondsStr = streamConfigs.get(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS);
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

  public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName,
      Stat stat) {
    ZNRecord znRecord =
        _propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName),
            stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      LOGGER.error("Segment metadata not found for table {}, segment {}. (can happen during table drop)",
          realtimeTableName, segmentName);
      throw new RuntimeException(
          "Segment metadata not found for table " + realtimeTableName + " segment " + segmentName);
    }
    return new LLCRealtimeSegmentZKMetadata(znRecord);
  }

  protected long getPartitionOffset(StreamConfig streamConfig, final OffsetCriteria offsetCriteria,
      int partitionId) {
    return fetchPartitionOffset(streamConfig, offsetCriteria, partitionId);
  }

  private long fetchPartitionOffset(StreamConfig streamConfig, final OffsetCriteria offsetCriteria, int partitionId) {
    PartitionOffsetFetcher partitionOffsetFetcher =
        new PartitionOffsetFetcher(offsetCriteria, partitionId, streamConfig);
    try {
      RetryPolicies.fixedDelayRetryPolicy(3, 1000L).attempt(partitionOffsetFetcher);
      return partitionOffsetFetcher.getOffset();
    } catch (Exception e) {
      Exception fetcherException = partitionOffsetFetcher.getException();
      LOGGER.error("Could not get offset for topic {} partition {}, criteria {}", streamConfig.getTopicName(),
          partitionId, offsetCriteria, fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }

  private long getBetterStartOffsetIfNeeded(final String realtimeTableName, final int partition,
      final LLCSegmentName latestSegment, final long oldestOffsetInStream, final int nextSeqNum) {
    final LLCRealtimeSegmentZKMetadata oldSegMetadata =
        getRealtimeSegmentZKMetadata(realtimeTableName, latestSegment.getSegmentName(), null);
    CommonConstants.Segment.Realtime.Status status = oldSegMetadata.getStatus();
    long segmentStartOffset = oldestOffsetInStream;
    // Offset at which the prev segment intended to start consuming
    final long prevSegStartOffset = oldSegMetadata.getStartOffset();
    if (status.equals(CommonConstants.Segment.Realtime.Status.IN_PROGRESS)) {
      if (oldestOffsetInStream <= prevSegStartOffset) {
        // We still have the same start offset available, re-use it.
        segmentStartOffset = prevSegStartOffset;
        LOGGER.info("Choosing previous segment start offset {} for table {} for partition {}, sequence {}",
            oldestOffsetInStream, realtimeTableName, partition, nextSeqNum);
      } else {
        // There is data loss.
        LOGGER.warn("Data lost from offset {} to {} for table {} partition {} sequence {}", prevSegStartOffset,
            oldestOffsetInStream, realtimeTableName, partition, nextSeqNum);
        // Start from the earliest offset in the stream
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_KAFKA_DATA_LOSS, 1);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_STREAM_DATA_LOSS, 1);
      }
    } else {
      // Status must be DONE, so we have a valid end-offset for the previous segment
      final long prevSegEndOffset = oldSegMetadata.getEndOffset();  // Will be 0 if the prev segment was not completed.
      if (oldestOffsetInStream < prevSegEndOffset) {
        // We don't want to create a segment that overlaps in data with the prev segment. We know that the previous
        // segment's end offset is available, so use that.
        segmentStartOffset = prevSegEndOffset;
        LOGGER.info("Choosing newer offset {} for table {} for partition {}, sequence {}", oldestOffsetInStream,
            realtimeTableName, partition, nextSeqNum);
      } else if (oldestOffsetInStream > prevSegEndOffset) {
        // Stream's oldest offset is greater than the end offset of the prev segment, so there is data loss.
        LOGGER.warn("Data lost from offset {} to {} for table {} partition {} sequence {}", prevSegEndOffset,
            oldestOffsetInStream, realtimeTableName, partition, nextSeqNum);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_KAFKA_DATA_LOSS, 1);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_STREAM_DATA_LOSS, 1);
      } else {
        // The two happen to be equal. A rarity, so log it.
        LOGGER.info("Earliest offset {} is the same as new segment start offset", oldestOffsetInStream);
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
    SegmentPartitionMetadata partitionMetadata =
        getPartitionMetadataFromTableConfig(realtimeTableName, numPartitions, newSegmentName.getPartitionId());
    if (partitionMetadata != null) {
      newSegMetadata.setPartitionMetadata(partitionMetadata);
    }

    return newSegMetadata.toZNRecord();
  }

  /**
   * An instance is reporting that it has stopped consuming a topic due to some error.
   * Mark the state of the segment to be OFFLINE in idealstate.
   * When all replicas of this segment are marked offline, the {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager},
   * in its next run, will auto-create a new segment with the appropriate offset.
   */
  public void segmentStoppedConsuming(final LLCSegmentName segmentName, final String instance) {
    String rawTableName = segmentName.getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    final String segmentNameStr = segmentName.getSegmentName();
    try {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
        idealState.setPartitionState(segmentNameStr, instance,
            CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.OFFLINE);
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentNameStr);
        LOGGER.info("Attempting to mark {} offline. Current map:{}", segmentNameStr, instanceStateMap.toString());
        return idealState;
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

  protected int getPartitionCount(StreamConfig streamConfig) {
    return PinotTableIdealStateBuilder.getPartitionCount(streamConfig);
  }

  /**
   * Returns the LLC realtime segments for the given table.
   *
   * @param realtimeTableName Realtime table name
   * @return List of LLC realtime segment names
   */
  @VisibleForTesting
  protected List<String> getAllSegments(String realtimeTableName) {
    return ZKMetadataProvider.getLLCRealtimeSegments(_propertyStore, realtimeTableName);
  }

  /**
   * Returns the LLC realtime segment ZK metadata for the given table and segment.
   *
   * @param realtimeTableName Realtime table name
   * @param segmentName Segment name (String)
   * @return LLC realtime segment ZK metadata
   */
  @VisibleForTesting
  protected LLCRealtimeSegmentZKMetadata getSegmentMetadata(String realtimeTableName, String segmentName) {
    return (LLCRealtimeSegmentZKMetadata) ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore,
        realtimeTableName, segmentName);
  }

  /**
   * Returns the latest 2 LLC realtime segment ZK metadata for each partition.
   *
   * @param realtimeTableName Realtime table name
   * @return Map from partition to array of latest LLC realtime segment ZK metadata
   */
  @VisibleForTesting
  protected Map<Integer, LLCRealtimeSegmentZKMetadata[]> getLatestMetadata(String realtimeTableName) {
    List<String> llcRealtimeSegments = getAllSegments(realtimeTableName);

    Map<Integer, LLCSegmentName[]> partitionToLatestSegmentsMap = new HashMap<>();
    for (String llcRealtimeSegment : llcRealtimeSegments) {
      LLCSegmentName segmentName = new LLCSegmentName(llcRealtimeSegment);
      partitionToLatestSegmentsMap.compute(segmentName.getPartitionId(), (partitionId, segmentNames) -> {
        if (segmentNames == null) {
          return new LLCSegmentName[]{segmentName, null};
        } else {
          if (segmentName.getSequenceNumber() > segmentNames[0].getSequenceNumber()) {
            segmentNames[1] = segmentNames[0];
            segmentNames[0] = segmentName;
          } else if (segmentNames[1] == null || segmentName.getSequenceNumber() > segmentNames[1].getSequenceNumber()) {
            segmentNames[1] = segmentName;
          }
          return segmentNames;
        }
      });
    }

    Map<Integer, LLCRealtimeSegmentZKMetadata[]> partitionToLatestMetadataMap = new HashMap<>();
    for (Map.Entry<Integer, LLCSegmentName[]> entry : partitionToLatestSegmentsMap.entrySet()) {
      LLCSegmentName[] latestSegments = entry.getValue();
      LLCRealtimeSegmentZKMetadata latestMetadata =
          getSegmentMetadata(realtimeTableName, latestSegments[0].getSegmentName());
      LLCRealtimeSegmentZKMetadata secondLatestMetadata = null;
      if (latestSegments[1] != null) {
        secondLatestMetadata = getSegmentMetadata(realtimeTableName, latestSegments[1].getSegmentName());
      }
      partitionToLatestMetadataMap.put(entry.getKey(),
          new LLCRealtimeSegmentZKMetadata[]{latestMetadata, secondLatestMetadata});
    }

    return partitionToLatestMetadataMap;
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
  public void ensureAllPartitionsConsuming(final TableConfig tableConfig) {
    final String tableNameWithType = tableConfig.getTableName();
    final StreamConfig streamConfig = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    final int partitionCount = getPartitionCount(streamConfig);
    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        return ensureAllPartitionsConsuming(tableConfig, idealState, partitionCount);
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
    final StreamConfig streamConfig = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    final long now = getCurrentTimeMs();

    PartitionAssignment partitionAssignment =
        _streamPartitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, partitionCount);

    Set<Integer> newPartitions = new HashSet<>(partitionCount);
    for (int partition = 0; partition < partitionCount; partition++) {
      newPartitions.add(partition);
    }

    OffsetCriteria offsetCriteria = streamConfig.getOffsetCriteria();
    Set<String> consumingSegments =
        setupNewPartitions(tableConfig, streamConfig, offsetCriteria, partitionAssignment, newPartitions, now);

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
      LOGGER.info("Too soon to correct segment:{} updateTime: {} now:{}", segmentId, metadataUpdateTime, now);
      return true;
    }
    return false;
  }

  private boolean isAllInstancesInState(Map<String, String> instanceStateMap, String state) {
    return instanceStateMap.values().stream().allMatch(value -> value.equals(state));
  }

  /*
   * Validate LLC segments of a table.
   *
   * Iterates over latest metadata for each partition and checks for following scenarios and repairs them:
   * 1) Segment present in ideal state
   * a) metadata status is IN_PROGRESS, segment state is CONSUMING - happy path
   * b) metadata status is IN_PROGRESS, segment state is OFFLINE - create new metadata and new CONSUMING segment
   * c) metadata status is DONE, segment state is OFFLINE - create new metadata and new CONSUMING segment
   * d) metadata status is DONE, segment state is CONSUMING - create new metadata and new CONSUMING segment
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
  protected IdealState ensureAllPartitionsConsuming(final TableConfig tableConfig, IdealState idealState,
      final int partitionCount) {
    final String tableNameWithType = tableConfig.getTableName();
    final StreamConfig streamConfig = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    if (!idealState.isEnabled()) {
      LOGGER.info("Skipping validation for disabled table {}", tableNameWithType);
      return idealState;
    }
    final long now = getCurrentTimeMs();

    // Get the metadata for the latest 2 segments of each partition
    Map<Integer, LLCRealtimeSegmentZKMetadata[]> partitionToLatestMetadata = getLatestMetadata(tableNameWithType);

    // Find partitions for which there is no metadata at all. These are new partitions that we need to start consuming.
    Set<Integer> newPartitions = new HashSet<>(partitionCount);
    for (int partition = 0; partition < partitionCount; partition++) {
      if (!partitionToLatestMetadata.containsKey(partition)) {
        LOGGER.info("Found partition {} with no segments", partition);
        newPartitions.add(partition);
      }
    }

    PartitionAssignment partitionAssignment;
    boolean skipNewPartitions = false;
    try {
      partitionAssignment =
          _streamPartitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, partitionCount);
    } catch (InvalidConfigException e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.PARTITION_ASSIGNMENT_GENERATION_ERROR,
          1L);
      LOGGER.warn(
          "Could not generate partition assignment. Fetching partition assignment from ideal state for repair of table {}",
          tableNameWithType);
      partitionAssignment =
          _streamPartitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);
      skipNewPartitions = true;
    }

    Set<String> onlineSegments = new HashSet<>(); // collect all segment names which should be updated to ONLINE state
    Set<String> consumingSegments =
        new HashSet<>(); // collect all segment names which should be created in CONSUMING state

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
    for (Map.Entry<Integer, LLCRealtimeSegmentZKMetadata[]> entry : partitionToLatestMetadata.entrySet()) {
      int partition = entry.getKey();
      LLCRealtimeSegmentZKMetadata[] latestMetadataArray = entry.getValue();
      LLCRealtimeSegmentZKMetadata latestMetadata = latestMetadataArray[0];
      final String segmentId = latestMetadata.getSegmentName();
      final LLCSegmentName segmentName = new LLCSegmentName(segmentId);

      Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
      if (mapFields.containsKey(segmentId)) {
        // Latest segment of metadata is in idealstate.
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        if (instanceStateMap.values().contains(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
          if (latestMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {

            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done either.
            if (isTooSoonToCorrect(tableNameWithType, segmentId, now)) {
              continue;
            }
            LOGGER.info("{}:Repairing segment for partition {}. "
                    + "Old segment metadata {} has status DONE, but segments are still in CONSUMING state in ideal STATE",
                tableNameWithType, partition, segmentId);

            LLCSegmentName newLLCSegmentName = makeNextLLCSegmentName(segmentName, partition, now);
            LOGGER.info("{}: Creating new segment metadata for {}", tableNameWithType,
                newLLCSegmentName.getSegmentName());

            CommittingSegmentDescriptor committingSegmentDescriptor =
                new CommittingSegmentDescriptor(segmentId, latestMetadata.getEndOffset(), 0);
            boolean success =
                createNewSegmentMetadataZNRecord(tableConfig, segmentName, newLLCSegmentName, partitionAssignment,
                    committingSegmentDescriptor, false);

            // creation of segment metadata could fail due to lost leadership or an unsuccessful write to property store
            // in such a case, we will exclude the segment from ideal state update and let the next iteration of validation manager fix it
            if (success) {
              onlineSegments.add(segmentId);
              consumingSegments.add(newLLCSegmentName.getSegmentName());
            }
          }
          // else, the metadata should be IN_PROGRESS, which is the right state for a consuming segment.
        } else { // no replica in CONSUMING state

          // Possible scenarios: for any of these scenarios, we need to create new metadata IN_PROGRESS and new CONSUMING segment
          // 1. all replicas OFFLINE and metadata IN_PROGRESS/DONE - a segment marked itself OFFLINE during consumption for some reason
          // 2. all replicas ONLINE and metadata DONE - Resolved in https://github.com/linkedin/pinot/pull/2890
          // 3. we should never end up with some replicas ONLINE and some OFFLINE.
          if (isAllInstancesInState(instanceStateMap, PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE)
              || !(isTooSoonToCorrect(tableNameWithType, segmentId, now))) {

            // No instances are consuming, so create a new consuming segment.
            LLCSegmentName newLLCSegmentName = makeNextLLCSegmentName(segmentName, partition, now);
            LOGGER.info("Creating CONSUMING segment {} for {} partition {}", newLLCSegmentName.getSegmentName(),
                tableNameWithType, partition);

            // To begin with, set startOffset to the oldest available offset in the stream. Fix it to be the one we want,
            // depending on what the prev segment had.
            long startOffset = getPartitionOffset(streamConfig, OffsetCriteria.SMALLEST_OFFSET_CRITERIA, partition);
            LOGGER.info("Found smallest offset {} for table {} for partition {}", startOffset, tableNameWithType,
                partition);
            startOffset = getBetterStartOffsetIfNeeded(tableNameWithType, partition, segmentName, startOffset,
                newLLCSegmentName.getSequenceNumber());

            CommittingSegmentDescriptor committingSegmentDescriptor =
                new CommittingSegmentDescriptor(segmentId, startOffset, 0);

            boolean success =
                createNewSegmentMetadataZNRecord(tableConfig, segmentName, newLLCSegmentName, partitionAssignment,
                    committingSegmentDescriptor, false);

            // creation of segment metadata could fail due to lost leadership or an unsuccessful write to property store
            // in such a case, we will exclude the segment from ideal state update and let the next iteration of validation manager fix it
            if (success) {
              consumingSegments.add(newLLCSegmentName.getSegmentName());
            }
          }
        }
      } else {
        // idealstate does not have an entry for the segment (but metadata is present)
        // controller has failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE)
        // and creating new segment metadata (new segment metadata state = IN_PROGRESS),
        // but before updating ideal state (new segment ideal missing from ideal state)
        if (isTooSoonToCorrect(tableNameWithType, segmentId, now)) {
          continue;
        }

        Preconditions.checkArgument(
            latestMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.IN_PROGRESS));
        LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", tableNameWithType,
            partition, segmentId);

        // If there was a prev segment in the same partition, then we need to fix it to be ONLINE.
        LLCRealtimeSegmentZKMetadata secondLatestMetadata = latestMetadataArray[1];
        if (secondLatestMetadata == null && skipNewPartitions) {
          continue;
        }
        if (secondLatestMetadata != null) {
          onlineSegments.add(secondLatestMetadata.getSegmentName());
        }
        consumingSegments.add(segmentId);
      }
    }

    if (!skipNewPartitions) {
      Set<String> newPartitionSegments =
          setupNewPartitions(tableConfig, streamConfig, OffsetCriteria.SMALLEST_OFFSET_CRITERIA, partitionAssignment,
              newPartitions, now);
      consumingSegments.addAll(newPartitionSegments);
    }

    RealtimeSegmentAssignmentStrategy segmentAssignmentStrategy = new ConsumingSegmentAssignmentStrategy();
    Map<String, List<String>> assignments;
    try {
      assignments = segmentAssignmentStrategy.assign(consumingSegments, partitionAssignment);
    } catch (InvalidConfigException e) {
      throw new IllegalStateException(
          "Caught exception when assigning segments using partition assignment for table " + tableNameWithType);
    }

    updateIdealState(idealState, onlineSegments, consumingSegments, assignments);
    return idealState;
  }

  private LLCSegmentName makeNextLLCSegmentName(LLCSegmentName segmentName, int partition, long now) {
    final int newSeqNum = segmentName.getSequenceNumber() + 1;
    LLCSegmentName newLLCSegmentName = new LLCSegmentName(segmentName.getTableName(), partition, newSeqNum, now);
    return newLLCSegmentName;
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
   * @param tableConfig  the table configuration to use for the new partition
   * @param streamConfig stream configuration associated with the table
   * @param offsetCriteria the offset to query to start consumption from. Can be different for a
   *                       new table being setup vs new partitions being added to an existing table
   * @param partitionAssignment the partition assignment strategy to use
   * @param newPartitions the new partitions to set up
   * @param now the current timestamp in milliseconds
   * @return set of newly created segment names
   */
  private Set<String> setupNewPartitions(TableConfig tableConfig, StreamConfig streamConfig,
      OffsetCriteria offsetCriteria, PartitionAssignment partitionAssignment, Set<Integer> newPartitions, long now) {

    String tableName = tableConfig.getTableName();
    Set<String> newSegmentNames = new HashSet<>(newPartitions.size());
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int nextSeqNum = STARTING_SEQUENCE_NUMBER;

    for (int partition : newPartitions) {
      LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", tableName, partition,
          nextSeqNum);
      long startOffset = getPartitionOffset(streamConfig, offsetCriteria, partition);

      LOGGER.info("Found offset {} for table {} for partition {}", startOffset, tableName, partition);

      LLCSegmentName newLLCSegmentName = new LLCSegmentName(rawTableName, partition, nextSeqNum, now);
      CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(null, startOffset, 0);

      boolean success = createNewSegmentMetadataZNRecord(tableConfig, null, newLLCSegmentName, partitionAssignment,
          committingSegmentDescriptor, true);
      // creation of segment metadata could fail due to an unsuccessful write to property store
      // in such a case, we will exclude the segment from ideal state update and let the validation manager fix it
      if (success) {
        newSegmentNames.add(newLLCSegmentName.getSegmentName());
      }
    }
    return newSegmentNames;
  }

  @VisibleForTesting
  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  protected IdealState updateIdealStateOnSegmentCompletion(@Nonnull IdealState idealState,
      @Nonnull String currentSegmentId, @Nonnull String newSegmentId,
      @Nonnull PartitionAssignment partitionAssignment) {

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
}
