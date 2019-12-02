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
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsType;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdateManager;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdater;
import org.apache.pinot.controller.util.SegmentCompletionUtils;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.apache.pinot.core.realtime.stream.PartitionOffsetFetcher;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment manager for LLC real-time table.
 * <p>Segment management APIs:
 * <ul>
 *   <li>setUpNewTable(): From any controller</li>
 *   <li>removeLLCSegments(): From any controller</li>
 *   <li>commitSegmentFile(): From lead controller only</li>
 *   <li>commitSegmentMetadata(): From lead controller only</li>
 *   <li>segmentStoppedConsuming(): From lead controller only</li>
 *   <li>ensureAllPartitionsConsuming(): From lead controller only</li>
 * </ul>
 */
public class PinotLLCRealtimeSegmentManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);

  private static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments
  private static final long END_OFFSET_FOR_CONSUMING_SEGMENTS = Long.MAX_VALUE;
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";

  // Max time to wait for all LLC segments to complete committing their metadata while stopping the controller.
  private static final long MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS = 30_000L;

  // TODO: make this configurable with default set to 10
  /**
   * After step 1 of segment completion is done,
   * this is the max time until which step 3 is allowed to complete.
   * See {@link #commitSegmentMetadataInternal(String, CommittingSegmentDescriptor)} for explanation of steps 1 2 3
   * This includes any backoffs and retries for the steps 2 and 3
   * The segment will be eligible for repairs by the validation manager, if the time  exceeds this value
   */
  private static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 300_000L; // 5 MINUTES

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;
  private final MetadataEventNotifierFactory _metadataEventNotifierFactory;
  private final int _numIdealStateUpdateLocks;
  private final Lock[] _idealStateUpdateLocks;
  private final TableConfigCache _tableConfigCache;
  private final FlushThresholdUpdateManager _flushThresholdUpdateManager;

  private volatile boolean _isStopping = false;
  private AtomicInteger _numCompletingSegments = new AtomicInteger(0);

  public PinotLLCRealtimeSegmentManager(PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    _helixAdmin = helixResourceManager.getHelixAdmin();
    _helixManager = helixResourceManager.getHelixZkManager();
    _propertyStore = helixResourceManager.getPropertyStore();
    _helixResourceManager = helixResourceManager;
    _clusterName = helixResourceManager.getHelixClusterName();
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
    _metadataEventNotifierFactory =
        MetadataEventNotifierFactory.loadFactory(controllerConf.subset(METADATA_EVENT_NOTIFIER_PREFIX));
    _numIdealStateUpdateLocks = controllerConf.getRealtimeSegmentMetadataCommitNumLocks();
    _idealStateUpdateLocks = new Lock[_numIdealStateUpdateLocks];
    for (int i = 0; i < _numIdealStateUpdateLocks; i++) {
      _idealStateUpdateLocks[i] = new ReentrantLock();
    }
    _tableConfigCache = new TableConfigCache(_propertyStore);
    _flushThresholdUpdateManager = new FlushThresholdUpdateManager();
  }

  public boolean getIsSplitCommitEnabled() {
    return _controllerConf.getAcceptSplitCommit();
  }

  public String getControllerVipUrl() {
    return _controllerConf.generateVipUrl();
  }

  public void stop() {
    _isStopping = true;
    LOGGER
        .info("Awaiting segment metadata commits: maxWaitTimeMillis = {}", MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS);
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
        LOGGER.info("Interrupted: Remaining wait time {} (out of {})", millisToWait,
            MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS);
        return;
      }
    }
    LOGGER.info("Wait completed: Number of completing segments = {}", _numCompletingSegments.get());
  }

  /**
   * Sets up the initial segments for a new LLC real-time table.
   * <p>NOTE: the passed in IdealState may contain HLC segments if both HLC and LLC are configured.
   */
  public void setUpNewTable(TableConfig tableConfig, IdealState idealState) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = tableConfig.getTableName();
    LOGGER.info("Setting up new LLC table: {}", realtimeTableName);

    // Make sure all the existing segments are HLC segments
    List<String> currentSegments = getAllSegments(realtimeTableName);
    for (String segmentName : currentSegments) {
      // TODO: Should return 4xx HTTP status code. Currently all exceptions are returning 500
      Preconditions.checkState(SegmentName.isHighLevelConsumerSegmentName(segmentName),
          "Cannot set up new LLC table: %s with existing non-HLC segment: %s", realtimeTableName, segmentName);
    }

    _flushThresholdUpdateManager.clearFlushThresholdUpdater(realtimeTableName);

    PartitionLevelStreamConfig streamConfig = new PartitionLevelStreamConfig(tableConfig);
    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    int numPartitions = getNumPartitions(streamConfig);
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    long currentTimeMs = getCurrentTimeMs();
    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      String segmentName =
          setupNewPartition(tableConfig, streamConfig, partitionId, currentTimeMs, instancePartitions, numPartitions,
              numReplicas);
      updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, segmentName, segmentAssignment,
          instancePartitionsMap);
    }

    setIdealState(realtimeTableName, idealState);
  }

  /**
   * Removes all LLC segments from the given IdealState.
   */
  public void removeLLCSegments(IdealState idealState) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = idealState.getResourceName();
    LOGGER.info("Removing LLC segments for table: {}", realtimeTableName);

    List<String> segmentsToRemove = new ArrayList<>();
    for (String segmentName : idealState.getRecord().getMapFields().keySet()) {
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        segmentsToRemove.add(segmentName);
      }
    }
    _helixResourceManager.deleteSegments(realtimeTableName, segmentsToRemove);
  }

  @VisibleForTesting
  TableConfig getTableConfig(String realtimeTableName) {
    try {
      return _tableConfigCache.getTableConfig(realtimeTableName);
    } catch (ExecutionException e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw new IllegalStateException(
          "Caught exception while loading table config from property store to cache for table: " + realtimeTableName,
          e);
    }
  }

  @VisibleForTesting
  InstancePartitions getConsumingInstancePartitions(TableConfig tableConfig) {
    try {
      return InstancePartitionsUtils
          .fetchOrComputeInstancePartitions(_helixManager, tableConfig, InstancePartitionsType.CONSUMING);
    } catch (Exception e) {
      _controllerMetrics
          .addMeteredTableValue(tableConfig.getTableName(), ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  List<String> getAllSegments(String realtimeTableName) {
    try {
      return ZKMetadataProvider.getSegments(_propertyStore, realtimeTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  List<String> getLLCSegments(String realtimeTableName) {
    try {
      return ZKMetadataProvider.getLLCRealtimeSegments(_propertyStore, realtimeTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  private LLCRealtimeSegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName) {
    return getSegmentZKMetadata(realtimeTableName, segmentName, null);
  }

  @VisibleForTesting
  LLCRealtimeSegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName, @Nullable Stat stat) {
    try {
      ZNRecord znRecord = _propertyStore
          .get(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName), stat,
              AccessOption.PERSISTENT);
      Preconditions
          .checkState(znRecord != null, "Failed to find segment ZK metadata for segment: %s of table: %s", segmentName,
              realtimeTableName);
      return new LLCRealtimeSegmentZKMetadata(znRecord);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  void persistSegmentZKMetadata(String realtimeTableName, LLCRealtimeSegmentZKMetadata segmentZKMetadata,
      int expectedVersion) {
    String segmentName = segmentZKMetadata.getSegmentName();
    LOGGER.info("Persisting segment ZK metadata for segment: {}", segmentName);
    try {
      Preconditions.checkState(_propertyStore
              .set(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName),
                  segmentZKMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT),
          "Failed to persist segment ZK metadata for segment: %s of table: %s", segmentName, realtimeTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  IdealState getIdealState(String realtimeTableName) {
    try {
      IdealState idealState = HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
      Preconditions.checkState(idealState != null, "Failed to find IdealState for table: " + realtimeTableName);
      return idealState;
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  void setIdealState(String realtimeTableName, IdealState idealState) {
    try {
      _helixAdmin.setResourceIdealState(_clusterName, realtimeTableName, idealState);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      throw e;
    }
  }

  /**
   * This method moves the segment file from another location to its permanent location.
   * When splitCommit is enabled, segment file is uploaded to the segmentLocation in the committingSegmentDescriptor,
   * and we need to move the segment file to its permanent location before committing the segment metadata.
   */
  public void commitSegmentFile(String realtimeTableName, CommittingSegmentDescriptor committingSegmentDescriptor)
      throws Exception {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    String segmentName = committingSegmentDescriptor.getSegmentName();
    LOGGER.info("Committing segment file for segment: {}", segmentName);

    // Copy the segment file to the controller
    String segmentLocation = committingSegmentDescriptor.getSegmentLocation();
    URI segmentFileURI = URIUtils.getUri(segmentLocation);
    URI tableDirURI = URIUtils.getUri(_controllerConf.getDataDir(), rawTableName);
    URI uriToMoveTo = URIUtils.getUri(_controllerConf.getDataDir(), rawTableName, URIUtils.encode(segmentName));
    PinotFS pinotFS = PinotFSFactory.create(tableDirURI.getScheme());
    Preconditions.checkState(pinotFS.move(segmentFileURI, uriToMoveTo, true),
        "Failed to move segment file for segment: %s from: %s to: %s", segmentName, segmentLocation, uriToMoveTo);

    // Cleans up tmp segment files under table dir.
    // We only clean up tmp segment files in table level dir, so there's no need to list recursively.
    // See LLCSegmentCompletionHandlers.uploadSegment().
    // TODO: move tmp file logic into SegmentCompletionUtils.
    try {
      for (String uri : pinotFS.listFiles(tableDirURI, false)) {
        if (uri.contains(SegmentCompletionUtils.getSegmentNamePrefix(segmentName))) {
          LOGGER.warn("Deleting temporary segment file: {}", uri);
          Preconditions.checkState(pinotFS.delete(new URI(uri), true), "Failed to delete file: %s", uri);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while deleting temporary segment files for segment: {}", segmentName, e);
    }
  }

  /**
   * This method is invoked after the realtime segment is uploaded but before a response is sent to the server.
   * It updates the propertystore segment metadata from IN_PROGRESS to DONE, and also creates new propertystore
   * records for new segments, and puts them in idealstate in CONSUMING state.
   */
  public void commitSegmentMetadata(String realtimeTableName, CommittingSegmentDescriptor committingSegmentDescriptor) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    try {
      _numCompletingSegments.addAndGet(1);
      commitSegmentMetadataInternal(realtimeTableName, committingSegmentDescriptor);
    } finally {
      _numCompletingSegments.addAndGet(-1);
    }
  }

  private void commitSegmentMetadataInternal(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String committingSegmentName = committingSegmentDescriptor.getSegmentName();
    LOGGER.info("Committing segment metadata for segment: {}", committingSegmentName);

    TableConfig tableConfig = getTableConfig(realtimeTableName);
    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    IdealState idealState = getIdealState(realtimeTableName);
    Preconditions.checkState(idealState.getInstanceStateMap(committingSegmentName)
            .containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING),
        "Failed to find instance in CONSUMING state in IdealState for segment: %s", committingSegmentName);
    int numPartitions = getNumPartitionsFromIdealState(idealState);
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    /*
     * Update zookeeper in 3 steps.
     *
     * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE
     * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
     * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
     */

    // Step-1
    LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata =
        updateCommittingSegmentZKMetadata(realtimeTableName, committingSegmentDescriptor);

    // Step-2
    long newSegmentCreationTimeMs = getCurrentTimeMs();
    LLCSegmentName newLLCSegmentName =
        getNextLLCSegmentName(new LLCSegmentName(committingSegmentName), newSegmentCreationTimeMs);
    createNewSegmentZKMetadata(tableConfig, new PartitionLevelStreamConfig(tableConfig), newLLCSegmentName,
        newSegmentCreationTimeMs, committingSegmentDescriptor, committingSegmentZKMetadata, instancePartitions,
        numPartitions, numReplicas);

    // Step-3
    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    // When multiple segments of the same table complete around the same time it is possible that
    // the idealstate update fails due to contention. We serialize the updates to the idealstate
    // to reduce this contention. We may still contend with RetentionManager, or other updates
    // to idealstate from other controllers, but then we have the retry mechanism to get around that.
    // hash code can be negative, so make sure we are getting a positive lock index
    int lockIndex = (realtimeTableName.hashCode() & Integer.MAX_VALUE) % _numIdealStateUpdateLocks;
    Lock lock = _idealStateUpdateLocks[lockIndex];
    try {
      lock.lock();
      updateIdealStateOnSegmentCompletion(realtimeTableName, committingSegmentName, newLLCSegmentName.getSegmentName(),
          segmentAssignment, instancePartitionsMap);
    } finally {
      lock.unlock();
    }

    // Trigger the metadata event notifier
    _metadataEventNotifierFactory.create().notifyOnSegmentFlush(tableConfig);
  }

  /**
   * Updates segment ZK metadata for the committing segment.
   */
  private LLCRealtimeSegmentZKMetadata updateCommittingSegmentZKMetadata(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String segmentName = committingSegmentDescriptor.getSegmentName();
    LOGGER.info("Updating segment ZK metadata for committing segment: {}", segmentName);

    Stat stat = new Stat();
    LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata =
        getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    Preconditions.checkState(committingSegmentZKMetadata.getStatus() == Status.IN_PROGRESS,
        "Segment status for segment: %s should be IN_PROGRESS, found: %s", segmentName,
        committingSegmentZKMetadata.getStatus());
    SegmentMetadataImpl segmentMetadata = committingSegmentDescriptor.getSegmentMetadata();
    Preconditions.checkState(segmentMetadata != null, "Failed to find segment metadata from descriptor for segment: %s",
        segmentName);

    committingSegmentZKMetadata.setEndOffset(committingSegmentDescriptor.getNextOffset());
    committingSegmentZKMetadata.setStatus(Status.DONE);
    committingSegmentZKMetadata.setDownloadUrl(URIUtils
        .constructDownloadUrl(_controllerConf.generateVipUrl(), TableNameBuilder.extractRawTableName(realtimeTableName),
            segmentName));
    committingSegmentZKMetadata.setCrc(Long.valueOf(segmentMetadata.getCrc()));
    committingSegmentZKMetadata.setStartTime(segmentMetadata.getTimeInterval().getStartMillis());
    committingSegmentZKMetadata.setEndTime(segmentMetadata.getTimeInterval().getEndMillis());
    committingSegmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    committingSegmentZKMetadata.setIndexVersion(segmentMetadata.getVersion());
    committingSegmentZKMetadata.setTotalRawDocs(segmentMetadata.getTotalRawDocs());

    persistSegmentZKMetadata(realtimeTableName, committingSegmentZKMetadata, stat.getVersion());
    return committingSegmentZKMetadata;
  }

  /**
   * Creates and persists segment ZK metadata for the new CONSUMING segment.
   */
  private void createNewSegmentZKMetadata(TableConfig tableConfig, PartitionLevelStreamConfig streamConfig,
      LLCSegmentName newLLCSegmentName, long creationTimeMs, CommittingSegmentDescriptor committingSegmentDescriptor,
      @Nullable LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata, InstancePartitions instancePartitions,
      int numPartitions, int numReplicas) {
    String realtimeTableName = tableConfig.getTableName();
    String segmentName = newLLCSegmentName.getSegmentName();
    long startOffset = committingSegmentDescriptor.getNextOffset();
    LOGGER
        .info("Creating segment ZK metadata for new CONSUMING segment: {} with start offset: {} and creation time: {}",
            segmentName, startOffset, creationTimeMs);

    LLCRealtimeSegmentZKMetadata newSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegmentZKMetadata.setTableName(realtimeTableName);
    newSegmentZKMetadata.setSegmentName(segmentName);
    newSegmentZKMetadata.setCreationTime(creationTimeMs);
    newSegmentZKMetadata.setStartOffset(startOffset);
    newSegmentZKMetadata.setEndOffset(END_OFFSET_FOR_CONSUMING_SEGMENTS);
    newSegmentZKMetadata.setNumReplicas(numReplicas);
    newSegmentZKMetadata.setStatus(Status.IN_PROGRESS);

    // Add the partition metadata if available
    SegmentPartitionMetadata partitionMetadata =
        getPartitionMetadataFromTableConfig(tableConfig, newLLCSegmentName.getPartitionId());
    if (partitionMetadata != null) {
      newSegmentZKMetadata.setPartitionMetadata(partitionMetadata);
    }

    // Update the flush threshold
    FlushThresholdUpdater flushThresholdUpdater = _flushThresholdUpdateManager.getFlushThresholdUpdater(streamConfig);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, getMaxNumPartitionsPerInstance(instancePartitions, numPartitions, numReplicas));

    persistSegmentZKMetadata(realtimeTableName, newSegmentZKMetadata, -1);
  }

  @Nullable
  private SegmentPartitionMetadata getPartitionMetadataFromTableConfig(TableConfig tableConfig, int partitionId) {
    SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (partitionConfig == null) {
      return null;
    }
    Map<String, ColumnPartitionMetadata> partitionMetadataMap = new TreeMap<>();
    for (Map.Entry<String, ColumnPartitionConfig> entry : partitionConfig.getColumnPartitionMap().entrySet()) {
      String columnName = entry.getKey();
      ColumnPartitionConfig columnPartitionConfig = entry.getValue();
      partitionMetadataMap.put(columnName,
          new ColumnPartitionMetadata(columnPartitionConfig.getFunctionName(), columnPartitionConfig.getNumPartitions(),
              Collections.singleton(partitionId)));
    }
    return new SegmentPartitionMetadata(partitionMetadataMap);
  }

  public long getCommitTimeoutMS(String realtimeTableName) {
    long commitTimeoutMS = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
    if (_propertyStore == null) {
      return commitTimeoutMS;
    }
    TableConfig tableConfig = getTableConfig(realtimeTableName);
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

  @VisibleForTesting
  int getNumPartitions(StreamConfig streamConfig) {
    return PinotTableIdealStateBuilder.getPartitionCount(streamConfig);
  }

  @VisibleForTesting
  long getPartitionOffset(StreamConfig streamConfig, OffsetCriteria offsetCriteria, int partitionId) {
    PartitionOffsetFetcher partitionOffsetFetcher =
        new PartitionOffsetFetcher(offsetCriteria, partitionId, streamConfig);
    try {
      RetryPolicies.fixedDelayRetryPolicy(3, 1000L).attempt(partitionOffsetFetcher);
      return partitionOffsetFetcher.getOffset();
    } catch (Exception e) {
      throw new IllegalStateException(String
          .format("Failed to fetch the offset for topic: %s, partition: %s with criteria: %s",
              streamConfig.getTopicName(), partitionId, offsetCriteria));
    }
  }

  /**
   * An instance is reporting that it has stopped consuming a topic due to some error.
   * If the segment is in CONSUMING state, mark the state of the segment to be OFFLINE in idealstate.
   * When all replicas of this segment are marked offline, the {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager},
   * in its next run, will auto-create a new segment with the appropriate offset.
   */
  public void segmentStoppedConsuming(LLCSegmentName llcSegmentName, String instanceName) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());
    String segmentName = llcSegmentName.getSegmentName();
    LOGGER.info("Marking CONSUMING segment: {} OFFLINE on instance: {}", segmentName, instanceName);

    try {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
        assert idealState != null;
        Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
        String state = stateMap.get(instanceName);
        if (RealtimeSegmentOnlineOfflineStateModel.CONSUMING.equals(state)) {
          stateMap.put(instanceName, RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        } else {
          LOGGER.info("Segment {} in state {} when trying to register consumption stop from {}",
              segmentName, state, instanceName);
        }
        return idealState;
      }, RetryPolicies.exponentialBackoffRetryPolicy(10, 500L, 1.2f), true);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      throw e;
    }
  }

  /**
   * Returns the latest LLC realtime segment ZK metadata for each partition.
   *
   * @param realtimeTableName Realtime table name
   * @return Map from partition id to the latest LLC realtime segment ZK metadata
   */
  private Map<Integer, LLCRealtimeSegmentZKMetadata> getLatestSegmentZKMetadataMap(String realtimeTableName) {
    List<String> segments = getLLCSegments(realtimeTableName);

    Map<Integer, LLCSegmentName> latestLLCSegmentNameMap = new HashMap<>();
    for (String segmentName : segments) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      latestLLCSegmentNameMap.compute(llcSegmentName.getPartitionId(), (partitionId, latestLLCSegmentName) -> {
        if (latestLLCSegmentName == null) {
          return llcSegmentName;
        } else {
          if (llcSegmentName.getSequenceNumber() > latestLLCSegmentName.getSequenceNumber()) {
            return llcSegmentName;
          } else {
            return latestLLCSegmentName;
          }
        }
      });
    }

    Map<Integer, LLCRealtimeSegmentZKMetadata> latestSegmentZKMetadataMap = new HashMap<>();
    for (Map.Entry<Integer, LLCSegmentName> entry : latestLLCSegmentNameMap.entrySet()) {
      LLCRealtimeSegmentZKMetadata latestSegmentZKMetadata =
          getSegmentZKMetadata(realtimeTableName, entry.getValue().getSegmentName());
      latestSegmentZKMetadataMap.put(entry.getKey(), latestSegmentZKMetadata);
    }

    return latestSegmentZKMetadataMap;
  }

  /**
   * Validates LLC segments in ideal state and repairs them if necessary. This method should only be called from the
   * leader of the table.
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
  public void ensureAllPartitionsConsuming(TableConfig tableConfig, PartitionLevelStreamConfig streamConfig) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = tableConfig.getTableName();
    int numPartitions = getNumPartitions(streamConfig);
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
      assert idealState != null;
      if (idealState.isEnabled()) {
        return ensureAllPartitionsConsuming(tableConfig, streamConfig, idealState, numPartitions);
      } else {
        LOGGER.info("Skipping LLC segments validation for disabled table: {}", realtimeTableName);
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f), true);
  }

  /**
   * Updates ideal state after completion of a realtime segment
   */
  @VisibleForTesting
  void updateIdealStateOnSegmentCompletion(String realtimeTableName, String committingSegmentName,
      String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
      assert idealState != null;
      // When segment completion begins, the zk metadata is updated, followed by ideal state.
      // We allow only {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms for a segment to complete,
      // after which the segment is eligible for repairs by the {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager}
      // After updating metadata, if more than {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms elapse and ideal state is still not updated,
      // the segment could have already been fixed by {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager}
      // Therefore, we do not want to proceed with ideal state update if max segment completion time has exceeded
      if (isExceededMaxSegmentCompletionTime(realtimeTableName, committingSegmentName, getCurrentTimeMs())) {
        LOGGER.error("Exceeded max segment completion time. Skipping ideal state update for segment: {}",
            committingSegmentName);
        throw new HelixHelper.PermanentUpdaterException(
            "Exceeded max segment completion time for segment " + committingSegmentName);
      }
      updateInstanceStatesForNewConsumingSegment(idealState.getRecord().getMapFields(), committingSegmentName,
          newSegmentName, segmentAssignment, instancePartitionsMap);
      return idealState;
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
  }

  @VisibleForTesting
  void updateInstanceStatesForNewConsumingSegment(Map<String, Map<String, String>> instanceStatesMap,
      @Nullable String committingSegmentName, String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    if (committingSegmentName != null) {
      // Change committing segment state to ONLINE
      Set<String> instances = instanceStatesMap.get(committingSegmentName).keySet();
      instanceStatesMap.put(committingSegmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instances, RealtimeSegmentOnlineOfflineStateModel.ONLINE));
      LOGGER.info("Updating segment: {} to ONLINE state", committingSegmentName);
    }

    // Assign instances to the new segment and add instances as state CONSUMING
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(newSegmentName, instanceStatesMap, instancePartitionsMap);
    instanceStatesMap.put(newSegmentName, SegmentAssignmentUtils
        .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
    LOGGER.info("Adding new CONSUMING segment: {} to instances: {}", newSegmentName, instancesAssigned);
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

  /**
   * Returns true if more than {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms have elapsed since segment metadata update
   */
  @VisibleForTesting
  boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName, long currentTimeMs) {
    Stat stat = new Stat();
    getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    if (currentTimeMs > stat.getMtime() + MAX_SEGMENT_COMPLETION_TIME_MILLIS) {
      LOGGER.info("Segment: {} exceeds the max completion time: {}ms, metadata update time: {}, current time: {}",
          segmentName, MAX_SEGMENT_COMPLETION_TIME_MILLIS, stat.getMtime(), currentTimeMs);
      return true;
    } else {
      return false;
    }
  }

  private boolean isAllInstancesInState(Map<String, String> instanceStateMap, String state) {
    for (String value : instanceStateMap.values()) {
      if (!value.equals(state)) {
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
  IdealState ensureAllPartitionsConsuming(TableConfig tableConfig, PartitionLevelStreamConfig streamConfig,
      IdealState idealState, int numPartitions) {
    String realtimeTableName = tableConfig.getTableName();

    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    long currentTimeMs = getCurrentTimeMs();

    // Get the latest segment ZK metadata for each partition
    Map<Integer, LLCRealtimeSegmentZKMetadata> latestSegmentZKMetadataMap =
        getLatestSegmentZKMetadataMap(realtimeTableName);

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
    for (Map.Entry<Integer, LLCRealtimeSegmentZKMetadata> entry : latestSegmentZKMetadataMap.entrySet()) {
      int partitionId = entry.getKey();
      LLCRealtimeSegmentZKMetadata latestSegmentZKMetadata = entry.getValue();
      String latestSegmentName = latestSegmentZKMetadata.getSegmentName();
      LLCSegmentName latestLLCSegmentName = new LLCSegmentName(latestSegmentName);

      Map<String, String> instanceStateMap = instanceStatesMap.get(latestSegmentName);
      if (instanceStateMap != null) {
        // Latest segment of metadata is in idealstate.
        if (instanceStateMap.values().contains(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          if (latestSegmentZKMetadata.getStatus() == Status.DONE) {

            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done either.
            if (!isExceededMaxSegmentCompletionTime(realtimeTableName, latestSegmentName, currentTimeMs)) {
              continue;
            }
            LOGGER.info("Repairing segment: {} which is DONE in segment ZK metadata, but is CONSUMING in IdealState",
                latestSegmentName);

            LLCSegmentName newLLCSegmentName = getNextLLCSegmentName(latestLLCSegmentName, currentTimeMs);
            String newSegmentName = newLLCSegmentName.getSegmentName();
            CommittingSegmentDescriptor committingSegmentDescriptor =
                new CommittingSegmentDescriptor(latestSegmentName, latestSegmentZKMetadata.getEndOffset(), 0);
            createNewSegmentZKMetadata(tableConfig, streamConfig, newLLCSegmentName, currentTimeMs,
                committingSegmentDescriptor, latestSegmentZKMetadata, instancePartitions, numPartitions, numReplicas);
            updateInstanceStatesForNewConsumingSegment(instanceStatesMap, latestSegmentName, newSegmentName,
                segmentAssignment, instancePartitionsMap);
          }
          // else, the metadata should be IN_PROGRESS, which is the right state for a consuming segment.
        } else { // no replica in CONSUMING state

          // Possible scenarios: for any of these scenarios, we need to create new metadata IN_PROGRESS and new CONSUMING segment
          // 1. all replicas OFFLINE and metadata IN_PROGRESS/DONE - a segment marked itself OFFLINE during consumption for some reason
          // 2. all replicas ONLINE and metadata DONE - Resolved in https://github.com/linkedin/pinot/pull/2890
          // 3. we should never end up with some replicas ONLINE and some OFFLINE.
          if (isAllInstancesInState(instanceStateMap, RealtimeSegmentOnlineOfflineStateModel.OFFLINE)) {
            LOGGER.info("Repairing segment: {} which is OFFLINE for all instances in IdealState", latestSegmentName);

            // Create a new segment to re-consume from the previous start offset
            LLCSegmentName newLLCSegmentName = getNextLLCSegmentName(latestLLCSegmentName, currentTimeMs);
            long startOffset = latestSegmentZKMetadata.getStartOffset();
            // Start offset must be higher than the start offset of the stream
            long partitionStartOffset =
                getPartitionOffset(streamConfig, OffsetCriteria.SMALLEST_OFFSET_CRITERIA, partitionId);
            if (partitionStartOffset > startOffset) {
              LOGGER.error("Data lost from offset: {} to: {} for partition: {} of table: {}", startOffset,
                  partitionStartOffset, partitionId, realtimeTableName);
              _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_STREAM_DATA_LOSS, 1L);
              startOffset = partitionStartOffset;
            }

            CommittingSegmentDescriptor committingSegmentDescriptor =
                new CommittingSegmentDescriptor(latestSegmentName, startOffset, 0);
            createNewSegmentZKMetadata(tableConfig, streamConfig, newLLCSegmentName, currentTimeMs,
                committingSegmentDescriptor, latestSegmentZKMetadata, instancePartitions, numPartitions, numReplicas);
            String newSegmentName = newLLCSegmentName.getSegmentName();
            updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, newSegmentName, segmentAssignment,
                instancePartitionsMap);
          } else {
            // If we get here, that means in IdealState, the latest segment has no CONSUMING replicas, but has replicas
            // not OFFLINE. That is an unexpected state which cannot be fixed by the validation manager currently. In
            // that case, we need to either extend this part to handle the state, or prevent segments from getting into
            // such state.
            LOGGER.error("Got unexpected instance state map: {} for segment: {}", instanceStateMap, latestSegmentName);
          }
        }
      } else {
        // idealstate does not have an entry for the segment (but metadata is present)
        // controller has failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE)
        // and creating new segment metadata (new segment metadata state = IN_PROGRESS),
        // but before updating ideal state (new segment ideal missing from ideal state)
        if (!isExceededMaxSegmentCompletionTime(realtimeTableName, latestSegmentName, currentTimeMs)) {
          continue;
        }
        LOGGER.info("Repairing segment: {} which has segment ZK metadata but does not exist in IdealState",
            latestSegmentName);

        if (latestSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
          // Find the previous CONSUMING segment
          String previousConsumingSegment = null;
          for (Map.Entry<String, Map<String, String>> segmentEntry : instanceStatesMap.entrySet()) {
            LLCSegmentName llcSegmentName = new LLCSegmentName(segmentEntry.getKey());
            if (llcSegmentName.getPartitionId() == partitionId && segmentEntry.getValue()
                .containsValue(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
              previousConsumingSegment = llcSegmentName.getSegmentName();
              break;
            }
          }
          if (previousConsumingSegment == null) {
            LOGGER
                .error("Failed to find previous CONSUMING segment for partition: {} of table: {}, potential data loss",
                    partitionId, realtimeTableName);
            _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_STREAM_DATA_LOSS, 1L);
          }
          updateInstanceStatesForNewConsumingSegment(instanceStatesMap, previousConsumingSegment, latestSegmentName,
              segmentAssignment, instancePartitionsMap);
        } else {
          LOGGER.error("Got unexpected status: {} in segment ZK metadata for segment: {}",
              latestSegmentZKMetadata.getStatus(), latestSegmentName);
        }
      }
    }

    // Set up new partitions if not exist
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      if (!latestSegmentZKMetadataMap.containsKey(partitionId)) {
        String newSegmentName =
            setupNewPartition(tableConfig, streamConfig, partitionId, currentTimeMs, instancePartitions, numPartitions,
                numReplicas);
        updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, newSegmentName, segmentAssignment,
            instancePartitionsMap);
      }
    }

    return idealState;
  }

  private LLCSegmentName getNextLLCSegmentName(LLCSegmentName lastLLCSegmentName, long creationTimeMs) {
    return new LLCSegmentName(lastLLCSegmentName.getTableName(), lastLLCSegmentName.getPartitionId(),
        lastLLCSegmentName.getSequenceNumber() + 1, creationTimeMs);
  }

  /**
   * Sets up a new partition.
   * <p>Persists the ZK metadata for the first CONSUMING segment, and returns the segment name.
   */
  private String setupNewPartition(TableConfig tableConfig, PartitionLevelStreamConfig streamConfig, int partitionId,
      long creationTimeMs, InstancePartitions instancePartitions, int numPartitions, int numReplicas) {
    String realtimeTableName = tableConfig.getTableName();
    LOGGER.info("Setting up new partition: {} for table: {}", partitionId, realtimeTableName);

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    LLCSegmentName newLLCSegmentName =
        new LLCSegmentName(rawTableName, partitionId, STARTING_SEQUENCE_NUMBER, creationTimeMs);
    String newSegmentName = newLLCSegmentName.getSegmentName();
    long startOffset = getPartitionOffset(streamConfig, streamConfig.getOffsetCriteria(), partitionId);
    CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(null, startOffset, 0);
    createNewSegmentZKMetadata(tableConfig, streamConfig, newLLCSegmentName, creationTimeMs,
        committingSegmentDescriptor, null, instancePartitions, numPartitions, numReplicas);

    return newSegmentName;
  }

  @VisibleForTesting
  long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  private int getNumPartitionsFromIdealState(IdealState idealState) {
    int numPartitions = 0;
    for (String segmentName : idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        numPartitions = Math.max(numPartitions, new LLCSegmentName(segmentName).getPartitionId() + 1);
      }
    }
    return numPartitions;
  }

  private int getNumReplicas(TableConfig tableConfig, InstancePartitions instancePartitions) {
    if (instancePartitions.getNumReplicaGroups() == 1) {
      // Non-replica-group based
      return tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    } else {
      // Replica-group based
      return instancePartitions.getNumReplicaGroups();
    }
  }

  private int getMaxNumPartitionsPerInstance(InstancePartitions instancePartitions, int numPartitions,
      int numReplicas) {
    if (instancePartitions.getNumReplicaGroups() == 1) {
      // Non-replica-group based assignment:
      // Uniformly spray the partitions and replicas across the instances.
      // E.g. (6 instances, 3 partitions, 4 replicas)
      // "0_0": [i0,  i1,  i2,  i3,  i4,  i5  ]
      //         p0r0 p0r1 p0r2 p0r3 p1r0 p1r1
      //         p1r2 p1r3 p2r0 p2r1 p2r2 p2r3

      int numInstances = instancePartitions.getInstances(0, 0).size();
      return (numPartitions * numReplicas + numInstances - 1) / numInstances;
    } else {
      // Replica-group based assignment:
      // Within a replica-group, uniformly spray the partitions across the instances.
      // E.g. (within a replica-group, 3 instances, 6 partitions)
      // "0_0": [i0, i1, i2]
      //         p0  p1  p2
      //         p3  p4  p5

      int numInstancesPerReplicaGroup = instancePartitions.getInstances(0, 0).size();
      return (numPartitions + numInstancesPerReplicaGroup - 1) / numInstancesPerReplicaGroup;
    }
  }
}
