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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.messages.ForceCommitMessage;
import org.apache.pinot.common.messages.IngestionMetricsRemoveMessage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.restlet.resources.TableLLCSegmentUploadResponse;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdateManager;
import org.apache.pinot.controller.helix.core.realtime.segment.FlushThresholdUpdater;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
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
 *   <li>uploadToDeepStoreIfMissing(): From lead controller only</li>
 * </ul>
 *
 * TODO: migrate code in this class to other places for better readability
 */
public class PinotLLCRealtimeSegmentManager {

  // simple field in Ideal State representing pause status for the table
  // Deprecated in favour of PAUSE_STATE
  @Deprecated
  public static final String IS_TABLE_PAUSED = "isTablePaused";
  public static final String PAUSE_STATE = "pauseState";
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);

  private static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments
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
  /**
   * When controller asks server to upload missing LLC segment copy to deep store, it could happen that the segment
   * retention is short time away, and RetentionManager walks in to purge the segment. To avoid this data racing issue,
   * check the segment expiration time to see if it is about to be deleted (i.e. less than this threshold). Skip the
   * deep store fix if necessary. RetentionManager will delete this kind of segments shortly anyway.
   */
  private static final long MIN_TIME_BEFORE_SEGMENT_EXPIRATION_FOR_FIXING_DEEP_STORE_COPY_MILLIS = 60 * 60 * 1000L;
  // 1 hour
  private static final Random RANDOM = new Random();

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;
  private final ControllerConf _controllerConf;
  private final ControllerMetrics _controllerMetrics;
  private final MetadataEventNotifierFactory _metadataEventNotifierFactory;
  private final FlushThresholdUpdateManager _flushThresholdUpdateManager;
  private final boolean _isDeepStoreLLCSegmentUploadRetryEnabled;
  private final boolean _isTmpSegmentAsyncDeletionEnabled;
  private final int _deepstoreUploadRetryTimeoutMs;
  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private final AtomicInteger _numCompletingSegments = new AtomicInteger(0);
  private final ExecutorService _deepStoreUploadExecutor;
  private final Set<String> _deepStoreUploadExecutorPendingSegments;

  private volatile boolean _isStopping = false;

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
        MetadataEventNotifierFactory.loadFactory(controllerConf.subset(METADATA_EVENT_NOTIFIER_PREFIX),
            helixResourceManager);
    _flushThresholdUpdateManager = new FlushThresholdUpdateManager();
    _isDeepStoreLLCSegmentUploadRetryEnabled = controllerConf.isDeepStoreRetryUploadLLCSegmentEnabled();
    _isTmpSegmentAsyncDeletionEnabled = controllerConf.isTmpSegmentAsyncDeletionEnabled();
    _deepstoreUploadRetryTimeoutMs = controllerConf.getDeepStoreRetryUploadTimeoutMs();
    _fileUploadDownloadClient = _isDeepStoreLLCSegmentUploadRetryEnabled ? initFileUploadDownloadClient() : null;
    _deepStoreUploadExecutor = _isDeepStoreLLCSegmentUploadRetryEnabled ? Executors.newFixedThreadPool(
        controllerConf.getDeepStoreRetryUploadParallelism()) : null;
    _deepStoreUploadExecutorPendingSegments =
        _isDeepStoreLLCSegmentUploadRetryEnabled ? ConcurrentHashMap.newKeySet() : null;
  }

  public boolean isDeepStoreLLCSegmentUploadRetryEnabled() {
    return _isDeepStoreLLCSegmentUploadRetryEnabled;
  }

  public boolean isTmpSegmentAsyncDeletionEnabled() {
    return _isTmpSegmentAsyncDeletionEnabled;
  }

  @VisibleForTesting
  FileUploadDownloadClient initFileUploadDownloadClient() {
    return new FileUploadDownloadClient();
  }

  /**
   * Using the ideal state and segment metadata, return a list of {@link PartitionGroupConsumptionStatus}
   * for latest segment of each partition group.
   */
  public List<PartitionGroupConsumptionStatus> getPartitionGroupConsumptionStatusList(IdealState idealState,
      List<StreamConfig> streamConfigs) {
    List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList = new ArrayList<>();

    // From all segment names in the ideal state, find unique partition group ids and their latest segment
    Map<Integer, LLCSegmentName> partitionGroupIdToLatestSegment = new HashMap<>();
    for (String segment : idealState.getRecord().getMapFields().keySet()) {
      // With Pinot upsert table allowing uploads of segments, the segment name of an upsert table segment may not
      // conform to LLCSegment format. We can skip such segments because they are NOT the consuming segments.
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segment);
      if (llcSegmentName == null) {
        continue;
      }
      int partitionGroupId = llcSegmentName.getPartitionGroupId();
      partitionGroupIdToLatestSegment.compute(partitionGroupId, (k, latestSegment) -> {
        if (latestSegment == null) {
          return llcSegmentName;
        } else {
          return latestSegment.getSequenceNumber() > llcSegmentName.getSequenceNumber() ? latestSegment
              : llcSegmentName;
        }
      });
    }

    // Create a {@link PartitionGroupConsumptionStatus} for each latest segment
    StreamPartitionMsgOffsetFactory offsetFactory =
        StreamConsumerFactoryProvider.create(streamConfigs.get(0)).createStreamMsgOffsetFactory();
    for (Map.Entry<Integer, LLCSegmentName> entry : partitionGroupIdToLatestSegment.entrySet()) {
      int partitionGroupId = entry.getKey();
      LLCSegmentName llcSegmentName = entry.getValue();
      SegmentZKMetadata segmentZKMetadata =
          getSegmentZKMetadata(streamConfigs.get(0).getTableNameWithType(), llcSegmentName.getSegmentName());
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partitionGroupId, llcSegmentName.getSequenceNumber(),
              offsetFactory.create(segmentZKMetadata.getStartOffset()),
              segmentZKMetadata.getEndOffset() == null ? null : offsetFactory.create(segmentZKMetadata.getEndOffset()),
              segmentZKMetadata.getStatus().toString());
      partitionGroupConsumptionStatusList.add(partitionGroupConsumptionStatus);
    }
    return partitionGroupConsumptionStatusList;
  }

  public String getControllerVipUrl() {
    return _controllerConf.generateVipUrl();
  }

  public void stop() {
    _isStopping = true;
    LOGGER.info("Awaiting segment metadata commits: maxWaitTimeMillis = {}",
        MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS);
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

    if (_fileUploadDownloadClient != null) {
      try {
        _fileUploadDownloadClient.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close fileUploadDownloadClient.");
      }
    }
  }

  /**
   * Sets up the initial segments for a new LLC real-time table.
   * <p>NOTE: the passed in IdealState may contain HLC segments if both HLC and LLC are configured.
   */
  public void setUpNewTable(TableConfig tableConfig, IdealState idealState) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = tableConfig.getTableName();
    LOGGER.info("Setting up new LLC table: {}", realtimeTableName);

    _flushThresholdUpdateManager.clearFlushThresholdUpdater(realtimeTableName);

    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig).stream().map(
        streamConfig -> new StreamConfig(tableConfig.getTableName(), streamConfig)
    ).collect(Collectors.toList());
    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    List<PartitionGroupMetadata> newPartitionGroupMetadataList =
        getNewPartitionGroupMetadataList(streamConfigs, Collections.emptyList());
    int numPartitionGroups = newPartitionGroupMetadataList.size();
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, _controllerMetrics);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    long currentTimeMs = getCurrentTimeMs();
    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    for (PartitionGroupMetadata partitionGroupMetadata : newPartitionGroupMetadataList) {
      String segmentName =
          setupNewPartitionGroup(tableConfig, streamConfigs.get(0), partitionGroupMetadata, currentTimeMs,
              instancePartitions,
              numPartitionGroups, numReplicas);
      updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, segmentName, segmentAssignment,
          instancePartitionsMap);
    }

    setIdealState(realtimeTableName, idealState);
  }

  // TODO: Consider using TableCache to read the table config
  @VisibleForTesting
  public TableConfig getTableConfig(String realtimeTableName) {
    TableConfig tableConfig;
    try {
      tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, realtimeTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
    if (tableConfig == null) {
      throw new IllegalStateException("Failed to find table config for table: " + realtimeTableName);
    }
    return tableConfig;
  }

  @VisibleForTesting
  InstancePartitions getConsumingInstancePartitions(TableConfig tableConfig) {
    try {
      return InstancePartitionsUtils.fetchOrComputeInstancePartitions(_helixManager, tableConfig,
          InstancePartitionsType.CONSUMING);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableConfig.getTableName(), ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES,
          1L);
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

  private SegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName) {
    return getSegmentZKMetadata(realtimeTableName, segmentName, null);
  }

  @VisibleForTesting
  SegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName, @Nullable Stat stat) {
    try {
      ZNRecord znRecord =
          _propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName),
              stat, AccessOption.PERSISTENT);
      Preconditions.checkState(znRecord != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
          segmentName, realtimeTableName);
      return new SegmentZKMetadata(znRecord);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  void persistSegmentZKMetadata(String realtimeTableName, SegmentZKMetadata segmentZKMetadata, int expectedVersion) {
    String segmentName = segmentZKMetadata.getSegmentName();
    LOGGER.info("Persisting segment ZK metadata for segment: {}", segmentName);
    try {
      Preconditions.checkState(
          _propertyStore.set(ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName),
              segmentZKMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT),
          "Failed to persist segment ZK metadata for segment: %s of table: %s", segmentName, realtimeTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      throw e;
    }
  }

  public IdealState getIdealState(String realtimeTableName) {
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
   * Modifies the segment location in committingSegmentDescriptor to the uri which the segment is moved to
   * unless committingSegmentDescriptor has a peer download uri scheme in segment location.
   */
  public void commitSegmentFile(String realtimeTableName, CommittingSegmentDescriptor committingSegmentDescriptor)
      throws Exception {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    String segmentName = committingSegmentDescriptor.getSegmentName();
    LOGGER.info("Committing segment file for segment: {}", segmentName);

    // Copy the segment file to the controller
    String segmentLocation = committingSegmentDescriptor.getSegmentLocation();
    Preconditions.checkArgument(segmentLocation != null, "Segment location must be provided");
    if (segmentLocation.regionMatches(true, 0, CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME, 0,
        CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME.length())) {
      LOGGER.info("No moving needed for segment on peer servers: {}", segmentLocation);
      return;
    }

    URI tableDirURI = URIUtils.getUri(_controllerConf.getDataDir(), rawTableName);
    PinotFS pinotFS = PinotFSFactory.create(tableDirURI.getScheme());
    String uriToMoveTo = moveSegmentFile(rawTableName, segmentName, segmentLocation, pinotFS);

    if (!isTmpSegmentAsyncDeletionEnabled()) {
      try {
        for (String uri : pinotFS.listFiles(tableDirURI, false)) {
          if (uri.contains(SegmentCompletionUtils.getTmpSegmentNamePrefix(segmentName))) {
            LOGGER.warn("Deleting temporary segment file: {}", uri);
            Preconditions.checkState(pinotFS.delete(new URI(uri), true), "Failed to delete file: %s", uri);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deleting temporary segment files for segment: {}", segmentName, e);
      }
    }
    committingSegmentDescriptor.setSegmentLocation(uriToMoveTo);
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
    LLCSegmentName committingLLCSegment = new LLCSegmentName(committingSegmentName);
    int committingSegmentPartitionGroupId = committingLLCSegment.getPartitionGroupId();
    LOGGER.info("Committing segment metadata for segment: {}", committingSegmentName);
    if (StringUtils.isBlank(committingSegmentDescriptor.getSegmentLocation())) {
      LOGGER.warn("Committing segment: {} was not uploaded to deep store", committingSegmentName);
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.SEGMENT_MISSING_DEEP_STORE_LINK, 1);
    }

    TableConfig tableConfig = getTableConfig(realtimeTableName);
    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    IdealState idealState = getIdealState(realtimeTableName);
    Preconditions.checkState(
        idealState.getInstanceStateMap(committingSegmentName).containsValue(SegmentStateModel.CONSUMING),
        "Failed to find instance in CONSUMING state in IdealState for segment: %s", committingSegmentName);
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    /*
     * Update zookeeper in 3 steps.
     *
     * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE
     * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
     * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
     */

    // Step-1
    long startTimeNs1 = System.nanoTime();
    SegmentZKMetadata committingSegmentZKMetadata =
        updateCommittingSegmentZKMetadata(realtimeTableName, committingSegmentDescriptor);
    // Refresh the Broker routing to reflect the changes in the segment ZK metadata
    _helixResourceManager.sendSegmentRefreshMessage(realtimeTableName, committingSegmentName, false, true);

    // Step-2
    long startTimeNs2 = System.nanoTime();
    String newConsumingSegmentName = null;
    if (!isTablePaused(idealState)) {
      List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig).stream().map(
          streamConfig -> new StreamConfig(tableConfig.getTableName(), streamConfig)
      ).collect(Collectors.toList());
      Set<Integer> partitionIds = getPartitionIds(streamConfigs, idealState);
      if (partitionIds.contains(committingSegmentPartitionGroupId)) {
        String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
        long newSegmentCreationTimeMs = getCurrentTimeMs();
        LLCSegmentName newLLCSegment = new LLCSegmentName(rawTableName, committingSegmentPartitionGroupId,
            committingLLCSegment.getSequenceNumber() + 1, newSegmentCreationTimeMs);
        createNewSegmentZKMetadata(tableConfig, streamConfigs.get(0), newLLCSegment, newSegmentCreationTimeMs,
            committingSegmentDescriptor, committingSegmentZKMetadata, instancePartitions, partitionIds.size(),
            numReplicas);
        newConsumingSegmentName = newLLCSegment.getSegmentName();
      }
    }

    // Step-3
    long startTimeNs3 = System.nanoTime();
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, _controllerMetrics);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    // When multiple segments of the same table complete around the same time it is possible that
    // the idealstate update fails due to contention. We serialize the updates to the idealstate
    // to reduce this contention. We may still contend with RetentionManager, or other updates
    // to idealstate from other controllers, but then we have the retry mechanism to get around that.
    idealState =
        updateIdealStateOnSegmentCompletion(realtimeTableName, committingSegmentName, newConsumingSegmentName,
            segmentAssignment, instancePartitionsMap);

    long endTimeNs = System.nanoTime();
    LOGGER.info(
        "Finished committing segment metadata for segment: {}. Time taken for updating committing segment metadata: "
            + "{}ms; creating new consuming segment ({}) metadata: {}ms; updating ideal state: {}ms; total: {}ms",
        committingSegmentName, TimeUnit.NANOSECONDS.toMillis(startTimeNs2 - startTimeNs1), newConsumingSegmentName,
        TimeUnit.NANOSECONDS.toMillis(startTimeNs3 - startTimeNs2),
        TimeUnit.NANOSECONDS.toMillis(endTimeNs - startTimeNs3),
        TimeUnit.NANOSECONDS.toMillis(endTimeNs - startTimeNs1));

    // TODO: also create the new partition groups here, instead of waiting till the {@link
    //  RealtimeSegmentValidationManager} runs
    //  E.g. If current state is A, B, C, and newPartitionGroupMetadataList contains B, C, D, E,
    //  then create metadata/idealstate entries for D, E along with the committing partition's entries.
    //  Ensure that multiple committing segments don't create multiple new segment metadata and ideal state entries
    //  for the same partitionGroup

    // Trigger the metadata event notifier
    _metadataEventNotifierFactory.create().notifyOnSegmentFlush(tableConfig);

    // Handle segment movement if necessary
    if (newConsumingSegmentName != null) {
      handleSegmentMovement(realtimeTableName, idealState.getRecord().getMapFields(), committingSegmentName,
          newConsumingSegmentName);
    }
  }

  /**
   * Updates segment ZK metadata for the committing segment.
   */
  private SegmentZKMetadata updateCommittingSegmentZKMetadata(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String segmentName = committingSegmentDescriptor.getSegmentName();
    LOGGER.info("Updating segment ZK metadata for committing segment: {}", segmentName);

    Stat stat = new Stat();
    SegmentZKMetadata committingSegmentZKMetadata = getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    Preconditions.checkState(committingSegmentZKMetadata.getStatus() == Status.IN_PROGRESS,
        "Segment status for segment: %s should be IN_PROGRESS, found: %s", segmentName,
        committingSegmentZKMetadata.getStatus());
    SegmentMetadataImpl segmentMetadata = committingSegmentDescriptor.getSegmentMetadata();
    Preconditions.checkState(segmentMetadata != null, "Failed to find segment metadata from descriptor for segment: %s",
        segmentName);

    // TODO Issue 5953 remove the long parsing once metadata is set correctly.
    committingSegmentZKMetadata.setEndOffset(committingSegmentDescriptor.getNextOffset());
    committingSegmentZKMetadata.setStatus(Status.DONE);
    // If the download url set by the server is a peer download url format with peer scheme, put
    // METADATA_URI_FOR_PEER_DOWNLOAD in zk; otherwise just use the location in the descriptor.
    committingSegmentZKMetadata.setDownloadUrl(isPeerURL(committingSegmentDescriptor.getSegmentLocation())
        ? CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD : committingSegmentDescriptor.getSegmentLocation());
    committingSegmentZKMetadata.setCrc(Long.valueOf(segmentMetadata.getCrc()));
    if (segmentMetadata.getTotalDocs() > 0) {
      Preconditions.checkNotNull(segmentMetadata.getTimeInterval(),
          "start/end time information is not correctly written to the segment for table: " + realtimeTableName);
      committingSegmentZKMetadata.setStartTime(segmentMetadata.getTimeInterval().getStartMillis());
      committingSegmentZKMetadata.setEndTime(segmentMetadata.getTimeInterval().getEndMillis());
    } else {
      // Set current time as start/end time if total docs is 0
      long now = System.currentTimeMillis();
      committingSegmentZKMetadata.setStartTime(now);
      committingSegmentZKMetadata.setEndTime(now);
    }
    committingSegmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentVersion segmentVersion = segmentMetadata.getVersion();
    if (segmentVersion != null) {
      committingSegmentZKMetadata.setIndexVersion(segmentVersion.name());
    }
    committingSegmentZKMetadata.setTotalDocs(segmentMetadata.getTotalDocs());
    committingSegmentZKMetadata.setSizeInBytes(committingSegmentDescriptor.getSegmentSizeBytes());

    // Update the partition group metadata based on the segment metadata
    // NOTE: When the stream partition changes, or the records are not properly partitioned from the stream, the
    //       partition of the segment (based on the actual consumed records) can be different from the stream partition.
    committingSegmentZKMetadata.setPartitionMetadata(getPartitionMetadataFromSegmentMetadata(segmentMetadata));

    persistSegmentZKMetadata(realtimeTableName, committingSegmentZKMetadata, stat.getVersion());
    return committingSegmentZKMetadata;
  }

  private boolean isPeerURL(String segmentLocation) {
    return segmentLocation != null && segmentLocation.toLowerCase()
        .startsWith(CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME);
  }

  /**
   * Creates and persists segment ZK metadata for the new CONSUMING segment.
   */
  private void createNewSegmentZKMetadata(TableConfig tableConfig, StreamConfig streamConfig,
      LLCSegmentName newLLCSegmentName, long creationTimeMs, CommittingSegmentDescriptor committingSegmentDescriptor,
      @Nullable SegmentZKMetadata committingSegmentZKMetadata, InstancePartitions instancePartitions, int numPartitions,
      int numReplicas) {
    String realtimeTableName = tableConfig.getTableName();
    String segmentName = newLLCSegmentName.getSegmentName();
    String startOffset = committingSegmentDescriptor.getNextOffset();

    LOGGER.info(
        "Creating segment ZK metadata for new CONSUMING segment: {} with start offset: {} and creation time: {}",
        segmentName, startOffset, creationTimeMs);

    SegmentZKMetadata newSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    newSegmentZKMetadata.setCreationTime(creationTimeMs);
    newSegmentZKMetadata.setStartOffset(startOffset);
    // Leave maxOffset as null.
    newSegmentZKMetadata.setNumReplicas(numReplicas);
    newSegmentZKMetadata.setStatus(Status.IN_PROGRESS);

    // Add the partition metadata if available
    SegmentPartitionMetadata partitionMetadata =
        getPartitionMetadataFromTableConfig(tableConfig, newLLCSegmentName.getPartitionGroupId(), numPartitions);
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
  private SegmentPartitionMetadata getPartitionMetadataFromTableConfig(TableConfig tableConfig, int partitionId,
      int numPartitionGroups) {
    SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (partitionConfig == null) {
      return null;
    }
    Map<String, ColumnPartitionConfig> columnPartitionMap = partitionConfig.getColumnPartitionMap();
    if (columnPartitionMap.size() == 1) {
      Map.Entry<String, ColumnPartitionConfig> entry = columnPartitionMap.entrySet().iterator().next();
      ColumnPartitionConfig columnPartitionConfig = entry.getValue();
      if (numPartitionGroups != columnPartitionConfig.getNumPartitions()) {
        LOGGER.warn("Number of partition groups fetched from the stream '{}' is different than "
                + "columnPartitionConfig.numPartitions '{}' in the table config. The stream partition count is used. "
                + "Please update the table config accordingly.", numPartitionGroups,
            columnPartitionConfig.getNumPartitions());
      }
      ColumnPartitionMetadata columnPartitionMetadata =
          new ColumnPartitionMetadata(columnPartitionConfig.getFunctionName(), numPartitionGroups,
              Collections.singleton(partitionId), columnPartitionConfig.getFunctionConfig());
      return new SegmentPartitionMetadata(Collections.singletonMap(entry.getKey(), columnPartitionMetadata));
    } else {
      LOGGER.warn(
          "Skip persisting partition metadata because there are other than exact one partition column for table: {}",
          tableConfig.getTableName());
      return null;
    }
  }

  @Nullable
  private SegmentPartitionMetadata getPartitionMetadataFromSegmentMetadata(SegmentMetadataImpl segmentMetadata) {
    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      // NOTE: There is at most one partition column.
      ColumnMetadata columnMetadata = entry.getValue();
      PartitionFunction partitionFunction = columnMetadata.getPartitionFunction();
      if (partitionFunction != null) {
        ColumnPartitionMetadata columnPartitionMetadata =
            new ColumnPartitionMetadata(partitionFunction.getName(), partitionFunction.getNumPartitions(),
                columnMetadata.getPartitions(), columnMetadata.getPartitionFunction().getFunctionConfig());
        return new SegmentPartitionMetadata(Collections.singletonMap(entry.getKey(), columnPartitionMetadata));
      }
    }
    return null;
  }

  public long getCommitTimeoutMS(String realtimeTableName) {
    long commitTimeoutMS = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
    if (_propertyStore == null) {
      return commitTimeoutMS;
    }
    TableConfig tableConfig = getTableConfig(realtimeTableName);
    final Map<String, String> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig).get(0);
    if (streamConfigs.containsKey(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS)) {
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
   * Fetches the partition ids for the stream. Some stream (e.g. Kinesis) might not support this operation, in which
   * case exception will be thrown.
   */
  @VisibleForTesting
  Set<Integer> getPartitionIds(StreamConfig streamConfig)
      throws Exception {
    String clientId =
        PinotLLCRealtimeSegmentManager.class.getSimpleName() + "-" + streamConfig.getTableNameWithType() + "-"
            + streamConfig.getTopicName();
    StreamConsumerFactory consumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    try (StreamMetadataProvider metadataProvider = consumerFactory.createStreamMetadataProvider(clientId)) {
      return metadataProvider.fetchPartitionIds(5000L);
    }
  }

  @VisibleForTesting
  Set<Integer> getPartitionIds(List<StreamConfig> streamConfigs, IdealState idealState) {
    Set<Integer> partitionIds = new HashSet<>();
    boolean allPartitionIdsFetched = true;
    for (int i = 0; i < streamConfigs.size(); i++) {
      final int index = i;
      try {
        partitionIds.addAll(getPartitionIds(streamConfigs.get(index)).stream()
            .map(partitionId -> IngestionConfigUtils.getPinotPartitionIdFromStreamPartitionId(partitionId, index))
            .collect(Collectors.toSet()));
      } catch (Exception e) {
        allPartitionIdsFetched = false;
        LOGGER.warn("Failed to fetch partition ids for stream: {}", streamConfigs.get(i).getTopicName(), e);
      }
    }

    // If it is failing to fetch partition ids from stream (usually transient due to stream metadata service outage),
    // we need to use the existing partition information from ideal state to keep same ingestion behavior.
    if (!allPartitionIdsFetched) {
      LOGGER.info(
          "Fetch partition ids from Stream incomplete, merge fetched partitionIds with partition group metadata "
              + "for: {}", idealState.getId());
      // TODO: Find a better way to determine partition count and if the committing partition group is fully consumed.
      //       We don't need to read partition group metadata for other partition groups.
      List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList =
          getPartitionGroupConsumptionStatusList(idealState, streamConfigs);
      List<PartitionGroupMetadata> newPartitionGroupMetadataList =
          getNewPartitionGroupMetadataList(streamConfigs, currentPartitionGroupConsumptionStatusList);
      partitionIds.addAll(newPartitionGroupMetadataList.stream().map(PartitionGroupMetadata::getPartitionGroupId)
          .collect(Collectors.toSet()));
    }
    return partitionIds;
  }

  /**
   * Fetches the latest state of the PartitionGroups for the stream
   * If any partition has reached end of life, and all messages of that partition have been consumed by the segment,
   * it will be skipped from the result
   */
  @VisibleForTesting
  List<PartitionGroupMetadata> getNewPartitionGroupMetadataList(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList) {
    return PinotTableIdealStateBuilder.getPartitionGroupMetadataList(streamConfigs,
        currentPartitionGroupConsumptionStatusList);
  }

  /**
   * An instance is reporting that it has stopped consuming a topic due to some error.
   * If the segment is in CONSUMING state, mark the state of the segment to be OFFLINE in idealstate.
   * When all replicas of this segment are marked offline, the
   * {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager},
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
        if (SegmentStateModel.CONSUMING.equals(state)) {
          stateMap.put(instanceName, SegmentStateModel.OFFLINE);
        } else {
          LOGGER.info("Segment {} in state {} when trying to register consumption stop from {}", segmentName, state,
              instanceName);
        }
        return idealState;
      }, RetryPolicies.exponentialBackoffRetryPolicy(10, 500L, 1.2f), true);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      throw e;
    }
    // We know that we have successfully set the idealstate to be OFFLINE.
    // We can now do a best effort to reset the externalview to be OFFLINE if it is in ERROR state.
    // If the externalview is not in error state, then this reset will be ignored by the helix participant
    // in the server when it receives the ERROR to OFFLINE state transition.
    // Helix throws an exception if we try to reset state of a partition that is NOT in ERROR state in EV,
    // So, if any exceptions are thrown, ignore it here.
    // TODO: https://github.com/apache/pinot/issues/12055
    // If we have reaosn codes, then the server can indicate to us the reason (in this case, consumer was never
    // created, OR consumer was created but could not consume the segment compeltely), and we can call reset()
    // in one of the cases and not the other.
    try {
      _helixAdmin.resetPartition(_helixManager.getClusterName(), instanceName,
          realtimeTableName, Collections.singletonList(segmentName));
    } catch (Exception e) {
      // Ignore
    }
  }

  /**
   * Returns the latest LLC realtime segment ZK metadata for each partition.
   *
   * @param realtimeTableName Realtime table name
   * @return Map from partition group id to the latest LLC realtime segment ZK metadata
   */
  private Map<Integer, SegmentZKMetadata> getLatestSegmentZKMetadataMap(String realtimeTableName) {
    List<String> segments = getLLCSegments(realtimeTableName);

    Map<Integer, LLCSegmentName> latestLLCSegmentNameMap = new HashMap<>();
    for (String segmentName : segments) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      latestLLCSegmentNameMap.compute(llcSegmentName.getPartitionGroupId(), (partitionId, latestLLCSegmentName) -> {
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

    Map<Integer, SegmentZKMetadata> latestSegmentZKMetadataMap = new HashMap<>();
    for (Map.Entry<Integer, LLCSegmentName> entry : latestLLCSegmentNameMap.entrySet()) {
      SegmentZKMetadata latestSegmentZKMetadata =
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
   * Check whether there are any segments in the PROPERTYSTORE with status DONE, but no new segment in status
   * IN_PROGRESS,
   * and hence the status of the segment in the IDEALSTATE is still CONSUMING
   *
   * If it fails between step-2 and-3:
   * Check whether there are any segments in PROPERTYSTORE with status IN_PROGRESS, that are not accounted for in
   * idealState.
   * If so, it should create the new segments in idealState.
   *
   * If the controller fails after step-3, we are fine because the idealState has the new segments.
   * If the controller fails before step-1, the server will see this as an upload failure, and will re-try.
   *
   * If the consuming segment is deleted by user intentionally or by mistake:
   * Check whether there are segments in the PROPERTYSTORE with status DONE, but no new segment in status
   * IN_PROGRESS, and the state for the latest segment in the IDEALSTATE is ONLINE.
   * If so, it should create a new CONSUMING segment for the partition.
   */
  public void ensureAllPartitionsConsuming(TableConfig tableConfig, List<StreamConfig> streamConfigs,
      OffsetCriteria offsetCriteria) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = tableConfig.getTableName();
    HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
      assert idealState != null;
      boolean isTableEnabled = idealState.isEnabled();
      boolean isTablePaused = isTablePaused(idealState);
      boolean offsetsHaveToChange = offsetCriteria != null;
      if (isTableEnabled && !isTablePaused) {
        List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList =
            offsetsHaveToChange
                ? Collections.emptyList() // offsets from metadata are not valid anymore; fetch for all partitions
                : getPartitionGroupConsumptionStatusList(idealState, streamConfigs);
        // FIXME: Right now, we assume topics are sharing same offset criteria
        OffsetCriteria originalOffsetCriteria = streamConfigs.get(0).getOffsetCriteria();
        // Read the smallest offset when a new partition is detected
        streamConfigs.stream().forEach(streamConfig -> streamConfig.setOffsetCriteria(offsetsHaveToChange
            ? offsetCriteria : OffsetCriteria.SMALLEST_OFFSET_CRITERIA));
        List<PartitionGroupMetadata> newPartitionGroupMetadataList =
            getNewPartitionGroupMetadataList(streamConfigs, currentPartitionGroupConsumptionStatusList);
        streamConfigs.stream().forEach(streamConfig -> streamConfig.setOffsetCriteria(originalOffsetCriteria));
        return ensureAllPartitionsConsuming(tableConfig, streamConfigs, idealState, newPartitionGroupMetadataList,
            offsetCriteria);
      } else {
        LOGGER.info("Skipping LLC segments validation for table: {}, isTableEnabled: {}, isTablePaused: {}",
            realtimeTableName, isTableEnabled, isTablePaused);
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f), true);
  }

  /**
   * Updates ideal state after completion of a realtime segment
   */
  @VisibleForTesting
  IdealState updateIdealStateOnSegmentCompletion(String realtimeTableName, String committingSegmentName,
      String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    return HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
      assert idealState != null;
      // When segment completion begins, the zk metadata is updated, followed by ideal state.
      // We allow only {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms for a segment to
      // complete, after which the segment is eligible for repairs by the
      // {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager}
      // After updating metadata, if more than
      // {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms elapse and ideal state is still
      // not updated, the segment could have already been fixed by
      // {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager}
      // Therefore, we do not want to proceed with ideal state update if max segment completion time has exceeded
      if (isExceededMaxSegmentCompletionTime(realtimeTableName, committingSegmentName, getCurrentTimeMs())) {
        LOGGER.error("Exceeded max segment completion time. Skipping ideal state update for segment: {}",
            committingSegmentName);
        throw new HelixHelper.PermanentUpdaterException(
            "Exceeded max segment completion time for segment " + committingSegmentName);
      }
      updateInstanceStatesForNewConsumingSegment(idealState.getRecord().getMapFields(), committingSegmentName,
          isTablePaused(idealState) ? null : newSegmentName, segmentAssignment, instancePartitionsMap);
      return idealState;
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
  }

  public static boolean isTablePaused(IdealState idealState) {
    PauseState pauseState = extractTablePauseState(idealState);
    if (pauseState != null) {
      return pauseState.isPaused();
    }
    // backward compatibility
    // TODO : remove this handling after next release.
    //  Expectation is that all table IS are migrated to the newer representation.
    return Boolean.parseBoolean(idealState.getRecord().getSimpleField(IS_TABLE_PAUSED));
  }

  private static PauseState extractTablePauseState(IdealState idealState) {
    String pauseStateStr = idealState.getRecord().getSimpleField(PinotLLCRealtimeSegmentManager.PAUSE_STATE);
    try {
      if (pauseStateStr != null) {
        return JsonUtils.stringToObject(pauseStateStr, PauseState.class);
      }
    } catch (JsonProcessingException e) {
      LOGGER.warn("Unable to parse the pause state from ideal state : {}", pauseStateStr);
    }
    return null;
  }

  @VisibleForTesting
  void updateInstanceStatesForNewConsumingSegment(Map<String, Map<String, String>> instanceStatesMap,
      @Nullable String committingSegmentName, @Nullable String newSegmentName, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    if (committingSegmentName != null) {
      // Change committing segment state to ONLINE
      Set<String> instances = instanceStatesMap.get(committingSegmentName).keySet();
      instanceStatesMap.put(committingSegmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instances, SegmentStateModel.ONLINE));
      LOGGER.info("Updating segment: {} to ONLINE state", committingSegmentName);
    }

    // There used to be a race condition in pinot (caused by heavy GC on the controller during segment commit)
    // that ended up creating multiple consuming segments for the same stream partition, named somewhat like
    // tableName__1__25__20210920T190005Z and tableName__1__25__20210920T190007Z. It was fixed by checking the
    // Zookeeper Stat object before updating the segment metadata.
    // These conditions can happen again due to manual operations considered as fixes in Issues #5559 and #5263
    // The following check prevents the table from going into such a state (but does not prevent the root cause
    // of attempting such a zk update).
    if (newSegmentName != null) {
      LLCSegmentName newLLCSegmentName = new LLCSegmentName(newSegmentName);
      int partitionId = newLLCSegmentName.getPartitionGroupId();
      int seqNum = newLLCSegmentName.getSequenceNumber();
      for (String segmentNameStr : instanceStatesMap.keySet()) {
        LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentNameStr);
        if (llcSegmentName == null) {
          // skip the segment name if the name is not in low-level consumer format
          // such segment name can appear for uploaded segment
          LOGGER.debug("Skip segment name {} not in low-level consumer format", segmentNameStr);
          continue;
        }
        if (llcSegmentName.getPartitionGroupId() == partitionId && llcSegmentName.getSequenceNumber() == seqNum) {
          String errorMsg =
              String.format("Segment %s is a duplicate of existing segment %s", newSegmentName, segmentNameStr);
          LOGGER.error(errorMsg);
          throw new HelixHelper.PermanentUpdaterException(errorMsg);
        }
      }
      // Assign instances to the new segment and add instances as state CONSUMING
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(newSegmentName, instanceStatesMap, instancePartitionsMap);
      instanceStatesMap.put(newSegmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.CONSUMING));
      LOGGER.info("Adding new CONSUMING segment: {} to instances: {}", newSegmentName, instancesAssigned);
    }
  }

  /**
   * Handles segment movement between instances.
   * If the new consuming segment is served by a different set of servers than the committed segment, notify the
   * servers no longer serving the stream partition to remove the ingestion metrics. This can prevent servers from
   * emitting high ingestion delay alerts on stream partitions no longer served.
   */
  private void handleSegmentMovement(String realtimeTableName, Map<String, Map<String, String>> instanceStatesMap,
      String committedSegment, String newConsumingSegment) {
    Set<String> oldInstances = instanceStatesMap.get(committedSegment).keySet();
    Set<String> newInstances = instanceStatesMap.get(newConsumingSegment).keySet();
    if (newInstances.containsAll(oldInstances)) {
      return;
    }
    Set<String> instancesNoLongerServe = new HashSet<>(oldInstances);
    instancesNoLongerServe.removeAll(newInstances);
    LOGGER.info("Segment movement detected for committed segment: {} (served by: {}), "
            + "consuming segment: {} (served by: {}) in table: {}, "
            + "sending message to instances: {} to remove ingestion metrics", committedSegment, oldInstances,
        newConsumingSegment, newInstances, realtimeTableName, instancesNoLongerServe);

    ClusterMessagingService messagingService = _helixManager.getMessagingService();
    List<String> instancesSent = new ArrayList<>(instancesNoLongerServe.size());
    for (String instance : instancesNoLongerServe) {
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName(instance);
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResource(realtimeTableName);
      recipientCriteria.setPartition(committedSegment);
      recipientCriteria.setSessionSpecific(true);
      IngestionMetricsRemoveMessage message = new IngestionMetricsRemoveMessage();
      if (messagingService.send(recipientCriteria, message, null, -1) > 0) {
        instancesSent.add(instance);
      } else {
        LOGGER.warn("Failed to send ingestion metrics remove message for table: {} segment: {} to instance: {}",
            realtimeTableName, committedSegment, instance);
      }
    }
    LOGGER.info("Sent ingestion metrics remove message for table: {} segment: {} to instances: {}", realtimeTableName,
        committedSegment, instancesSent);
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
   * Returns true if more than {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms have
   * elapsed since segment metadata update
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
   * Iterates over latest metadata for each group and checks for following scenarios and repairs them:
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
  IdealState ensureAllPartitionsConsuming(TableConfig tableConfig, List<StreamConfig> streamConfigs,
        IdealState idealState, List<PartitionGroupMetadata> partitionGroupMetadataList, OffsetCriteria offsetCriteria) {
    String realtimeTableName = tableConfig.getTableName();

    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);
    int numPartitions = partitionGroupMetadataList.size();

    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, _controllerMetrics);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    Map<String, Map<String, String>> instanceStatesMap = idealState.getRecord().getMapFields();
    StreamPartitionMsgOffsetFactory offsetFactory =
        StreamConsumerFactoryProvider.create(streamConfigs.get(0)).createStreamMsgOffsetFactory();

    // Get the latest segment ZK metadata for each partition
    Map<Integer, SegmentZKMetadata> latestSegmentZKMetadataMap = getLatestSegmentZKMetadataMap(realtimeTableName);

    // Create a map from partition id to start offset
    // TODO: Directly return map from StreamMetadataProvider
    Map<Integer, StreamPartitionMsgOffset> partitionIdToStartOffset =
        Maps.newHashMapWithExpectedSize(numPartitions);
    for (PartitionGroupMetadata metadata : partitionGroupMetadataList) {
      partitionIdToStartOffset.put(metadata.getPartitionGroupId(), metadata.getStartOffset());
    }
    // Create a map from partition id to the smallest stream offset
    Map<Integer, StreamPartitionMsgOffset> partitionIdToSmallestOffset = null;
    if (offsetCriteria == OffsetCriteria.SMALLEST_OFFSET_CRITERIA) {
      partitionIdToSmallestOffset = partitionIdToStartOffset;
    }

    // Walk over all partitions that we have metadata for, and repair any partitions necessary.
    // Possible things to repair:
    // 1. The latest metadata is in DONE state, but the idealstate says segment is CONSUMING:
    //    a. Create metadata for next segment and find hosts to assign it to.
    //    b. update current segment in idealstate to ONLINE (only if partition is present in newPartitionGroupMetadata)
    //    c. add new segment in idealstate to CONSUMING on the hosts (only if partition is present in
    //    newPartitionGroupMetadata)
    // 2. The latest metadata is in DONE state, but the idealstate has no segment in CONSUMING state.
    //    a. Create metadata for new IN_PROGRESS segment with startOffset set to latest segments' end offset.
    //    b. Add the newly created segment to idealstate with segment state set to CONSUMING.
    // 3. The latest metadata is IN_PROGRESS, but segment is not there in idealstate.
    //    a. change prev segment to ONLINE in idealstate
    //    b. add latest segment to CONSUMING in idealstate.
    // 4. All instances of a segment are in OFFLINE state.
    //    a. Create a new segment (with the next seq number)
    //       and restart consumption from the same offset (if possible) or a newer offset (if realtime stream does
    //       not have the same offset).
    //       In latter case, report data loss.
    long currentTimeMs = getCurrentTimeMs();
    for (Map.Entry<Integer, SegmentZKMetadata> entry : latestSegmentZKMetadataMap.entrySet()) {
      int partitionId = entry.getKey();
      SegmentZKMetadata latestSegmentZKMetadata = entry.getValue();
      String latestSegmentName = latestSegmentZKMetadata.getSegmentName();
      LLCSegmentName latestLLCSegmentName = new LLCSegmentName(latestSegmentName);

      Map<String, String> instanceStateMap = instanceStatesMap.get(latestSegmentName);
      if (instanceStateMap != null) {
        // Latest segment of metadata is in idealstate.
        if (instanceStateMap.containsValue(SegmentStateModel.CONSUMING)) {
          if (latestSegmentZKMetadata.getStatus() == Status.DONE) {

            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done
            // either.
            if (!isExceededMaxSegmentCompletionTime(realtimeTableName, latestSegmentName, currentTimeMs)) {
              continue;
            }
            if (partitionIdToStartOffset.containsKey(partitionId)) {
              LOGGER.info("Repairing segment: {} which is DONE in segment ZK metadata, but is CONSUMING in IdealState",
                  latestSegmentName);

              LLCSegmentName newLLCSegmentName = getNextLLCSegmentName(latestLLCSegmentName, currentTimeMs);
              String newSegmentName = newLLCSegmentName.getSegmentName();
              CommittingSegmentDescriptor committingSegmentDescriptor =
                  new CommittingSegmentDescriptor(latestSegmentName,
                      (offsetFactory.create(latestSegmentZKMetadata.getEndOffset()).toString()), 0);
              createNewSegmentZKMetadata(tableConfig, streamConfigs.get(0), newLLCSegmentName, currentTimeMs,
                  committingSegmentDescriptor, latestSegmentZKMetadata, instancePartitions, numPartitions, numReplicas);
              updateInstanceStatesForNewConsumingSegment(instanceStatesMap, latestSegmentName, newSegmentName,
                  segmentAssignment, instancePartitionsMap);
            } else { // partition group reached end of life
              LOGGER.info("PartitionGroup: {} has reached end of life. Updating ideal state for segment: {}. "
                      + "Skipping creation of new ZK metadata and new segment in ideal state", partitionId,
                  latestSegmentName);
              updateInstanceStatesForNewConsumingSegment(instanceStatesMap, latestSegmentName, null, segmentAssignment,
                  instancePartitionsMap);
            }
          }
          // else, the metadata should be IN_PROGRESS, which is the right state for a consuming segment.
        } else {
          // No replica in CONSUMING state

          // Possible scenarios: for any of these scenarios, we need to create a new CONSUMING segment unless the stream
          // partition has reached end of life
          // 1. All replicas OFFLINE and metadata IN_PROGRESS/DONE - a segment marked itself OFFLINE during consumption
          //    for some reason
          // 2. All replicas ONLINE and metadata DONE/UPLOADED
          // 3. We should never end up with some replicas ONLINE and some OFFLINE
          boolean allInstancesOffline = isAllInstancesInState(instanceStateMap, SegmentStateModel.OFFLINE);
          boolean allInstancesOnlineAndMetadataCompleted =
              isAllInstancesInState(instanceStateMap, SegmentStateModel.ONLINE)
                  && latestSegmentZKMetadata.getStatus().isCompleted();
          if (!allInstancesOffline && !allInstancesOnlineAndMetadataCompleted) {
            LOGGER.error("Got unexpected instance state map: {} for segment: {} with status: {}", instanceStateMap,
                latestSegmentName, latestSegmentZKMetadata.getStatus());
            continue;
          }

          // Smallest offset is fetched from stream once and cached in partitionIdToSmallestOffset.
          if (partitionIdToSmallestOffset == null) {
            partitionIdToSmallestOffset = fetchPartitionGroupIdToSmallestOffset(streamConfigs);
          }

          // Do not create new CONSUMING segment when the stream partition has reached end of life.
          if (!partitionIdToSmallestOffset.containsKey(partitionId)) {
            continue;
          }

          if (allInstancesOffline) {
            LOGGER.info("Repairing segment: {} which is OFFLINE for all instances in IdealState", latestSegmentName);
            StreamPartitionMsgOffset startOffset =
                selectStartOffset(offsetCriteria, partitionId, partitionIdToStartOffset,
                    partitionIdToSmallestOffset, tableConfig.getTableName(), offsetFactory,
                    latestSegmentZKMetadata.getStartOffset()); // segments are OFFLINE; start from beginning
            createNewConsumingSegment(tableConfig, streamConfigs.get(0), latestSegmentZKMetadata, currentTimeMs,
                partitionGroupMetadataList, instancePartitions, instanceStatesMap, segmentAssignment,
                instancePartitionsMap, startOffset);
          } else {
            LOGGER.info("Resuming consumption for partition: {} of table: {}", partitionId, realtimeTableName);
            StreamPartitionMsgOffset startOffset =
                selectStartOffset(offsetCriteria, partitionId, partitionIdToStartOffset,
                    partitionIdToSmallestOffset, tableConfig.getTableName(), offsetFactory,
                    latestSegmentZKMetadata.getEndOffset());
            createNewConsumingSegment(tableConfig, streamConfigs.get(0), latestSegmentZKMetadata, currentTimeMs,
                partitionGroupMetadataList, instancePartitions, instanceStatesMap, segmentAssignment,
                instancePartitionsMap, startOffset);
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
            if (segmentEntry.getValue().containsValue(SegmentStateModel.CONSUMING)
                && new LLCSegmentName(segmentEntry.getKey()).getPartitionGroupId() == partitionId) {
              previousConsumingSegment = segmentEntry.getKey();
              break;
            }
          }
          if (previousConsumingSegment == null) {
            LOGGER.error(
                "Failed to find previous CONSUMING segment for partition: {} of table: {}, potential data loss",
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
    for (PartitionGroupMetadata partitionGroupMetadata : partitionGroupMetadataList) {
      int partitionId = partitionGroupMetadata.getPartitionGroupId();
      if (!latestSegmentZKMetadataMap.containsKey(partitionId)) {
        String newSegmentName =
            setupNewPartitionGroup(tableConfig, streamConfigs.get(0), partitionGroupMetadata, currentTimeMs,
                instancePartitions,
                numPartitions, numReplicas);
        updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, newSegmentName, segmentAssignment,
            instancePartitionsMap);
      }
    }

    return idealState;
  }

  private void createNewConsumingSegment(TableConfig tableConfig, StreamConfig streamConfig,
      SegmentZKMetadata latestSegmentZKMetadata, long currentTimeMs,
      List<PartitionGroupMetadata> newPartitionGroupMetadataList, InstancePartitions instancePartitions,
      Map<String, Map<String, String>> instanceStatesMap, SegmentAssignment segmentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, StreamPartitionMsgOffset startOffset) {
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);
    int numPartitions = newPartitionGroupMetadataList.size();
    LLCSegmentName latestLLCSegmentName = new LLCSegmentName(latestSegmentZKMetadata.getSegmentName());
    LLCSegmentName newLLCSegmentName = getNextLLCSegmentName(latestLLCSegmentName, currentTimeMs);
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(latestSegmentZKMetadata.getSegmentName(), startOffset.toString(), 0);
    createNewSegmentZKMetadata(tableConfig, streamConfig, newLLCSegmentName, currentTimeMs, committingSegmentDescriptor,
        latestSegmentZKMetadata, instancePartitions, numPartitions, numReplicas);
    String newSegmentName = newLLCSegmentName.getSegmentName();
    updateInstanceStatesForNewConsumingSegment(instanceStatesMap, null, newSegmentName, segmentAssignment,
        instancePartitionsMap);
  }

  private Map<Integer, StreamPartitionMsgOffset> fetchPartitionGroupIdToSmallestOffset(
      List<StreamConfig> streamConfigs) {
    Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToSmallestOffset = new HashMap<>();
    for (StreamConfig streamConfig : streamConfigs) {
      OffsetCriteria originalOffsetCriteria = streamConfig.getOffsetCriteria();
      streamConfig.setOffsetCriteria(OffsetCriteria.SMALLEST_OFFSET_CRITERIA);
      List<PartitionGroupMetadata> partitionGroupMetadataList =
          getNewPartitionGroupMetadataList(streamConfigs, Collections.emptyList());
      streamConfig.setOffsetCriteria(originalOffsetCriteria);
      for (PartitionGroupMetadata metadata : partitionGroupMetadataList) {
        partitionGroupIdToSmallestOffset.put(metadata.getPartitionGroupId(), metadata.getStartOffset());
      }
    }
    return partitionGroupIdToSmallestOffset;
  }

  private StreamPartitionMsgOffset selectStartOffset(OffsetCriteria offsetCriteria, int partitionGroupId,
      Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToStartOffset,
      Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToSmallestStreamOffset, String tableName,
      StreamPartitionMsgOffsetFactory offsetFactory, String startOffsetInSegmentZkMetadataStr) {
    if (offsetCriteria != null) {
      // use the fetched offset according to offset criteria
      return partitionGroupIdToStartOffset.get(partitionGroupId);
    } else {
      // use offset from segment ZK metadata
      StreamPartitionMsgOffset startOffsetInSegmentZkMetadata = offsetFactory.create(startOffsetInSegmentZkMetadataStr);
      StreamPartitionMsgOffset streamSmallestOffset = partitionGroupIdToSmallestStreamOffset.get(partitionGroupId);
      // Start offset in ZK must be higher than the start offset of the stream
      if (streamSmallestOffset.compareTo(startOffsetInSegmentZkMetadata) > 0) {
        LOGGER.error("Data lost from offset: {} to: {} for partition: {} of table: {}", startOffsetInSegmentZkMetadata,
            streamSmallestOffset, partitionGroupId, tableName);
        _controllerMetrics.addMeteredTableValue(tableName, ControllerMeter.LLC_STREAM_DATA_LOSS, 1L);
        return streamSmallestOffset;
      }
      return startOffsetInSegmentZkMetadata;
    }
  }

  private LLCSegmentName getNextLLCSegmentName(LLCSegmentName lastLLCSegmentName, long creationTimeMs) {
    return new LLCSegmentName(lastLLCSegmentName.getTableName(), lastLLCSegmentName.getPartitionGroupId(),
        lastLLCSegmentName.getSequenceNumber() + 1, creationTimeMs);
  }

  /**
   * Sets up a new partition group.
   * <p>Persists the ZK metadata for the first CONSUMING segment, and returns the segment name.
   */
  private String setupNewPartitionGroup(TableConfig tableConfig, StreamConfig streamConfig,
      PartitionGroupMetadata partitionGroupMetadata, long creationTimeMs, InstancePartitions instancePartitions,
      int numPartitions, int numReplicas) {
    String realtimeTableName = tableConfig.getTableName();
    int partitionGroupId = partitionGroupMetadata.getPartitionGroupId();
    String startOffset = partitionGroupMetadata.getStartOffset().toString();
    LOGGER.info("Setting up new partition group: {} for table: {}", partitionGroupId, realtimeTableName);

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    LLCSegmentName newLLCSegmentName =
        new LLCSegmentName(rawTableName, partitionGroupId, STARTING_SEQUENCE_NUMBER, creationTimeMs);
    String newSegmentName = newLLCSegmentName.getSegmentName();

    CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(null, startOffset, 0);
    createNewSegmentZKMetadata(tableConfig, streamConfig, newLLCSegmentName, creationTimeMs,
        committingSegmentDescriptor, null, instancePartitions, numPartitions, numReplicas);

    return newSegmentName;
  }

  @VisibleForTesting
  long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  private int getNumReplicas(TableConfig tableConfig, InstancePartitions instancePartitions) {
    if (instancePartitions.getNumReplicaGroups() == 1) {
      // Non-replica-group based
      return tableConfig.getReplication();
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

  /**
   * Fix the missing LLC segment in deep store by asking servers to upload, and add deep store download uri in ZK.
   * Since uploading to deep store involves expensive compression step (first tar up the segment and then upload),
   * we don't want to retry the uploading. Segment without deep store copy can still be downloaded from peer servers.
   *
   * @see <a href="
   * https://cwiki.apache.org/confluence/display/PINOT/By-passing+deep-store+requirement+for+Realtime+segment+completion
   * "> By-passing deep-store requirement for Realtime segment completion:Failure cases and handling</a>
   *
   * TODO: Add an on-demand way to upload LLC segment to deep store for a specific table.
   */
  public void uploadToDeepStoreIfMissing(TableConfig tableConfig, List<SegmentZKMetadata> segmentsZKMetadata) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    String realtimeTableName = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    RetentionStrategy retentionStrategy = new TimeRetentionStrategy(TimeUnit.MILLISECONDS,
        TimeUtils.VALID_MAX_TIME_MILLIS);

    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (validationConfig.getRetentionTimeUnit() != null && !validationConfig.getRetentionTimeUnit().isEmpty()
        && validationConfig.getRetentionTimeValue() != null && !validationConfig.getRetentionTimeValue().isEmpty()) {
      // Use this retention value to avoid the data racing between segment upload and retention management.
      long retentionMs = TimeUnit.valueOf(validationConfig.getRetentionTimeUnit().toUpperCase())
          .toMillis(Long.parseLong(validationConfig.getRetentionTimeValue()));
      retentionStrategy = new TimeRetentionStrategy(TimeUnit.MILLISECONDS,
          retentionMs - MIN_TIME_BEFORE_SEGMENT_EXPIRATION_FOR_FIXING_DEEP_STORE_COPY_MILLIS);
    }

    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_controllerConf.getDataDir()).getScheme());

    // Iterate through LLC segments and upload missing deep store copy by following steps:
    //  1. Ask servers which have online segment replica to upload to deep store.
    //     Servers return deep store download url after successful uploading.
    //  2. Update the LLC segment ZK metadata by adding deep store download url.
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      String segmentName = segmentZKMetadata.getSegmentName();
      try {
        // Only fix the committed (DONE) LLC segment without deep store copy (empty download URL)
        if (segmentZKMetadata.getStatus() != Status.DONE
            || !CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD.equals(segmentZKMetadata.getDownloadUrl())) {
          continue;
        }
        // Skip the fix for the segment if it is already out of retention.
        if (retentionStrategy.isPurgeable(realtimeTableName, segmentZKMetadata)) {
          LOGGER.info("Skipped deep store uploading of LLC segment {} which is already out of retention",
              segmentName);
          continue;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed checking segment deep store URL for segment {}", segmentName);
      }

      // Skip the fix if an upload is already queued for this segment
      if (!_deepStoreUploadExecutorPendingSegments.add(segmentName)) {
        int queueSize = _deepStoreUploadExecutorPendingSegments.size();
        _controllerMetrics.setOrUpdateGauge(
            ControllerGauge.LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_QUEUE_SIZE.getGaugeName(), queueSize);
        continue;
      }

      // create Runnable to perform the upload
      Runnable uploadRunnable = () -> {
        try {
          LOGGER.info("Fixing LLC segment {} whose deep store copy is unavailable", segmentName);
          // Find servers which have online replica

          String peerSegmentDownloadScheme = validationConfig.getPeerSegmentDownloadScheme();
          if (peerSegmentDownloadScheme == null) {
            peerSegmentDownloadScheme = CommonConstants.HTTP_PROTOCOL;
          }

          List<URI> peerSegmentURIs =
              PeerServerSegmentFinder.getPeerServerURIs(_helixManager, realtimeTableName, segmentName,
                  peerSegmentDownloadScheme);
          if (peerSegmentURIs.isEmpty()) {
            throw new IllegalStateException(
                String.format("Failed to upload segment %s to deep store because no online replica is found",
                    segmentName));
          }

          // Randomly ask one server to upload
          URI uri = peerSegmentURIs.get(RANDOM.nextInt(peerSegmentURIs.size()));
          try {
            String serverUploadRequestUrl = StringUtil.join("/", uri.toString(), "uploadLLCSegment");
            serverUploadRequestUrl =
                String.format("%s?uploadTimeoutMs=%d", serverUploadRequestUrl, _deepstoreUploadRetryTimeoutMs);
            LOGGER.info("Ask server to upload LLC segment {} to deep store by this path: {}", segmentName,
                serverUploadRequestUrl);
            TableLLCSegmentUploadResponse tableLLCSegmentUploadResponse
                = _fileUploadDownloadClient.uploadLLCToSegmentStore(serverUploadRequestUrl);
            String segmentDownloadUrl =
                moveSegmentFile(rawTableName, segmentName, tableLLCSegmentUploadResponse.getDownloadUrl(), pinotFS);
            LOGGER.info("Updating segment {} download url in ZK to be {}", segmentName, segmentDownloadUrl);
            // Update segment ZK metadata by adding the download URL
            segmentZKMetadata.setDownloadUrl(segmentDownloadUrl);
            // Update ZK crc to that of the server segment crc if unmatched
            if (tableLLCSegmentUploadResponse.getCrc() != segmentZKMetadata.getCrc()) {
              LOGGER.info("Updating segment {} crc in ZK to be {} from previous {}", segmentName,
                  tableLLCSegmentUploadResponse.getCrc(), segmentZKMetadata.getCrc());
              segmentZKMetadata.setCrc(tableLLCSegmentUploadResponse.getCrc());
            }
          } catch (Exception e) {
            // this is a fallback call for backward compatibility to the original API /upload in pinot-server
            // should be deprecated in the long run
            String serverUploadRequestUrl = StringUtil.join("/", uri.toString(), "upload");
            serverUploadRequestUrl =
                String.format("%s?uploadTimeoutMs=%d", serverUploadRequestUrl, _deepstoreUploadRetryTimeoutMs);
            LOGGER.info("Ask server to upload LLC segment {} to deep store by this path: {}", segmentName,
                serverUploadRequestUrl);
            String tempSegmentDownloadUrl = _fileUploadDownloadClient.uploadToSegmentStore(serverUploadRequestUrl);
            String segmentDownloadUrl = moveSegmentFile(rawTableName, segmentName, tempSegmentDownloadUrl, pinotFS);
            LOGGER.info("Updating segment {} download url in ZK to be {}", segmentName, segmentDownloadUrl);
            // Update segment ZK metadata by adding the download URL
            segmentZKMetadata.setDownloadUrl(segmentDownloadUrl);
          }
          // TODO: add version check when persist segment ZK metadata
          persistSegmentZKMetadata(realtimeTableName, segmentZKMetadata, -1);
          LOGGER.info("Successfully uploaded LLC segment {} to deep store with download url: {}", segmentName,
              segmentZKMetadata.getDownloadUrl());
          _controllerMetrics.addMeteredTableValue(realtimeTableName,
              ControllerMeter.LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_SUCCESS, 1L);
        } catch (Exception e) {
          _controllerMetrics.addMeteredTableValue(realtimeTableName,
              ControllerMeter.LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_ERROR, 1L);
          LOGGER.error("Failed to upload segment {} to deep store", segmentName, e);
        } finally {
          _deepStoreUploadExecutorPendingSegments.remove(segmentName);
          // Monitoring in case segment upload retry is lagging
          int queueSize = _deepStoreUploadExecutorPendingSegments.size();
          _controllerMetrics.setOrUpdateGauge(
              ControllerGauge.LLC_SEGMENTS_DEEP_STORE_UPLOAD_RETRY_QUEUE_SIZE.getGaugeName(), queueSize);
        }
      };

      // Submit the runnable to execute asynchronously
      _deepStoreUploadExecutor.submit(uploadRunnable);
    }
  }

  @VisibleForTesting
  boolean deepStoreUploadExecutorPendingSegmentsIsEmpty() {
    return _deepStoreUploadExecutorPendingSegments.isEmpty();
  }

  /**
   * Delete tmp segments for realtime table with low level consumer, split commit and async deletion is enabled.
   * @return number of deleted orphan temporary segments
   */
  public int deleteTmpSegments(String realtimeTableName, List<SegmentZKMetadata> segmentsZKMetadata)
      throws IOException {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    // NOTE: Do not delete the file if it is used as download URL. This could happen when user uses temporary file to
    // backfill segment.
    Set<String> downloadUrls = Sets.newHashSetWithExpectedSize(segmentsZKMetadata.size());
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (segmentZKMetadata.getStatus() == Status.DONE) {
        downloadUrls.add(segmentZKMetadata.getDownloadUrl());
      }
    }

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    URI tableDirURI = URIUtils.getUri(_controllerConf.getDataDir(), rawTableName);
    PinotFS pinotFS = PinotFSFactory.create(tableDirURI.getScheme());
    int numDeletedTmpSegments = 0;
    for (String filePath : pinotFS.listFiles(tableDirURI, false)) {
      if (isTmpAndCanDelete(filePath, downloadUrls, pinotFS)) {
        URI uri = URIUtils.getUri(filePath);
        String canonicalPath = uri.toString();
        LOGGER.info("Deleting temporary segment file: {}", canonicalPath);
        try {
          if (pinotFS.delete(uri, true)) {
            LOGGER.info("Deleted temporary segment file: {}", canonicalPath);
            numDeletedTmpSegments++;
          } else {
            LOGGER.warn("Failed to delete temporary segment file: {}", canonicalPath);
          }
        } catch (Exception e) {
          LOGGER.error("Caught exception while deleting temporary segment file: {}", canonicalPath, e);
        }
      }
    }
    return numDeletedTmpSegments;
  }

  private boolean isTmpAndCanDelete(String filePath, Set<String> downloadUrls, PinotFS pinotFS) {
    if (!SegmentCompletionUtils.isTmpFile(filePath)) {
      return false;
    }
    // Prepend scheme
    URI uri = URIUtils.getUri(filePath);
    String canonicalPath = uri.toString();
    // NOTE: Do not delete the file if it is used as download URL. This could happen when user uses temporary file to
    // backfill segment.
    if (downloadUrls.contains(canonicalPath)) {
      return false;
    }
    long lastModified;
    try {
      lastModified = pinotFS.lastModified(uri);
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting last modified time for file: {}, ineligible for delete",
          canonicalPath, e);
      return false;
    }
    if (lastModified <= 0) {
      LOGGER.warn("Last modified time for file: {} is not positive: {}, ineligible for delete", canonicalPath,
          lastModified);
      return false;
    }
    return getCurrentTimeMs() - lastModified > _controllerConf.getTmpSegmentRetentionInSeconds() * 1000L;
  }

  /**
   * Force commit the current segments in consuming state and restart consumption
   * Commit all partitions unless either partitionsToCommit or segmentsToCommit are provided.
   *
   * @param tableNameWithType  table name with type
   * @param partitionGroupIdsToCommit  comma separated list of partition group IDs to commit
   * @param segmentsToCommit  comma separated list of consuming segments to commit
   * @return the set of consuming segments for which commit was initiated
   */
  public Set<String> forceCommit(String tableNameWithType, @Nullable String partitionGroupIdsToCommit,
      @Nullable String segmentsToCommit) {
    IdealState idealState = getIdealState(tableNameWithType);
    Set<String> allConsumingSegments = findConsumingSegments(idealState);
    Set<String> targetConsumingSegments = filterSegmentsToCommit(allConsumingSegments, partitionGroupIdsToCommit,
        segmentsToCommit);
    sendForceCommitMessageToServers(tableNameWithType, targetConsumingSegments);
    return targetConsumingSegments;
  }

  /**
   * Among all consuming segments, filter the ones that are in the given partitions or segments.
   */
  private Set<String> filterSegmentsToCommit(Set<String> allConsumingSegments,
      @Nullable String partitionGroupIdsToCommitStr, @Nullable String segmentsToCommitStr) {
    if (partitionGroupIdsToCommitStr == null && segmentsToCommitStr == null) {
      return allConsumingSegments;
    }

    if (segmentsToCommitStr != null) {
      Set<String> segmentsToCommit = Arrays.stream(segmentsToCommitStr.split(","))
          .map(String::trim)
          .collect(Collectors.toSet());
      Preconditions.checkState(allConsumingSegments.containsAll(segmentsToCommit),
          "Cannot commit segments that are not in CONSUMING state. "
              + "All consuming segments: %s, provided segments to commit: %s", allConsumingSegments,
          segmentsToCommitStr);
      return segmentsToCommit;
    }

    // partitionGroupIdsToCommitStr != null
    Set<Integer> partitionsToCommit = Arrays.stream(partitionGroupIdsToCommitStr.split(","))
        .map(String::trim)
        .map(Integer::parseInt)
        .collect(Collectors.toSet());
    Set<String> targetSegments = allConsumingSegments.stream()
        .filter(segmentName -> partitionsToCommit.contains(new LLCSegmentName(segmentName).getPartitionGroupId()))
        .collect(Collectors.toSet());
    Preconditions.checkState(!targetSegments.isEmpty(), "Cannot find segments to commit for partitions: %s",
        partitionGroupIdsToCommitStr);
    return targetSegments;
  }

  /**
   * Pause consumption on a table by
   *   1) update PauseState in the table ideal state and
   *   2) sending force commit messages to servers
   */
  public PauseStatusDetails pauseConsumption(String tableNameWithType, PauseState.ReasonCode reasonCode,
      @Nullable String comment) {
    IdealState updatedIdealState = updatePauseStateInIdealState(tableNameWithType, true, reasonCode, comment);
    Set<String> consumingSegments = findConsumingSegments(updatedIdealState);
    sendForceCommitMessageToServers(tableNameWithType, consumingSegments);
    return new PauseStatusDetails(true, consumingSegments, reasonCode, comment != null ? comment
        : "Pause flag is set. Consuming segments are being committed."
            + " Use /pauseStatus endpoint in a few moments to check if all consuming segments have been committed.",
        new Timestamp(System.currentTimeMillis()).toString());
  }

  /**
   * Resume consumption on a table by
   *   1) update PauseState by clearing all pause reasons in the table ideal state and
   *   2) triggering segment validation job to create new consuming segments in ideal states
   */
  public PauseStatusDetails resumeConsumption(String tableNameWithType, @Nullable String offsetCriteria,
      PauseState.ReasonCode reasonCode, @Nullable String comment) {
    IdealState updatedIdealState = updatePauseStateInIdealState(tableNameWithType, false, reasonCode, comment);

    // trigger realtime segment validation job to resume consumption
    Map<String, String> taskProperties = new HashMap<>();
    if (offsetCriteria != null) {
      taskProperties.put(RealtimeSegmentValidationManager.OFFSET_CRITERIA, offsetCriteria);
    }
    _helixResourceManager
        .invokeControllerPeriodicTask(tableNameWithType, Constants.REALTIME_SEGMENT_VALIDATION_MANAGER, taskProperties);

    return new PauseStatusDetails(false, findConsumingSegments(updatedIdealState), reasonCode,
        comment != null ? comment : "Pause flag is cleared. Consuming segments are being created. Use /pauseStatus "
            + "endpoint in a few moments to double check.", new Timestamp(System.currentTimeMillis()).toString());
  }

  public IdealState updatePauseStateInIdealState(String tableNameWithType, boolean pause,
      PauseState.ReasonCode reasonCode, @Nullable String comment) {
    PauseState pauseState = new PauseState(pause, reasonCode, comment,
        new Timestamp(System.currentTimeMillis()).toString());
    IdealState updatedIdealState = HelixHelper.updateIdealState(_helixManager, tableNameWithType, idealState -> {
      ZNRecord znRecord = idealState.getRecord();
      znRecord.setSimpleField(PAUSE_STATE, pauseState.toJsonString());
      // maintain for backward compatibility
      znRecord.setSimpleField(IS_TABLE_PAUSED, Boolean.valueOf(pause).toString());
      return new IdealState(znRecord);
    }, RetryPolicies.noDelayRetryPolicy(3));
    LOGGER.info("Set 'pauseState' to {} in the Ideal State for table {}. "
        + "Also set 'isTablePaused' to {} for backward compatibility.", pauseState, tableNameWithType, pause);
    return updatedIdealState;
  }

  private void sendForceCommitMessageToServers(String tableNameWithType, Set<String> consumingSegments) {
    if (!consumingSegments.isEmpty()) {
      Criteria recipientCriteria = new Criteria();
      recipientCriteria.setInstanceName("%");
      recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      recipientCriteria.setResource(tableNameWithType);
      recipientCriteria.setSessionSpecific(true);
      ForceCommitMessage message = new ForceCommitMessage(tableNameWithType, consumingSegments);
      int numMessagesSent = _helixManager.getMessagingService().send(recipientCriteria, message, null, -1);
      if (numMessagesSent > 0) {
        LOGGER.info("Sent {} force commit messages for table: {} segments: {}", numMessagesSent, tableNameWithType,
            consumingSegments);
      } else {
        throw new RuntimeException(String
            .format("No force commit message was sent for table: %s segments: %s", tableNameWithType,
                consumingSegments));
      }
    }
  }

  private Set<String> findConsumingSegments(IdealState idealState) {
    Set<String> consumingSegments = new TreeSet<>();
    idealState.getRecord().getMapFields().forEach((segmentName, instanceToStateMap) -> {
      for (String state : instanceToStateMap.values()) {
        if (state.equals(SegmentStateModel.CONSUMING)) {
          consumingSegments.add(segmentName);
          break;
        }
      }
    });
    return consumingSegments;
  }

  /**
   * Return pause status:
   *   - Information from the 'pauseState' in the table ideal state
   *   - list of consuming segments
   */
  public PauseStatusDetails getPauseStatusDetails(String tableNameWithType) {
    IdealState idealState = getIdealState(tableNameWithType);
    Set<String> consumingSegments = findConsumingSegments(idealState);
    PauseState pauseState = extractTablePauseState(idealState);
    if (pauseState != null) {
      return new PauseStatusDetails(pauseState.isPaused(), consumingSegments, pauseState.getReasonCode(),
          pauseState.getComment(), pauseState.getTimeInMillis());
    }
    String isTablePausedStr = idealState.getRecord().getSimpleField(IS_TABLE_PAUSED);
    return new PauseStatusDetails(Boolean.parseBoolean(isTablePausedStr), consumingSegments,
        PauseState.ReasonCode.ADMINISTRATIVE, null, "");
  }

  @VisibleForTesting
  String moveSegmentFile(String rawTableName, String segmentName, String segmentLocation, PinotFS pinotFS)
      throws IOException {
    URI segmentFileURI = URIUtils.getUri(segmentLocation);
    URI uriToMoveTo = createSegmentPath(rawTableName, segmentName);
    Preconditions.checkState(pinotFS.move(segmentFileURI, uriToMoveTo, true),
        "Failed to move segment file for segment: %s from: %s to: %s", segmentName, segmentLocation, uriToMoveTo);
    return uriToMoveTo.toString();
  }

  @VisibleForTesting
  URI createSegmentPath(String rawTableName, String segmentName) {
    return URIUtils.getUri(_controllerConf.getDataDir(), rawTableName, URIUtils.encode(segmentName));
  }
}
