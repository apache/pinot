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
import com.google.common.collect.BiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.messages.ForceCommitMessage;
import org.apache.pinot.common.messages.IngestionMetricsRemoveMessage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.restlet.resources.TableLLCSegmentUploadResponse;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.ForceCommitBatchConfig;
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
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
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
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptFailureException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
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

  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f);
  public static final String COMMITTING_SEGMENTS = "committingSegments";
  private static final int STARTING_SEQUENCE_NUMBER = 0; // Initial sequence number for new table segments
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";

  // Max time to wait for all LLC segments to complete committing their metadata while stopping the controller.
  private static final long MAX_LLC_SEGMENT_METADATA_COMMIT_TIME_MILLIS = 30_000L;

  // TODO: make this configurable with default set to 10
  /**
   * After step 1 of segment completion is done,
   * this is the max time until which step 3 is allowed to complete.
   * See {@link #commitSegmentMetadataInternal(String, CommittingSegmentDescriptor, boolean)}
   * for explanation of steps 1 2 3
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
  private static final String REINGEST_SEGMENT_PATH = "/reingestSegment";

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

    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(tableConfig);
    streamConfigs.forEach(_flushThresholdUpdateManager::clearFlushThresholdUpdater);
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
              instancePartitions, numPartitionGroups, numReplicas);
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
  protected SegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName, @Nullable Stat stat) {
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

  private boolean addSegmentToCommittingSegmentsList(String realtimeTableName, String segmentName) {
    String committingSegmentsListPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);
    Stat stat = new Stat();
    ZNRecord znRecord = _propertyStore.get(committingSegmentsListPath, stat, AccessOption.PERSISTENT);
    int expectedVersion = stat.getVersion();
    LOGGER.info("Committing segments list size: {} before adding the segment: {}", Optional.ofNullable(znRecord)
        .map(record -> record.getListField(COMMITTING_SEGMENTS))
        .map(List::size)
        .orElse(0), segmentName);

    // empty ZN record for the table
    if (znRecord == null) {
      znRecord = new ZNRecord(realtimeTableName);
      znRecord.setListField(COMMITTING_SEGMENTS, List.of(segmentName));
      return _propertyStore.create(committingSegmentsListPath, znRecord, AccessOption.PERSISTENT);
    }

    // segment already present in the list
    List<String> committingSegmentList = znRecord.getListField(COMMITTING_SEGMENTS);
    if (committingSegmentList != null && committingSegmentList.contains(segmentName)) {
      return true;
    }

    if (committingSegmentList == null) {
      committingSegmentList = List.of(segmentName);
    } else {
      committingSegmentList.add(segmentName);
    }
    znRecord.setListField(COMMITTING_SEGMENTS, committingSegmentList);
    try {
      return _propertyStore.set(committingSegmentsListPath, znRecord, expectedVersion, AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
    }
  }

  private boolean removeSegmentFromCommittingSegmentsList(String realtimeTableName, String segmentName) {
    String committingSegmentsListPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);
    Stat stat = new Stat();
    ZNRecord znRecord = _propertyStore.get(committingSegmentsListPath, stat, AccessOption.PERSISTENT);

    LOGGER.info("Committing segments list size: {} before removing the segment: {}", Optional.ofNullable(znRecord)
        .map(record -> record.getListField(COMMITTING_SEGMENTS))
        .map(List::size)
        .orElse(0), segmentName);

    if (znRecord == null || znRecord.getListField(COMMITTING_SEGMENTS) == null || !znRecord.getListField(
        COMMITTING_SEGMENTS).contains(segmentName)) {
      return true;
    }
    znRecord.getListField(COMMITTING_SEGMENTS).remove(segmentName);
    try {
      return _propertyStore.set(committingSegmentsListPath, znRecord, stat.getVersion(), AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
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
      // Validate segment location only for metadata commit
      if (StringUtils.isBlank(committingSegmentDescriptor.getSegmentLocation())) {
        LOGGER.warn("Committing segment: {} was not uploaded to deep store",
            committingSegmentDescriptor.getSegmentName());
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.SEGMENT_MISSING_DEEP_STORE_LINK, 1);
      }
      commitSegmentMetadataInternal(realtimeTableName, committingSegmentDescriptor, false);
    } finally {
      _numCompletingSegments.addAndGet(-1);
    }
  }

  private void commitSegmentMetadataInternal(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor, boolean toCommitting) {
    String committingSegmentName = committingSegmentDescriptor.getSegmentName();
    TableConfig tableConfig = getTableConfig(realtimeTableName);
    InstancePartitions instancePartitions = getConsumingInstancePartitions(tableConfig);
    IdealState idealState = getIdealState(realtimeTableName);
    Preconditions.checkState(
        idealState.getInstanceStateMap(committingSegmentName).containsValue(SegmentStateModel.CONSUMING),
        "Failed to find instance in CONSUMING state in IdealState for segment: %s", committingSegmentName);

    /*
     * Update zookeeper in 3 steps.
     *
     * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE/COMMITTING
     * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
     * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
     */

    // Step-1: Update PROPERTYSTORE
    LOGGER.info("Committing segment metadata for segment: {}", committingSegmentName);
    long startTimeNs1 = System.nanoTime();
    SegmentZKMetadata committingSegmentZKMetadata =
        toCommitting ? updateSegmentZKMetadataToCommitting(realtimeTableName, committingSegmentDescriptor)
            : updateSegmentZKMetadataToDone(realtimeTableName, committingSegmentDescriptor, Status.IN_PROGRESS);

    preProcessNewSegmentZKMetadata();

    // Step-2: Create new segment metadata if needed
    long startTimeNs2 = System.nanoTime();
    String newConsumingSegmentName =
        createNewSegmentMetadata(tableConfig, idealState, committingSegmentDescriptor, committingSegmentZKMetadata,
            instancePartitions);

    preProcessCommitIdealStateUpdate();

    // Step-3: Update IdealState
    LOGGER.info("Updating Idealstate for previous: {} and new segment: {}", committingSegmentName,
        newConsumingSegmentName);
    long startTimeNs3 = System.nanoTime();

    // When multiple segments of the same table complete around the same time it is possible that
    // the idealstate update fails due to contention. We serialize the updates to the idealstate
    // to reduce this contention. We may still contend with RetentionManager, or other updates
    // to idealstate from other controllers, but then we have the retry mechanism to get around that.
    idealState =
        updateIdealStateForSegments(tableConfig, committingSegmentName, newConsumingSegmentName, instancePartitions);

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

  @VisibleForTesting
  protected void preProcessNewSegmentZKMetadata() {
    // No-op
  }

  @VisibleForTesting
  protected void preProcessCommitIdealStateUpdate() {
    // No-op
  }

  @VisibleForTesting
  protected void preProcessCommitSegmentEndMetadata() {
    // No-op
  }

  private boolean updateCommittingSegmentsList(String realtimeTableName, Callable<Boolean> operation) {
    try {
      DEFAULT_RETRY_POLICY.attempt(operation);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1L);
      LOGGER.error("Failed to update committing segments list for table: {}", realtimeTableName, e);
      return false;
    }
    return true;
  }

  // Step 1: Update committing segment ZK metadata

  /// When invoked from non-pauseless table, expected status is IN_PROGRESS; when invoked from pauseless table, expected
  /// status is COMMITTING.
  private SegmentZKMetadata updateSegmentZKMetadataToDone(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor, Status expectedStatus) {
    String segmentName = committingSegmentDescriptor.getSegmentName();
    Stat stat = new Stat();
    SegmentZKMetadata segmentZKMetadata = getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    Preconditions.checkState(segmentZKMetadata.getStatus() == expectedStatus,
        "Segment status for segment: %s should be %s, found: %s", segmentName, expectedStatus,
        segmentZKMetadata.getStatus());

    // Update segment ZK metadata per committing descriptor
    SegmentMetadataImpl segmentMetadata = committingSegmentDescriptor.getSegmentMetadata();
    Preconditions.checkState(segmentMetadata != null, "Failed to find segment metadata from descriptor for segment: %s",
        segmentName);
    String segmentLocation = committingSegmentDescriptor.getSegmentLocation();
    String downloadUrl =
        isPeerURL(segmentLocation) ? CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD : segmentLocation;
    SegmentZKMetadataUtils.updateCommittingSegmentZKMetadata(realtimeTableName, segmentZKMetadata, segmentMetadata,
        downloadUrl, committingSegmentDescriptor.getSegmentSizeBytes(), committingSegmentDescriptor.getNextOffset());
    persistSegmentZKMetadata(realtimeTableName, segmentZKMetadata, stat.getVersion());

    // Refresh the Broker routing
    _helixResourceManager.sendSegmentRefreshMessage(realtimeTableName, segmentName, false, true);

    return segmentZKMetadata;
  }

  /// For pauseless consumption only, the status should be IN_PROGRESS.
  private SegmentZKMetadata updateSegmentZKMetadataToCommitting(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String segmentName = committingSegmentDescriptor.getSegmentName();
    Stat stat = new Stat();
    SegmentZKMetadata segmentZKMetadata = getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    Preconditions.checkState(segmentZKMetadata.getStatus() == Status.IN_PROGRESS,
        "Segment status for segment: %s should be IN_PROGRESS, found: %s", segmentName, segmentZKMetadata.getStatus());

    segmentZKMetadata.setStatus(Status.COMMITTING);
    segmentZKMetadata.setEndOffset(committingSegmentDescriptor.getNextOffset());
    persistSegmentZKMetadata(realtimeTableName, segmentZKMetadata, stat.getVersion());

    return segmentZKMetadata;
  }

  // Step 2: Create new segment metadata
  @Nullable
  private String createNewSegmentMetadata(TableConfig tableConfig, IdealState idealState,
      CommittingSegmentDescriptor committingSegmentDescriptor, SegmentZKMetadata committingSegmentZKMetadata,
      InstancePartitions instancePartitions) {
    String committingSegmentName = committingSegmentDescriptor.getSegmentName();

    String realtimeTableName = tableConfig.getTableName();
    int numReplicas = getNumReplicas(tableConfig, instancePartitions);

    String newConsumingSegmentName = null;
    if (!isTablePaused(idealState)) {
      LLCSegmentName committingLLCSegment = new LLCSegmentName(committingSegmentName);
      int committingSegmentPartitionGroupId = committingLLCSegment.getPartitionGroupId();

      List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(tableConfig);
      Set<Integer> partitionIds = getPartitionIds(streamConfigs, idealState);

      if (partitionIds.contains(committingSegmentPartitionGroupId)) {
        String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
        long newSegmentCreationTimeMs = getCurrentTimeMs();
        LLCSegmentName newLLCSegment = new LLCSegmentName(rawTableName, committingSegmentPartitionGroupId,
            committingLLCSegment.getSequenceNumber() + 1, newSegmentCreationTimeMs);
        // TODO: This code does not support size-based segment thresholds for tables with pauseless enabled. The
        //  calculation of row thresholds based on segment size depends on the size of the previously committed
        //  segment. For tables with pauseless mode enabled, this size is unavailable at this step because the
        //  segment has not yet been built.

        createNewSegmentZKMetadata(tableConfig, streamConfigs.get(0), newLLCSegment, newSegmentCreationTimeMs,
            committingSegmentDescriptor, committingSegmentZKMetadata, instancePartitions, partitionIds.size(),
            numReplicas);
        newConsumingSegmentName = newLLCSegment.getSegmentName();
        LOGGER.info("Created new segment metadata for segment: {} with status: {}.", newConsumingSegmentName,
            Status.IN_PROGRESS);
      }
    } else {
      LOGGER.info("Skipped creation of new segment metadata as the table: {} is paused", realtimeTableName);
    }
    return newConsumingSegmentName;
  }

  // Step 3: Update IdealState
  private IdealState updateIdealStateForSegments(TableConfig tableConfig, String committingSegmentName,
      String newConsumingSegmentName, InstancePartitions instancePartitions) {

    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, _controllerMetrics);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);

    return updateIdealStateOnSegmentCompletion(tableConfig.getTableName(), committingSegmentName,
        newConsumingSegmentName, segmentAssignment, instancePartitionsMap);
  }

  /**
   * For pauseless ingestion, invoked after the realtime segment has been ingested but before the response is sent to
   * the server to build the segment.
   * <p>
   * This method performs the following actions:
   * 1. Adds the segment to the committing segment list.
   * 2. Updates the segment ZK metadata status from IN_PROGRESS to COMMITTING, sets the end offset.
   * 3. Creates a new ZK metadata for the next consuming segment.
   * 4. Updates the ideal state to mark the committing segment as ONLINE and new segment as CONSUMING.
   */
  public void commitSegmentMetadataToCommitting(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    LOGGER.info("commitSegmentStartMetadata: starting segment commit for table:{}, segment: {}", realtimeTableName,
        committingSegmentDescriptor.getSegmentName());
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    try {
      _numCompletingSegments.addAndGet(1);
      LOGGER.info("Adding segment: {} to committing segment list", committingSegmentDescriptor.getSegmentName());
      if (!updateCommittingSegmentsList(realtimeTableName,
          () -> addSegmentToCommittingSegmentsList(realtimeTableName, committingSegmentDescriptor.getSegmentName()))) {
        LOGGER.error("Failed to update committing segments list for table: {}, segment: {}", realtimeTableName,
            committingSegmentDescriptor.getSegmentName());
      }
      commitSegmentMetadataInternal(realtimeTableName, committingSegmentDescriptor, true);
    } finally {
      _numCompletingSegments.addAndGet(-1);
    }
  }

  /**
   * For pauseless ingestion, invoked after the realtime segment has been built and uploaded.
   * <p>
   * This method performs the following actions:
   * 1. Updates CRC, download URL, etc. in the segment ZK metadata.
   * 2. Removes the segment from the committing segment list.
   * 3. Updates the flush threshold updater.
   */
  public void commitSegmentMetadataToDone(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");

    preProcessCommitSegmentEndMetadata();

    _numCompletingSegments.addAndGet(1);
    try {
      // Validate segment location only for metadata commit
      String segmentName = committingSegmentDescriptor.getSegmentName();
      if (StringUtils.isBlank(committingSegmentDescriptor.getSegmentLocation())) {
        LOGGER.warn("Committing segment: {} was not uploaded to deep store", segmentName);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.SEGMENT_MISSING_DEEP_STORE_LINK, 1);
      }
      // When segment completion begins, the zk metadata and ideal state are updated.
      // This is followed by updating zk metadata for the committing segment with crc, size, download url etc.
      // during the commit end metadata call.
      // We allow only {@link PinotLLCRealtimeSegmentManager::MAX_SEGMENT_COMPLETION_TIME_MILLIS} ms for a segment to
      // complete, after which the segment is eligible for repairs by the
      // {@link org.apache.pinot.controller.validation.RealtimeSegmentValidationManager}
      if (isExceededMaxSegmentCompletionTime(realtimeTableName, segmentName, getCurrentTimeMs())) {
        LOGGER.error("Exceeded max segment completion time. Skipping ZK Metadata update for segment: {}", segmentName);
        throw new HelixHelper.PermanentUpdaterException(
            "Exceeded max segment completion time for segment " + segmentName);
      }
      LOGGER.info("Updating segment ZK metadata for segment: {}", segmentName);
      SegmentZKMetadata committingSegmentZKMetadata =
          updateSegmentZKMetadataToDone(realtimeTableName, committingSegmentDescriptor, Status.COMMITTING);
      LOGGER.info("Successfully updated segment metadata for segment: {}", segmentName);
      // remove the segment from the committing segment list
      LOGGER.info("Removing segment: {} from committing segment list", segmentName);
      if (!updateCommittingSegmentsList(realtimeTableName,
          () -> removeSegmentFromCommittingSegmentsList(realtimeTableName, segmentName))) {
        LOGGER.error("Failed to update committing segments list for table: {}, segment: {}", realtimeTableName,
            segmentName);
      }
      try {
        TableConfig tableConfig = getTableConfig(realtimeTableName);
        StreamConfig streamConfig = IngestionConfigUtils.getFirstStreamConfig(tableConfig);
        FlushThresholdUpdater flushThresholdUpdater =
            _flushThresholdUpdateManager.getFlushThresholdUpdater(streamConfig);
        flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
      } catch (Exception e) {
        LOGGER.error("Caught exception while updating flush threshold for table: {}, segment: {}", realtimeTableName,
            segmentName, e);
      }
    } finally {
      _numCompletingSegments.addAndGet(-1);
    }
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
    if (committingSegmentZKMetadata != null) {
      flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    }
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata,
        getMaxNumPartitionsPerInstance(instancePartitions, numPartitions, numReplicas));

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

  public long getCommitTimeoutMS(String realtimeTableName) {
    long commitTimeoutMS = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
    if (_propertyStore == null) {
      return commitTimeoutMS;
    }
    TableConfig tableConfig = getTableConfig(realtimeTableName);
    Map<String, String> streamConfigMap = IngestionConfigUtils.getFirstStreamConfigMap(tableConfig);
    if (streamConfigMap.containsKey(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS)) {
      String commitTimeoutSecondsStr = streamConfigMap.get(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS);
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
    String clientId = StreamConsumerFactory.getUniqueClientId(
        PinotLLCRealtimeSegmentManager.class.getSimpleName() + "-" + streamConfig.getTableNameWithType() + "-"
            + streamConfig.getTopicName());
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
      } catch (UnsupportedOperationException ignored) {
        allPartitionIdsFetched = false;
        // Stream does not support fetching partition ids. There is a log in the fallback code which is sufficient
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
      partitionIds.addAll(newPartitionGroupMetadataList.stream()
          .map(PartitionGroupMetadata::getPartitionGroupId)
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
        currentPartitionGroupConsumptionStatusList, false);
  }

  /**
   * Fetches the latest state of the PartitionGroups for the stream
   * If any partition has reached end of life, and all messages of that partition have been consumed by the segment,
   * it will be skipped from the result
   */
  @VisibleForTesting
  List<PartitionGroupMetadata> getNewPartitionGroupMetadataList(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList,
      boolean forceGetOffsetFromStream) {
    return PinotTableIdealStateBuilder.getPartitionGroupMetadataList(streamConfigs,
        currentPartitionGroupConsumptionStatusList, forceGetOffsetFromStream);
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
    LOGGER.info("Attempting to mark segment: {} OFFLINE on instance: {} if it is currently CONSUMING.", segmentName,
        instanceName);
    try {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, idealState -> {
        assert idealState != null;
        Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
        if (stateMap == null) {
          LOGGER.info("Skipping update for segment: {} state to state: {} in ideal state as instanceStateMap is null.",
              segmentName, SegmentStateModel.OFFLINE);
          return idealState;
        }
        String state = stateMap.get(instanceName);
        if (SegmentStateModel.CONSUMING.equals(state)) {
          LOGGER.info("Marking CONSUMING segment: {} OFFLINE on instance: {}", segmentName, instanceName);
          stateMap.put(instanceName, SegmentStateModel.OFFLINE);
        } else {
          LOGGER.info("Segment {} in state {} when trying to register consumption stop from {}", segmentName, state,
              instanceName);
        }
        return idealState;
      }, DEFAULT_RETRY_POLICY, true);
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
      _helixAdmin.resetPartition(_helixManager.getClusterName(), instanceName, realtimeTableName,
          Collections.singletonList(segmentName));
    } catch (Exception e) {
      // Ignore
    }
  }

  /**
   * An instance is reporting that it cannot build segment due to non-recoverable error, usually due to size too large.
   * Reduce the segment "segment.flush.threshold.size" to half of the current segment size target.
   */
  public void reduceSegmentSizeAndReset(LLCSegmentName llcSegmentName, int prevNumRows) {
    Preconditions.checkState(!_isStopping, "Segment manager is stopping");
    // reduce the segment size to its half
    String segmentName = llcSegmentName.getSegmentName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());

    Stat stat = new Stat();
    SegmentZKMetadata prevSegmentZKMetadata = getSegmentZKMetadata(realtimeTableName, segmentName, stat);
    Preconditions.checkState(prevSegmentZKMetadata.getStatus() == Status.IN_PROGRESS,
        "Segment status for segment: %s should be IN_PROGRESS, found: %s", segmentName,
        prevSegmentZKMetadata.getStatus());

    int prevTargetNumRows = prevSegmentZKMetadata.getSizeThresholdToFlushSegment();
    int newNumRows = Math.min(prevNumRows / 2, prevTargetNumRows / 2);
    prevSegmentZKMetadata.setSizeThresholdToFlushSegment(newNumRows);

    persistSegmentZKMetadata(realtimeTableName, prevSegmentZKMetadata, stat.getVersion());
    _helixResourceManager.resetSegment(
        realtimeTableName, segmentName, null);
    LOGGER.info("Reduced segment size of {} from prevTarget {} prevActual {} to {}",
        segmentName, prevTargetNumRows, prevNumRows, newNumRows);
    _controllerMetrics.addMeteredTableValue(
        realtimeTableName, ControllerMeter.SEGMENT_SIZE_AUTO_REDUCTION, 1L);
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
   * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE/COMMITTING
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
        streamConfigs.stream()
            .forEach(streamConfig -> streamConfig.setOffsetCriteria(
                offsetsHaveToChange ? offsetCriteria : OffsetCriteria.SMALLEST_OFFSET_CRITERIA));
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
    }, DEFAULT_RETRY_POLICY, true);
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
    }, DEFAULT_RETRY_POLICY);
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
  protected boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName,
      long currentTimeMs) {
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
    Map<Integer, StreamPartitionMsgOffset> partitionIdToStartOffset = Maps.newHashMapWithExpectedSize(numPartitions);
    for (PartitionGroupMetadata metadata : partitionGroupMetadataList) {
      partitionIdToStartOffset.put(metadata.getPartitionGroupId(), metadata.getStartOffset());
    }
    // Create a map from partition id to the smallest stream offset
    Map<Integer, StreamPartitionMsgOffset> partitionIdToSmallestOffset = null;
    if (offsetCriteria != null && offsetCriteria.equals(OffsetCriteria.SMALLEST_OFFSET_CRITERIA)) {
      partitionIdToSmallestOffset = partitionIdToStartOffset;
    }

    // Walk over all partitions that we have metadata for, and repair any partitions necessary.
    // Possible things to repair:
    // 1. The latest metadata is in DONE/COMMITTING state, but the idealstate says segment is CONSUMING:
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

    // This is the expected segment status after completion of first of the 3 steps of the segment commit protocol
    // The status in step one is updated to
    // 1. DONE for normal consumption
    // 2. COMMITTING for pauseless consumption
    Status statusPostSegmentMetadataUpdate =
        PauselessConsumptionUtils.isPauselessEnabled(tableConfig) ? Status.COMMITTING : Status.DONE;

    for (Map.Entry<Integer, SegmentZKMetadata> entry : latestSegmentZKMetadataMap.entrySet()) {
      int partitionId = entry.getKey();
      SegmentZKMetadata latestSegmentZKMetadata = entry.getValue();
      String latestSegmentName = latestSegmentZKMetadata.getSegmentName();
      LLCSegmentName latestLLCSegmentName = new LLCSegmentName(latestSegmentName);

      Map<String, String> instanceStateMap = instanceStatesMap.get(latestSegmentName);
      if (instanceStateMap != null) {
        // Latest segment of metadata is in idealstate.
        if (instanceStateMap.containsValue(SegmentStateModel.CONSUMING)) {
          if (latestSegmentZKMetadata.getStatus() == statusPostSegmentMetadataUpdate) {

            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE/COMMITTING)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done
            // either.
            if (!isExceededMaxSegmentCompletionTime(realtimeTableName, latestSegmentName, currentTimeMs)) {
              continue;
            }
            if (partitionIdToStartOffset.containsKey(partitionId)) {
              LOGGER.info("Repairing segment: {} which is {} in segment ZK metadata, but is CONSUMING in IdealState",
                  latestSegmentName, statusPostSegmentMetadataUpdate);

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
          // 2. All replicas ONLINE and metadata DONE/UPLOADED/COMMITTING
          // 3. We should never end up with some replicas ONLINE and some OFFLINE
          boolean allInstancesOffline = isAllInstancesInState(instanceStateMap, SegmentStateModel.OFFLINE);
          boolean allInstancesOnlineAndMetadataNotInProgress =
              isAllInstancesInState(instanceStateMap, SegmentStateModel.ONLINE) && (latestSegmentZKMetadata.getStatus()
                  != Status.IN_PROGRESS);
          if (!allInstancesOffline && !allInstancesOnlineAndMetadataNotInProgress) {
            LOGGER.error("Got unexpected instance state map: {} for segment: {} with status: {}", instanceStateMap,
                latestSegmentName, latestSegmentZKMetadata.getStatus());
            continue;
          }

          // Smallest offset is fetched from stream once and cached in partitionIdToSmallestOffset.
          if (partitionIdToSmallestOffset == null) {
            partitionIdToSmallestOffset = fetchPartitionGroupIdToSmallestOffset(streamConfigs, idealState);
          }

          // Do not create new CONSUMING segment when the stream partition has reached end of life.
          if (!partitionIdToSmallestOffset.containsKey(partitionId)) {
            LOGGER.info("PartitionGroup: {} has reached end of life. Skipping creation of new segment {}",
                partitionId, latestSegmentName);
            continue;
          }

          if (allInstancesOffline) {
            LOGGER.info("Repairing segment: {} which is OFFLINE for all instances in IdealState", latestSegmentName);
            StreamPartitionMsgOffset startOffset =
                selectStartOffset(offsetCriteria, partitionId, partitionIdToStartOffset, partitionIdToSmallestOffset,
                    tableConfig.getTableName(), offsetFactory,
                    latestSegmentZKMetadata.getStartOffset()); // segments are OFFLINE; start from beginning
            createNewConsumingSegment(tableConfig, streamConfigs.get(0), latestSegmentZKMetadata, currentTimeMs,
                partitionGroupMetadataList, instancePartitions, instanceStatesMap, segmentAssignment,
                instancePartitionsMap, startOffset);
          } else {
            LOGGER.info("Resuming consumption for partition: {} of table: {}", partitionId, realtimeTableName);
            StreamPartitionMsgOffset startOffset =
                selectStartOffset(offsetCriteria, partitionId, partitionIdToStartOffset, partitionIdToSmallestOffset,
                    tableConfig.getTableName(), offsetFactory, latestSegmentZKMetadata.getEndOffset());
            createNewConsumingSegment(tableConfig, streamConfigs.get(0), latestSegmentZKMetadata, currentTimeMs,
                partitionGroupMetadataList, instancePartitions, instanceStatesMap, segmentAssignment,
                instancePartitionsMap, startOffset);
          }
        }
      } else {
        // idealstate does not have an entry for the segment (but metadata is present)
        // controller has failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE/COMMITTING)
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
                instancePartitions, numPartitions, numReplicas);
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
      List<StreamConfig> streamConfigs, IdealState idealState) {
    Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToSmallestOffset = new HashMap<>();
    for (StreamConfig streamConfig : streamConfigs) {
      List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList =
          getPartitionGroupConsumptionStatusList(idealState, streamConfigs);
      OffsetCriteria originalOffsetCriteria = streamConfig.getOffsetCriteria();
      streamConfig.setOffsetCriteria(OffsetCriteria.SMALLEST_OFFSET_CRITERIA);

      // Kinesis shard-split flow requires us to pass currentPartitionGroupConsumptionStatusList so that
      // we can check if its completely consumed
      // However the kafka implementation of computePartitionGroupMetadata() breaks if we pass the current status
      // This leads to streamSmallestOffset set to null in selectStartOffset() method
      // The overall dependency isn't clean and is causing the issue and requires refactor
      // Temporarily, we are passing a boolean flag to indicate if we want to use the current status
      // The kafka implementation of computePartitionGroupMetadata() will ignore the current status
      // while the kinesis implementation will use it.
      List<PartitionGroupMetadata> partitionGroupMetadataList =
          getNewPartitionGroupMetadataList(streamConfigs, currentPartitionGroupConsumptionStatusList, true);
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
    RetentionStrategy retentionStrategy =
        new TimeRetentionStrategy(TimeUnit.MILLISECONDS, TimeUtils.VALID_MAX_TIME_MILLIS);

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
      if (shouldSkipSegmentForDeepStoreUpload(tableConfig, segmentZKMetadata, retentionStrategy)) {
        continue;
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
          // upload the segment to deep store and update the segmentZKMetadata
          // the updated metadata is then persisted to ZK.
          uploadToDeepStoreWithFallback(uri, segmentName, rawTableName, segmentZKMetadata, pinotFS);
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

  private void uploadToDeepStoreWithFallback(URI uri, String segmentName, String rawTableName,
      SegmentZKMetadata segmentZKMetadata, PinotFS pinotFS) {
    String serverUploadRequestUrl = getUploadUrl(uri, "uploadCommittedSegment");
    LOGGER.info("Asking server to upload segment: {} by path: {}", segmentName, serverUploadRequestUrl);
    try {
      SegmentZKMetadata uploadedMetadata =
          _fileUploadDownloadClient.uploadLLCToSegmentStoreWithZKMetadata(serverUploadRequestUrl);
      handleMetadataUpload(rawTableName, segmentName, segmentZKMetadata, uploadedMetadata, pinotFS);
      return;
    } catch (Exception e) {
      if (segmentZKMetadata.getStatus() == Status.COMMITTING) {
        throw new RuntimeException("Failed to ask server to upload COMMITTING segment: " + segmentName, e);
      }
      LOGGER.warn("Failed to ask server to upload segment: {} by path: {}, falling back to other upload methods",
          segmentName, serverUploadRequestUrl, e);
    }

    serverUploadRequestUrl = getUploadUrl(uri, "uploadLLCSegment");
    LOGGER.info("Asking server to upload segment: {} by path: {}", segmentName, serverUploadRequestUrl);
    try {
      TableLLCSegmentUploadResponse response =
          _fileUploadDownloadClient.uploadLLCToSegmentStore(serverUploadRequestUrl);
      handleLLCUpload(segmentName, rawTableName, segmentZKMetadata, response, pinotFS);
      return;
    } catch (Exception e) {
      LOGGER.warn("Failed to ask server to upload segment: {} by path: {}, falling back to other upload methods",
          segmentName, serverUploadRequestUrl, e);
    }

    serverUploadRequestUrl = getUploadUrl(uri, "upload");
    LOGGER.info("Asking server to upload segment: {} by path: {}", segmentName, serverUploadRequestUrl);
    try {
      String segmentLocation = _fileUploadDownloadClient.uploadToSegmentStore(serverUploadRequestUrl);
      handleBasicUpload(rawTableName, segmentName, segmentZKMetadata, segmentLocation, pinotFS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to ask server to upload segment: " + segmentName, e);
    }
  }

  private String getUploadUrl(URI uri, String endpoint) {
    return String.format("%s/%s?uploadTimeoutMs=%d", uri.toString(), endpoint, _deepstoreUploadRetryTimeoutMs);
  }

  private void handleMetadataUpload(String rawTableName, String segmentName, SegmentZKMetadata currentMetadata,
      SegmentZKMetadata uploadedMetadata, PinotFS pinotFS)
      throws Exception {
    if (currentMetadata.getStatus() == Status.COMMITTING) {
      LOGGER.info("Updating ZK metadata for committing segment: {}", segmentName);
      currentMetadata.setSimpleFields(uploadedMetadata.getSimpleFields());
    } else if (currentMetadata.getCrc() != uploadedMetadata.getCrc()) {
      LOGGER.info("Updating CRC in ZK metadata for segment: {} from: {} to: {}", segmentName, currentMetadata.getCrc(),
          uploadedMetadata.getCrc());
      currentMetadata.setCrc(uploadedMetadata.getCrc());
    }
    moveSegmentAndSetDownloadUrl(rawTableName, segmentName, uploadedMetadata.getDownloadUrl(), pinotFS,
        currentMetadata);
  }

  private void moveSegmentAndSetDownloadUrl(String rawTableName, String segmentName, String segmentLocation,
      PinotFS pinotFS, SegmentZKMetadata segmentZKMetadata)
      throws Exception {
    String newDownloadUrl = moveSegmentFile(rawTableName, segmentName, segmentLocation, pinotFS);
    LOGGER.info("Updating download url in ZK metadata for segment: {} to: {}", segmentName, newDownloadUrl);
    segmentZKMetadata.setDownloadUrl(newDownloadUrl);
  }

  private void handleLLCUpload(String segmentName, String rawTableName, SegmentZKMetadata currentMetadata,
      TableLLCSegmentUploadResponse response, PinotFS pinotFS)
      throws Exception {
    long currentCrc = currentMetadata.getCrc();
    long newCrc = response.getCrc();
    if (currentCrc != newCrc) {
      LOGGER.info("Updating CRC in ZK metadata for segment: {} from: {} to: {}", segmentName, currentCrc, newCrc);
      currentMetadata.setCrc(newCrc);
    }
    moveSegmentAndSetDownloadUrl(rawTableName, segmentName, response.getDownloadUrl(), pinotFS, currentMetadata);
  }

  private void handleBasicUpload(String rawTableName, String segmentName, SegmentZKMetadata metadata,
      String segmentLocation, PinotFS pinotFS)
      throws Exception {
    moveSegmentAndSetDownloadUrl(rawTableName, segmentName, segmentLocation, pinotFS, metadata);
  }

  private boolean shouldSkipSegmentForDeepStoreUpload(TableConfig tableConfig, SegmentZKMetadata segmentZKMetadata,
      RetentionStrategy retentionStrategy) {
    String realtimeTableName = tableConfig.getTableName();
    String segmentName = segmentZKMetadata.getSegmentName();

    try {
      // Skip the fix for the segment if it is already out of retention.
      if (retentionStrategy.isPurgeable(realtimeTableName, segmentZKMetadata)) {
        LOGGER.info("Skipped deep store uploading of LLC segment {} which is already out of retention", segmentName);
        return true;
      }

      return PauselessConsumptionUtils.isPauselessEnabled(tableConfig) ? shouldSkipForPauselessMode(realtimeTableName,
          segmentZKMetadata) : shouldSkipForNonPauselessMode(segmentZKMetadata);
    } catch (Exception e) {
      LOGGER.warn("Failed checking segment deep store URL for segment {}", segmentName, e);
      return false;
    }
  }

  private boolean shouldSkipForPauselessMode(String realtimeTableName, SegmentZKMetadata segmentZKMetadata) {
    String segmentName = segmentZKMetadata.getSegmentName();

    // Skip if max completion time not exceeded
    if (!isExceededMaxSegmentCompletionTime(realtimeTableName, segmentName, getCurrentTimeMs())) {
      LOGGER.info("Segment: {} has not exceeded max completion time: {}. Not fixing upload failures", segmentName,
          MAX_SEGMENT_COMPLETION_TIME_MILLIS);
      return true;
    }

    // Fix the upload URL in case the controller could not finish commit end metadata step of the segment commit
    // protocol within the required completion time
    if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING) {
      LOGGER.info("Fixing upload URL for segment: {} which is in COMMITTING state", segmentName);
      return false;
    }

    // handle upload failure cases in which the segment is marked DONE but the upload url is missing.
    return shouldSkipForNonPauselessMode(segmentZKMetadata);
  }

  private boolean shouldSkipForNonPauselessMode(SegmentZKMetadata segmentZKMetadata) {
    // Only fix the committed (DONE) LLC segment without deep store copy (empty download URL)
    return segmentZKMetadata.getStatus() != Status.DONE
        || !CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD.equals(segmentZKMetadata.getDownloadUrl());
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
      @Nullable String segmentsToCommit, ForceCommitBatchConfig batchConfig) {
    IdealState idealState = getIdealState(tableNameWithType);
    Set<String> allConsumingSegments = findConsumingSegments(idealState);
    Set<String> targetConsumingSegments =
        filterSegmentsToCommit(allConsumingSegments, partitionGroupIdsToCommit, segmentsToCommit);
    int batchSize = batchConfig.getBatchSize();
    if (batchSize >= targetConsumingSegments.size()) {
      // No need to divide segments in batches.
      sendForceCommitMessageToServers(tableNameWithType, targetConsumingSegments);
    } else {
      List<Set<String>> segmentBatchList = getSegmentBatchList(idealState, targetConsumingSegments, batchSize);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(() -> processBatchesSequentially(segmentBatchList, tableNameWithType, batchConfig));
      executor.shutdown();
    }
    return targetConsumingSegments;
  }

  private void processBatchesSequentially(List<Set<String>> segmentBatchList, String tableNameWithType,
      ForceCommitBatchConfig forceCommitBatchConfig) {
    Set<String> prevBatch = null;
    try {
      for (Set<String> segmentBatchToCommit : segmentBatchList) {
        if (prevBatch != null) {
          waitUntilPrevBatchIsComplete(tableNameWithType, prevBatch, forceCommitBatchConfig);
        }
        sendForceCommitMessageToServers(tableNameWithType, segmentBatchToCommit);
        prevBatch = segmentBatchToCommit;
      }
    } catch (Throwable t) {
      LOGGER.error("Caught exception while force committing segment batches: {}", segmentBatchList, t);
      throw new RuntimeException(t);
    }
  }

  private void waitUntilPrevBatchIsComplete(String tableNameWithType, Set<String> segmentBatchToCommit,
      ForceCommitBatchConfig forceCommitBatchConfig)
      throws InterruptedException {
    int batchStatusCheckIntervalMs = forceCommitBatchConfig.getBatchStatusCheckIntervalMs();
    int batchStatusCheckTimeoutMs = forceCommitBatchConfig.getBatchStatusCheckTimeoutMs();

    Thread.sleep(batchStatusCheckIntervalMs);

    int maxAttempts = (batchStatusCheckTimeoutMs + batchStatusCheckIntervalMs - 1) / batchStatusCheckIntervalMs;
    RetryPolicy retryPolicy = RetryPolicies.fixedDelayRetryPolicy(maxAttempts, batchStatusCheckIntervalMs);
    Set<?>[] segmentsYetToBeCommitted = new Set[1];
    try {
      retryPolicy.attempt(() -> {
        segmentsYetToBeCommitted[0] = getSegmentsYetToBeCommitted(tableNameWithType, segmentBatchToCommit);
        return segmentsYetToBeCommitted[0].isEmpty();
      });
    } catch (AttemptFailureException e) {
      String errorMsg = String.format(
          "Exception occurred while waiting for the forceCommit of segments: %s, attempt count: %d, "
              + "segmentsYetToBeCommitted: %s", segmentBatchToCommit, e.getAttempts(), segmentsYetToBeCommitted[0]);
      throw new RuntimeException(errorMsg, e);
    }

    LOGGER.info("segmentBatch: {} successfully force committed", segmentBatchToCommit);
  }

  @VisibleForTesting
  List<Set<String>> getSegmentBatchList(IdealState idealState, Set<String> targetConsumingSegments, int batchSize) {
    int numSegments = targetConsumingSegments.size();
    List<Set<String>> segmentBatchList = new ArrayList<>((numSegments + batchSize - 1) / batchSize);

    Map<String, Queue<String>> instanceToConsumingSegments =
        getInstanceToConsumingSegments(idealState, targetConsumingSegments);

    Set<String> segmentsAdded = Sets.newHashSetWithExpectedSize(numSegments);
    Set<String> currentBatch = Sets.newHashSetWithExpectedSize(batchSize);
    Collection<Queue<String>> instanceSegmentsCollection = instanceToConsumingSegments.values();

    while (!instanceSegmentsCollection.isEmpty()) {
      Iterator<Queue<String>> instanceCollectionIterator = instanceSegmentsCollection.iterator();
      // Pick segments in round-robin fashion to parallelize forceCommit across max servers
      while (instanceCollectionIterator.hasNext()) {
        Queue<String> consumingSegments = instanceCollectionIterator.next();
        String segmentName = consumingSegments.poll();
        if (consumingSegments.isEmpty()) {
          instanceCollectionIterator.remove();
        }
        if (!segmentsAdded.add(segmentName)) {
          // There might be a segment replica hosted on another instance added before
          continue;
        }
        currentBatch.add(segmentName);
        if (currentBatch.size() == batchSize) {
          segmentBatchList.add(currentBatch);
          currentBatch = Sets.newHashSetWithExpectedSize(batchSize);
        }
      }
    }

    if (!currentBatch.isEmpty()) {
      segmentBatchList.add(currentBatch);
    }
    return segmentBatchList;
  }

  @VisibleForTesting
  Map<String, Queue<String>> getInstanceToConsumingSegments(IdealState idealState,
      Set<String> targetConsumingSegments) {
    Map<String, Queue<String>> instanceToConsumingSegments = new HashMap<>();
    Map<String, Map<String, String>> segmentNameToInstanceToStateMap = idealState.getRecord().getMapFields();

    for (String segmentName : targetConsumingSegments) {
      Map<String, String> instanceToStateMap = segmentNameToInstanceToStateMap.get(segmentName);

      for (Map.Entry<String, String> instanceToState : instanceToStateMap.entrySet()) {
        String instance = instanceToState.getKey();
        String state = instanceToState.getValue();
        if (state.equals(SegmentStateModel.CONSUMING)) {
          instanceToConsumingSegments.computeIfAbsent(instance, k -> new LinkedList<>()).add(segmentName);
        }
      }
    }
    return instanceToConsumingSegments;
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
      Set<String> segmentsToCommit =
          Arrays.stream(segmentsToCommitStr.split(",")).map(String::trim).collect(Collectors.toSet());
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
    _helixResourceManager.invokeControllerPeriodicTask(tableNameWithType, Constants.REALTIME_SEGMENT_VALIDATION_MANAGER,
        taskProperties);

    return new PauseStatusDetails(false, findConsumingSegments(updatedIdealState), reasonCode, comment != null ? comment
        : "Pause flag is cleared. Consuming segments are being created. Use /pauseStatus "
            + "endpoint in a few moments to double check.", new Timestamp(System.currentTimeMillis()).toString());
  }

  public IdealState updatePauseStateInIdealState(String tableNameWithType, boolean pause,
      PauseState.ReasonCode reasonCode, @Nullable String comment) {
    PauseState pauseState =
        new PauseState(pause, reasonCode, comment, new Timestamp(System.currentTimeMillis()).toString());
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
        throw new RuntimeException(
            String.format("No force commit message was sent for table: %s segments: %s", tableNameWithType,
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

  /**
   * Re-ingests segments that are in ERROR state in EV but ONLINE in IS with no peer copy on any server. This method
   * will call the server reingestSegment API
   * on one of the alive servers that are supposed to host that segment according to IdealState.
   *
   * API signature:
   *   POST http://[serverURL]/reingestSegment/[segmentName]
   *   Request body (JSON):
   *
   * If segment is in ERROR state in only few replicas but has download URL, we instead trigger a segment reset
   * @param tableConfig The table config
   */
  public void repairSegmentsInErrorStateForPauselessConsumption(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();
    // Fetch ideal state and external view
    IdealState idealState = getIdealState(realtimeTableName);
    ExternalView externalView = _helixResourceManager.getTableExternalView(realtimeTableName);
    if (externalView == null) {
      LOGGER.warn(
          "External view not found for table: {}, skipping repairing segments in error state for pauseless consumption",
          realtimeTableName);
      return;
    }
    Map<String, Map<String, String>> segmentToInstanceIdealStateMap = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> segmentToInstanceCurrentStateMap = externalView.getRecord().getMapFields();

    // Find segments in ERROR state in externalView
    List<String> segmentsInErrorStateInAllReplicas = new ArrayList<>();
    List<String> segmentsInErrorStateInAtLeastOneReplica = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentToInstanceCurrentStateMap.entrySet()) {
      String segmentName = entry.getKey();

      // Skip non-LLC segments
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName == null) {
        continue;
      }

      // Skip segments that are not in ideal state
      Map<String, String> idealStateMap = segmentToInstanceIdealStateMap.get(segmentName);
      if (idealStateMap == null) {
        continue;
      }

      Map<String, String> currentStateMap = entry.getValue();
      int numReplicasInError = 0;
      for (String state : currentStateMap.values()) {
        if (SegmentStateModel.ERROR.equals(state)) {
          numReplicasInError++;
        }
      }

      if (numReplicasInError == 0) {
        continue;
      }

      // Skip segments that are not ONLINE in ideal state
      boolean hasOnlineInstance = false;
      for (String state : idealStateMap.values()) {
        if (SegmentStateModel.ONLINE.equals(state)) {
          hasOnlineInstance = true;
          break;
        }
      }
      if (!hasOnlineInstance) {
        continue;
      }

      if (numReplicasInError > 0) {
        segmentsInErrorStateInAtLeastOneReplica.add(segmentName);
      }

      if (numReplicasInError == currentStateMap.size()) {
        segmentsInErrorStateInAllReplicas.add(segmentName);
      }
    }


    if (segmentsInErrorStateInAtLeastOneReplica.isEmpty()) {
      _controllerMetrics.setOrUpdateTableGauge(realtimeTableName,
          ControllerGauge.PAUSELESS_SEGMENTS_IN_ERROR_COUNT, 0);
      _controllerMetrics.setOrUpdateTableGauge(realtimeTableName,
          ControllerGauge.PAUSELESS_SEGMENTS_IN_UNRECOVERABLE_ERROR_COUNT, 0);
      return;
    }

    LOGGER.warn("Found {} segments with at least one replica in ERROR state: {}, "
            + "{} segments with all replicas in ERROR state: {} in table: {}, repairing them",
        segmentsInErrorStateInAtLeastOneReplica.size(), segmentsInErrorStateInAtLeastOneReplica,
        segmentsInErrorStateInAllReplicas.size(), segmentsInErrorStateInAllReplicas, realtimeTableName);

    boolean isPartialUpsertEnabled =
        tableConfig.getUpsertConfig() != null && tableConfig.getUpsertConfig().getMode() == UpsertConfig.Mode.PARTIAL;
    boolean isDedupEnabled = tableConfig.getDedupConfig() != null && tableConfig.getDedupConfig().isDedupEnabled();
    if ((isPartialUpsertEnabled || isDedupEnabled)) {
      // We do not run reingestion for dedup and partial upsert tables in pauseless as it can
      // lead to data inconsistencies
      _controllerMetrics.setOrUpdateTableGauge(realtimeTableName,
          ControllerGauge.PAUSELESS_SEGMENTS_IN_UNRECOVERABLE_ERROR_COUNT, segmentsInErrorStateInAllReplicas.size());
      LOGGER.error("Skipping repair for errored segments in table: {} because dedup or partial upsert is enabled.",
          realtimeTableName);
      return;
    } else {
      _controllerMetrics.setOrUpdateTableGauge(realtimeTableName,
          ControllerGauge.PAUSELESS_SEGMENTS_IN_ERROR_COUNT, segmentsInErrorStateInAllReplicas.size());
    }


    for (String segmentName : segmentsInErrorStateInAtLeastOneReplica) {
      SegmentZKMetadata segmentZKMetadata = getSegmentZKMetadata(realtimeTableName, segmentName);
      if (segmentZKMetadata == null) {
        LOGGER.warn("Segment metadata not found for segment: {} in table: {}, skipping repairing it", segmentName,
            realtimeTableName);
        continue;
      }
      // We only consider segments that are in COMMITTING state for reingestion
      if (segmentZKMetadata.getStatus() == Status.COMMITTING && segmentsInErrorStateInAllReplicas.contains(
          segmentName)) {
        LOGGER.info("Segment: {} in table: {} is COMMITTING with all replicas in ERROR state. Triggering re-ingestion.",
            segmentName, realtimeTableName);

        // Find at least one server that should host this segment and is alive
        Map<String, String> idealStateMap = segmentToInstanceIdealStateMap.get(segmentName);
        assert idealStateMap != null;
        String aliveServer = pickServerToReingest(idealStateMap.keySet());
        if (aliveServer == null) {
          LOGGER.warn("No alive server found to re-ingest segment: {} in table: {}, skipping re-ingestion", segmentName,
              realtimeTableName);
          continue;
        }

        try {
          triggerReingestion(aliveServer, segmentName);
          LOGGER.info("Successfully triggered re-ingestion for segment: {} on server: {}", segmentName, aliveServer);
        } catch (Exception e) {
          LOGGER.error("Failed to call reingestSegment for segment: {} on server: {}", segmentName, aliveServer, e);
        }
      } else if (segmentZKMetadata.getStatus() != Status.IN_PROGRESS) {
        // Trigger reset for segment not in IN_PROGRESS state to download the segment from deep store or peer server
        _helixResourceManager.resetSegment(realtimeTableName, segmentName, null);
      }
    }
  }

  /**
   * Invokes the server's reingestSegment API via a POST request with JSON payload,
   * using Simple HTTP APIs.
   *
   * POST http://[serverURL]/reingestSegment/[segmentName]
   */
  private void triggerReingestion(String serverHostPort, String segmentName)
      throws IOException, URISyntaxException, HttpErrorStatusException {
    String scheme = CommonConstants.HTTP_PROTOCOL;
    if (serverHostPort.contains(CommonConstants.HTTPS_PROTOCOL)) {
      scheme = CommonConstants.HTTPS_PROTOCOL;
      serverHostPort = serverHostPort.replace(CommonConstants.HTTPS_PROTOCOL + "://", "");
    } else if (serverHostPort.contains(CommonConstants.HTTP_PROTOCOL)) {
      serverHostPort = serverHostPort.replace(CommonConstants.HTTP_PROTOCOL + "://", "");
    }

    String serverHost = serverHostPort.split(":")[0];
    String serverPort = serverHostPort.split(":")[1];

    URI reingestUri = FileUploadDownloadClient.getURI(scheme, serverHost, Integer.parseInt(serverPort),
        REINGEST_SEGMENT_PATH + "/" + segmentName);
    HttpClient.wrapAndThrowHttpException(HttpClient.getInstance().sendJsonPostRequest(reingestUri, ""));
  }

  /**
   * Picks one server among a set of servers that are supposed to host the segment,
   */
  private String pickServerToReingest(Set<String> candidateServers) {
    try {
      List<String> serverList = new ArrayList<>(candidateServers);
      String server = serverList.get(RANDOM.nextInt(serverList.size()));
      // This should ideally handle https scheme as well
      BiMap<String, String> instanceToEndpointMap =
          _helixResourceManager.getDataInstanceAdminEndpoints(candidateServers);

      if (instanceToEndpointMap.isEmpty()) {
        LOGGER.warn("No instance data admin endpoints found for servers: {}", candidateServers);
        return null;
      }

      // return random server
      return instanceToEndpointMap.get(server);
    } catch (Exception e) {
      LOGGER.warn("Failed to get Helix instance data admin endpoints for servers: {}", candidateServers, e);
    }
    return null;
  }

  public Set<String> getSegmentsYetToBeCommitted(String tableNameWithType, Set<String> segmentsToCheck) {
    Set<String> segmentsYetToBeCommitted = new HashSet<>();
    for (String segmentName : segmentsToCheck) {
      SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
      if (segmentZKMetadata == null) {
        // Segment is deleted. No need to track this segment among segments yetToBeCommitted.
        continue;
      }
      if (!(segmentZKMetadata.getStatus().equals(Status.DONE))) {
        segmentsYetToBeCommitted.add(segmentName);
      }
    }
    return segmentsYetToBeCommitted;
  }

  /**
   * Synchronizes the list of committing segments for a realtime table in ZooKeeper by both adding new segments
   * and removing segments that are no longer in COMMITTING state. This function is designed to be called periodically
   * to maintain an up-to-date list of actively committing segments.
   *
   * The synchronization process works as follows:
   * 1. For a new table (no existing ZooKeeper record), creates a fresh list with the provided segments
   * 2. For an existing table, merges the new segments with currently committing segments while removing any
   *    segments that are no longer in COMMITTING state
   * 3. Maintains uniqueness of segments using a Set-based deduplication
   *
   * @param realtimeTableName Name of the realtime table whose committing segments list needs to be synchronized
   * @param committingSegments List of new segment names that are currently in COMMITTING state.
   *                          If null, returns true without making any changes to the existing list
   * @return true if the synchronization succeeds, false if there's a failure in updating ZooKeeper
   */
  public boolean syncCommittingSegments(String realtimeTableName, List<String> committingSegments) {
    String pauselessDebugMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);
    return updateCommittingSegmentsList(realtimeTableName, () -> {
      // Fetch the committing segments record from the property store.
      Stat stat = new Stat();
      ZNRecord znRecord = _propertyStore.get(pauselessDebugMetadataPath, stat, AccessOption.PERSISTENT);

      // Create ZN record if it doesn't exist
      if (znRecord == null) {
        znRecord = new ZNRecord(realtimeTableName);
        znRecord.setListField(COMMITTING_SEGMENTS, committingSegments);
        return _propertyStore.create(pauselessDebugMetadataPath, znRecord, AccessOption.PERSISTENT);
      }

      // Check ZK metadata again to get the latest list of committing segments
      List<String> committingSegmentsFromPropertyStore = znRecord.getListField(COMMITTING_SEGMENTS);
      List<String> latestCommittingSegments;
      if (CollectionUtils.isEmpty(committingSegmentsFromPropertyStore)) {
        latestCommittingSegments = getCommittingSegments(realtimeTableName, committingSegments);
      } else {
        Set<String> segmentsToCheck = new HashSet<>(committingSegments);
        segmentsToCheck.addAll(committingSegmentsFromPropertyStore);
        latestCommittingSegments = getCommittingSegments(realtimeTableName, segmentsToCheck);
      }
      znRecord.setListField(COMMITTING_SEGMENTS, latestCommittingSegments);
      return _propertyStore.set(pauselessDebugMetadataPath, znRecord, stat.getVersion(), AccessOption.PERSISTENT);
    });
  }

  /**
   * Retrieves and filters the list of committing segments for a realtime table from the property store.
   * This method:
   * 1. Constructs the ZK path for pauseless debug metadata
   * 2. Fetches the committing segments record from the property store
   * 3. Filters out segments that are either deleted or already committed
   *
   * @param realtimeTableName The name of the realtime table to fetch committing segments for
   * @return Filtered list of committing segments
   */
  public List<String> getCommittingSegments(String realtimeTableName) {
    String pauselessDebugMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);
    ZNRecord znRecord = _propertyStore.get(pauselessDebugMetadataPath, null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return List.of();
    }
    return getCommittingSegments(realtimeTableName, znRecord.getListField(COMMITTING_SEGMENTS));
  }

  /**
   * Returns the list of segments that are in COMMITTING state. Filters out segments that are either deleted or no
   * longer in COMMITTING state.
   */
  private List<String> getCommittingSegments(String realtimeTableName, @Nullable Collection<String> segmentsToCheck) {
    if (CollectionUtils.isEmpty(segmentsToCheck)) {
      return List.of();
    }
    List<String> committingSegments = new ArrayList<>(segmentsToCheck.size());
    for (String segment : segmentsToCheck) {
      SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segment);
      if (segmentZKMetadata != null && segmentZKMetadata.getStatus() == Status.COMMITTING) {
        committingSegments.add(segment);
      }
    }
    return committingSegments;
  }
}
