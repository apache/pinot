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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.partition.PartitionAssignment;
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
import com.linkedin.pinot.controller.helix.core.realtime.partition.StreamPartitionAssignmentGenerator;
import com.linkedin.pinot.controller.util.SegmentCompletionUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumer;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.IntRange;
import org.apache.helix.AccessOption;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
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
  private final StreamPartitionAssignmentGenerator _streamPartitionAssignmentGenerator;

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
    _idealstateUpdateLocks = new Lock[NUM_LOCKS];
    for (int i = 0; i < NUM_LOCKS; i++) {
      _idealstateUpdateLocks[i] = new ReentrantLock();
    }
    _tableConfigCache = new TableConfigCache(_propertyStore);
    _streamPartitionAssignmentGenerator = new StreamPartitionAssignmentGenerator(_propertyStore);
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
        // TODO We should not call Kafka to get new number of partitions during controller leadership transfer.
        // Perhaps we should just let validationmanager do all the adjustments, and let the leadership transfer
        // be quick. If the leadership transfer happened due to zk connection issues, then writing to zk at this time
        // may be bad as well.
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
  public void setupNewTable(TableConfig tableConfig, IdealState emptyIdealState) {
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    int partitionCount = getKafkaPartitionCount(streamMetadata);
    IdealState idealState = validateLLCSegments(tableConfig, emptyIdealState, partitionCount);
    _helixAdmin.setResourceIdealState(_clusterName, tableConfig.getTableName(), idealState);
  }

  /*
   * Use helix balancer to balance the kafka partitions amongst the realtime nodes (for a table).
   * The topic name is being used as a dummy helix resource name. We do not read or write to zk in this
   * method.
   */
  public void setupHelixEntries(RealtimeTagConfig realtimeTagConfig, StreamMetadata streamMetadata,
      int nPartitions, final List<String> instanceNames, IdealState idealState, boolean create) {

    // TODO: introduce some abstraction, to make PinotLLCRealtimeSegmentManager handle all types of streams

    TableConfig tableConfig = realtimeTagConfig.getTableConfig();
    final int nReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    final String topicName = streamMetadata.getKafkaTopicName();
    final String realtimeTableName = tableConfig.getTableName();
    final int flushSize = PinotLLCRealtimeSegmentManager.getLLCRealtimeTableFlushSize(tableConfig);

    if (nReplicas > instanceNames.size()) {
      throw new PinotHelixResourceManager.InvalidTableConfigException("Replicas requested(" + nReplicas + ") cannot fit within number of instances(" +
          instanceNames.size() + ") for table " + realtimeTableName + " topic " + topicName);
    }

    List<String> realtimeTablesWithSameTenant =
        getRealtimeTablesWithServerTenant(realtimeTagConfig.getServerTenantName());
    Map<String, PartitionAssignment> newPartitionAssignment =
        _streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig, nPartitions, instanceNames,
            realtimeTablesWithSameTenant);
    _streamPartitionAssignmentGenerator.writeStreamPartitionAssignment(newPartitionAssignment);
    setupInitialSegments(tableConfig, streamMetadata, newPartitionAssignment.get(realtimeTableName), idealState,
        create, flushSize);
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

  protected void setupInitialSegments(TableConfig tableConfig, StreamMetadata streamMetadata, PartitionAssignment partitionAssignment,
      IdealState idealState, boolean create, int flushSize) {
    final String realtimeTableName = tableConfig.getTableName();
    final int nReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    final String initialOffset = streamMetadata.getKafkaConsumerProperties().get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);

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
    final Map<String, List<String>> partitionToServersMap = partitionAssignment.getPartitionToInstances();
    final int nPartitions = partitionToServersMap.size();

    // Create one segment entry in PROPERTYSTORE for each kafka partition.
    // Any of these may already be there, so bail out clean if they are already present.
    final long now = System.currentTimeMillis();
    final int seqNum = STARTING_SEQUENCE_NUMBER;

    List<LLCRealtimeSegmentZKMetadata> segmentZKMetadatas = new ArrayList<>();

    // Create metadata for each segment
    for (int i = 0; i < nPartitions; i++) {
      final List<String> instances = partitionToServersMap.get(Integer.toString(i));
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      final String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
      LLCSegmentName llcSegmentName = new LLCSegmentName(rawTableName, i, seqNum, now);
      final String segName = llcSegmentName.getSegmentName();

      metadata.setCreationTime(now);

      final long startOffset = getPartitionOffset(initialOffset, i, streamMetadata);
      LOGGER.info("Setting start offset for segment {} to {}", segName, startOffset);
      metadata.setStartOffset(startOffset);
      metadata.setEndOffset(END_OFFSET_FOR_CONSUMING_SEGMENTS);

      metadata.setNumReplicas(instances.size());
      metadata.setTableName(rawTableName);
      metadata.setSegmentName(segName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

      // Add the partition metadata if available.
      SegmentPartitionMetadata partitionMetadata = getPartitionMetadataFromTableConfig(realtimeTableName,
          partitionToServersMap.size(), llcSegmentName.getPartitionId());
      if (partitionMetadata != null) {
        metadata.setPartitionMetadata(partitionMetadata);
      }

      segmentZKMetadatas.add(metadata);
      idealStateEntries.put(segName, instances);
    }

    // Compute the number of rows for each segment
    for (LLCRealtimeSegmentZKMetadata segmentZKMetadata : segmentZKMetadatas) {
      updateFlushThresholdForSegmentMetadata(segmentZKMetadata, partitionAssignment, flushSize);
    }

    // Write metadata for each segment to the Helix property store
    List<String> paths = new ArrayList<>(nPartitions);
    List<ZNRecord> records = new ArrayList<>(nPartitions);
    for (LLCRealtimeSegmentZKMetadata segmentZKMetadata : segmentZKMetadatas) {
      ZNRecord record = segmentZKMetadata.toZNRecord();
      final String znodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName,
          segmentZKMetadata.getSegmentName());
      paths.add(znodePath);
      records.add(record);
    }

    writeSegmentsToPropertyStore(paths, records, realtimeTableName);
    LOGGER.info("Added {} segments to propertyStore for table {}", paths.size(), realtimeTableName);

    updateIdealState(idealState, realtimeTableName, idealStateEntries, create, nReplicas);
  }

  void updateFlushThresholdForSegmentMetadata(LLCRealtimeSegmentZKMetadata segmentZKMetadata,
      PartitionAssignment partitionAssignment, int tableFlushSize) {
    // If config does not have a flush threshold, use the default.
    if (tableFlushSize < 1) {
      tableFlushSize = KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount();
    }

    // Gather list of instances for this partition
    Object2IntMap<String> partitionCountForInstance = new Object2IntLinkedOpenHashMap<>();
    String segmentPartitionId = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionRange();
    for (String instanceName : partitionAssignment.getInstancesListForPartition(segmentPartitionId)) {
      partitionCountForInstance.put(instanceName, 0);
    }

    // Find the maximum number of partitions served for each instance that is serving this segment
    int maxPartitionCountPerInstance = 1;
    for (Map.Entry<String, List<String>> partitionAndInstanceList : partitionAssignment.getPartitionToInstances().entrySet()) {
      for (String instance : partitionAndInstanceList.getValue()) {
        if (partitionCountForInstance.containsKey(instance)) {
          int partitionCountForThisInstance = partitionCountForInstance.getInt(instance);
          partitionCountForThisInstance++;
          partitionCountForInstance.put(instance, partitionCountForThisInstance);

          if (maxPartitionCountPerInstance < partitionCountForThisInstance) {
            maxPartitionCountPerInstance = partitionCountForThisInstance;
          }
        }
      }
    }

    // Configure the segment size flush limit based on the maximum number of partitions allocated to a replica
    int segmentFlushSize = (int) (((float) tableFlushSize) / maxPartitionCountPerInstance);
    segmentZKMetadata.setSizeThresholdToFlushSegment(segmentFlushSize);
  }

  // Update the helix idealstate when a new table is added. If createResource is true, then
  // we create a helix resource before setting the idealstate to what we want it to be. Otherwise
  // we expect that idealstate entry is already there, and we update it to what we want it to be.
  @Deprecated
  protected void updateIdealState(final IdealState idealState, String realtimeTableName,
      final Map<String, List<String>> idealStateEntries, boolean createResource, final int nReplicas) {
    if (createResource) {
      addLLCRealtimeSegmentsInIdealState(idealState, idealStateEntries);
      _helixAdmin.addResource(_clusterName, realtimeTableName, idealState);
    } else {
      try {
        HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
          @Override
          public IdealState apply(IdealState idealState) {
            idealState.setReplicas(Integer.toString(nReplicas));
            return addLLCRealtimeSegmentsInIdealState(idealState, idealStateEntries);
          }
        }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
      } catch (Exception e) {
        LOGGER.error("Failed to update idealstate for table {} entries {}", realtimeTableName, idealStateEntries, e);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1);
        throw e;
      }
    }
  }

  // Update the idealstate when an old segment commits and a new one is to be started.
  // This method changes the the idealstate to reflect ONLINE state for old segment,
  // and adds a new helix partition (i.e. pinot segment) in CONSUMING state.
  @Deprecated
  protected void updateIdealState(final String realtimeTableName, final List<String> newInstances,
      final String oldSegmentNameStr, final String newSegmentNameStr) {
    try {
      HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
        @Override
        public IdealState apply(IdealState idealState) {
          return updateForNewRealtimeSegment(idealState, newInstances, oldSegmentNameStr, newSegmentNameStr);
        }
      }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
    } catch (Exception e) {
      LOGGER.error("Failed to update idealstate for table {}, old segment {}, new segment {}, newInstances {}",
          realtimeTableName, oldSegmentNameStr, newSegmentNameStr, newInstances, e);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.LLC_ZOOKEEPER_UPDATE_FAILURES, 1);
      throw e;
    }
  }

  @Deprecated
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

  protected List<ZNRecord> getExistingSegmentMetadata(String realtimeTableName) {
    String propStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    return _propertyStore.getChildren(propStorePath, null, 0);
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

  protected List<String> getAllRealtimeTables() {
    return _helixResourceManager.getAllRealtimeTables();
  }

  protected IdealState getTableIdealState(String realtimeTableName) {
    return HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
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
   * @param committingSegmentNameStr Committing segment name
   * @param nextOffset The offset with which the next segment should start.
   * @param memoryUsedBytes The memory used by committing segment
   * @return
   */
  public boolean commitSegmentMetadata(String rawTableName, final String committingSegmentNameStr, long nextOffset,
      long memoryUsedBytes) {
    final String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);

    /*
     * Update zookeeper in 3 steps.
     *
     * Step 1: Update PROPERTYSTORE to change the old segment metadata status to DONE
     * Step 2: Update PROPERTYSTORE to create the new segment metadata with status IN_PROGRESS
     * Step 3: Update IDEALSTATES to include new segment in CONSUMING state, and change old segment to ONLINE state.
     *
     * The controller may fail between these three steps.
     * So when a new controller takes over as leader, or when validation manager runs, it needs to check the following:
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
     */

    final LLCSegmentName committingLLCSegmentName = new LLCSegmentName(committingSegmentNameStr);
    final int partitionId = committingLLCSegmentName.getPartitionId();
    final int newSeqNum = committingLLCSegmentName.getSequenceNumber() + 1;
    final long now = System.currentTimeMillis();
    LLCSegmentName newLLCSegmentName = new LLCSegmentName(committingLLCSegmentName.getTableName(), partitionId, newSeqNum, now);
    String newSegmentNameStr = newLLCSegmentName.getSegmentName();

    final PartitionAssignment partitionAssignment = getStreamPartitionAssignment(realtimeTableName);
    // If an LLC table is dropped (or cleaned up), we will get null here. In that case we should not be creating a new segment
    if (partitionAssignment == null) {
      LOGGER.warn("Kafka partition assignment not found for {}", realtimeTableName);
      throw new RuntimeException("Kafka partition assignment not found. Not committing segment");
    }
    List<String> newInstances = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));

    // Step-1
    boolean success = updateOldSegmentMetadataZNRecord(realtimeTableName, committingLLCSegmentName, nextOffset);
    if (!success) {
      return false;
    }

    // Step-2
    success = createNewSegmentMetadataZNRecord(realtimeTableName, newLLCSegmentName, nextOffset, partitionAssignment);
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
      updateIdealState(realtimeTableName, newInstances, committingSegmentNameStr, newSegmentNameStr);
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
  private boolean updateOldSegmentMetadataZNRecord(String realtimeTableName, LLCSegmentName committingLLCSegmentName,
      long nextOffset) {

    Stat stat = new Stat();
    String committingSegmentNameStr = committingLLCSegmentName.getSegmentName();
    final LLCRealtimeSegmentZKMetadata oldSegMetadata = getRealtimeSegmentZKMetadata(realtimeTableName,
        committingSegmentNameStr, stat);

    if (oldSegMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.IN_PROGRESS) {
      LOGGER.warn("Status of segment metadata {} has already been changed by other controller for table {}: Status={}",
          committingSegmentNameStr, realtimeTableName, oldSegMetadata.getStatus());
      return false;
    }

    // TODO: set number of rows to end consumption in new segment metadata, based on memory used and number of rows from old segment
    oldSegMetadata.setEndOffset(nextOffset);
    oldSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
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
    oldSegMetadata.setPartitionMetadata(getPartitionMetadataFromSegmentMetadata(segmentMetadata));

    final ZNRecord oldZnRecord = oldSegMetadata.toZNRecord();
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
   * @param realtimeTableName - table name of the segment for which new metadata is being created
   * @param newLLCSegmentName - new segment name
   * @param nextOffset - start offset for new segment
   * @param partitionAssignment - stream partition assignment for this table
   * @return
   */
  protected boolean createNewSegmentMetadataZNRecord(String realtimeTableName, LLCSegmentName newLLCSegmentName,
      long nextOffset, PartitionAssignment partitionAssignment) {

    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    int partitionId = newLLCSegmentName.getPartitionId();
    int numReplicas = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId)).size();
    ZNRecord newZnRecord = makeZnRecordForNewSegment(rawTableName, numReplicas, nextOffset, newLLCSegmentName,
        partitionAssignment.getNumPartitions());
    final LLCRealtimeSegmentZKMetadata newSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata(newZnRecord);

    updateFlushThresholdForSegmentMetadata(newSegmentZKMetadata, partitionAssignment,
        getRealtimeTableFlushSizeForTable(rawTableName));
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

  protected int getRealtimeTableFlushSizeForTable(String tableName) {
    TableConfig tableConfig = getRealtimeTableConfig(tableName);
    return getLLCRealtimeTableFlushSize(tableConfig);
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
   * Returns the max number of rows that a host holds across all consuming LLC partitions.
   * This number should be divided by the number of partitions on the host, so as to get
   * the flush limit for each segment.
   *
   * If flush threshold is configured for LLC, return it, otherwise, if flush threshold is
   * configured for HLC, then return that value, else return -1.
   *
   * @param tableConfig
   * @return -1 if tableConfig is null, or neither value is configured
   */
  public static int getLLCRealtimeTableFlushSize(TableConfig tableConfig) {
    final Map<String, String> streamConfigs = tableConfig.getIndexingConfig().getStreamConfigs();
    String flushSizeStr;
    if (streamConfigs == null) {
      return -1;
    }
    if (streamConfigs.containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE)) {
      flushSizeStr = streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE);
      try {
        return Integer.parseInt(flushSizeStr);
      } catch (Exception e1) {
        LOGGER.warn("Failed to parse LLC flush size of {} for table {}", flushSizeStr, tableConfig.getTableName(), e1);
      }
    }

    if (streamConfigs.containsKey(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE)) {
      flushSizeStr = streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE);
      try {
        return Integer.parseInt(flushSizeStr);
      } catch (Exception e2) {
        LOGGER.warn("Failed to parse flush size of {} for table {}", flushSizeStr, tableConfig.getTableName(), e2);
      }
    }
    return -1;
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

  @Deprecated
  private void completeCommittingSegments() {
    for (String realtimeTableName : getAllRealtimeTables()) {
      completeCommittingSegments(realtimeTableName);
    }
  }

  @Deprecated
  public void completeCommittingSegments(String realtimeTableName) {
    List<ZNRecord> segmentMetadataList = getExistingSegmentMetadata(realtimeTableName);
    if (segmentMetadataList == null || segmentMetadataList.isEmpty()) {
      return;
    }
    final Map<String, LLCRealtimeSegmentZKMetadata> segmentIdToMetadataMap = new HashMap<>(segmentMetadataList.size());
    for (ZNRecord segment : segmentMetadataList) {
      if (SegmentName.isLowLevelConsumerSegmentName(segment.getId())) {
        segmentIdToMetadataMap.put(segment.getId(), new LLCRealtimeSegmentZKMetadata(segment));
      }
    }

    if (segmentIdToMetadataMap.isEmpty()) {
      return;
    }

    completeCommittingSegments(realtimeTableName, segmentIdToMetadataMap);
  }

  @Deprecated
  private void completeCommittingSegmentsInternal(String realtimeTableName,
      Map<Integer, MinMaxPriorityQueue<LLCSegmentName>> partitionToLatestSegments,
      Map<String, LLCRealtimeSegmentZKMetadata> segmentIdToMetadataMap) {
    IdealState idealState = getTableIdealState(realtimeTableName);
    Set<String> segmentNamesIS = idealState.getPartitionSet();

    final PartitionAssignment partitionAssignment = getStreamPartitionAssignment(realtimeTableName);

    for (Map.Entry<Integer, MinMaxPriorityQueue<LLCSegmentName>> entry : partitionToLatestSegments.entrySet()) {
      final int partitionId = entry.getKey();
      List<String> newInstances = partitionAssignment.getInstancesListForPartition(Integer.toString(partitionId));
      final LLCSegmentName segmentName = entry.getValue().pollFirst();
      final String segmentId = segmentName.getSegmentName();
      LLCRealtimeSegmentZKMetadata segmentZKMetadata = segmentIdToMetadataMap.get(segmentId);

      if (segmentNamesIS.contains(segmentId)) {
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        if (segmentZKMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)
            && instanceStateMap.values().contains(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
          // controller failed between step-1 and step-2 of commitSegmentMetadata.
          // i.e. after updating old segment metadata (segment metadata state = DONE),
          // but before creating new segment metadata and updating ideal state (new segment metadata missing, old segment ideal state = CONSUMING)

          LOGGER.info("{}:Repairing segment for partition {}. "
              + "Old segment metadata {} has status DONE, but segments are still in CONSUMING state in ideal STATE",
              realtimeTableName, partitionId, segmentId);

          final int newSeqNum = segmentName.getSequenceNumber() + 1;
          final long now = System.currentTimeMillis();
          LLCSegmentName newLLCSegmentName = new LLCSegmentName(segmentName.getTableName(), partitionId, newSeqNum, now);

          LOGGER.info("{}: Creating new segment metadata for {}", realtimeTableName, newLLCSegmentName.getSegmentName());
          createNewSegmentMetadataZNRecord(realtimeTableName, newLLCSegmentName, segmentZKMetadata.getEndOffset(),
              partitionAssignment);
          updateIdealState(realtimeTableName, newInstances, segmentId, newLLCSegmentName.getSegmentName());
        }
      } else {
        // controller failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE) and creating new segment metadata (new segment metadata state = IN_PROGRESS),
        // but before updating ideal state (new segment ideal missing from ideal state)

        LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", realtimeTableName,
            partitionId, segmentId);

        LOGGER.info("{}: Assigning segment {} to {}", realtimeTableName, segmentId, newInstances);
        // TODO Re-write num-partitions in metadata if needed.
        // If there was a prev segment in the same partition, then we need to fix it to be ONLINE.

        LLCSegmentName prevSegmentName = entry.getValue().pollLast();
        String prevSegmentNameStr = null;
        if (prevSegmentName != null) {
          prevSegmentNameStr = prevSegmentName.getSegmentName();
        }
        updateIdealState(realtimeTableName, newInstances, prevSegmentNameStr, segmentId);
      }
    }
  }

  @Deprecated
  public void completeCommittingSegments(String realtimeTableName,
      Map<String, LLCRealtimeSegmentZKMetadata> segmentIdToMetadataMap) {
    Comparator<LLCSegmentName> comparator = new Comparator<LLCSegmentName>() {
      @Override
      public int compare(LLCSegmentName o1, LLCSegmentName o2) {
        return o2.compareTo(o1);
      }
    };

    Map<Integer, MinMaxPriorityQueue<LLCSegmentName>> partitionToLatestSegments = new HashMap<>();

    for (String segmentId : segmentIdToMetadataMap.keySet()) {
      LLCSegmentName segmentName = new LLCSegmentName(segmentId);
      final int partitionId = segmentName.getPartitionId();
      MinMaxPriorityQueue latestSegments = partitionToLatestSegments.get(partitionId);
      if (latestSegments == null) {
        latestSegments = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(2).create();
        partitionToLatestSegments.put(partitionId, latestSegments);
      }
      latestSegments.offer(segmentName);
    }

    completeCommittingSegmentsInternal(realtimeTableName, partitionToLatestSegments, segmentIdToMetadataMap);
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

  /**
   * Create a consuming segment for the kafka partitions that are missing one.
   *
   * @param realtimeTableName is the name of the realtime table (e.g. "table_REALTIME")
   * @param nonConsumingPartitions is a set of integers (kafka partitions that do not have a consuming segment)
   * @param llcSegments is a list of segment names in the ideal state as was observed last.
   */
  @Deprecated
  public void createConsumingSegment(final String realtimeTableName, final Set<Integer> nonConsumingPartitions,
      final List<String> llcSegments, final TableConfig tableConfig) {
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    final PartitionAssignment partitionAssignment = getStreamPartitionAssignment(realtimeTableName);
    final HashMap<Integer, LLCSegmentName> ncPartitionToLatestSegment = new HashMap<>(nonConsumingPartitions.size());
    final int nReplicas = partitionAssignment.getInstancesListForPartition("0").size(); // Number of replicas (should be same for all partitions)

    // For each non-consuming partition, find the latest segment (i.e. segment with highest seq number) for that partition.
    // (null if there is none).
    for (String segmentId : llcSegments) {
      LLCSegmentName segmentName = new LLCSegmentName(segmentId);
      int partitionId = segmentName.getPartitionId();
      if (nonConsumingPartitions.contains(partitionId)) {
        LLCSegmentName hashedSegName = ncPartitionToLatestSegment.get(partitionId);
        if (hashedSegName == null || hashedSegName.getSequenceNumber() < segmentName.getSequenceNumber()) {
          ncPartitionToLatestSegment.put(partitionId, segmentName);
        }
      }
    }

    // For each non-consuming partition, create a segment with a sequence number one higher than the latest segment.
    // If there are no segments, then this is the first segment, so create the new segment with sequence number
    // STARTING_SEQUENCE_NUMBER.
    // Pick the starting offset of the new segment depending on the end offset of the prev segment (if available
    // and completed), or the table configuration (smallest/largest).
    for (int partition : nonConsumingPartitions) {
      try {
        LLCSegmentName latestSegment = ncPartitionToLatestSegment.get(partition);
        long startOffset;
        int nextSeqNum;
        List<String> instances = partitionAssignment.getInstancesListForPartition(Integer.toString(partition));
        if (latestSegment == null) {
          // No segment yet in partition, Create a new one with a starting offset as per table config specification.
          nextSeqNum = STARTING_SEQUENCE_NUMBER;
          LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", realtimeTableName, partition,
              nextSeqNum);
          String consumerStartOffsetSpec = streamMetadata.getKafkaConsumerProperties()
              .get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
          startOffset = getKafkaPartitionOffset(streamMetadata, consumerStartOffsetSpec, partition);
          LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, realtimeTableName, partition);
        } else {
          nextSeqNum = latestSegment.getSequenceNumber() + 1;
          LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", realtimeTableName, partition,
              nextSeqNum);
          // To begin with, set startOffset to the oldest available offset in kafka. Fix it to be the one we want,
          // depending on what the prev segment had.
          startOffset = getKafkaPartitionOffset(streamMetadata, "smallest", partition);
          LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, realtimeTableName, partition);
          startOffset = getBetterStartOffsetIfNeeded(realtimeTableName, partition, latestSegment, startOffset,
              nextSeqNum);
        }
        createSegment(realtimeTableName, nReplicas, partition, nextSeqNum, instances, startOffset, partitionAssignment);
      } catch (Exception e) {
        LOGGER.error("Exception creating CONSUMING segment for {} partition {}", realtimeTableName, partition, e);
      }
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

  private void createSegment(String realtimeTableName, int numReplicas, int partitionId, int seqNum,
      List<String> serverInstances, long startOffset, PartitionAssignment partitionAssignment) {
    LOGGER.info("Attempting to auto-create a segment for partition {} of table {}", partitionId, realtimeTableName);
    final List<String> propStorePaths = new ArrayList<>(1);
    final List<ZNRecord> propStoreEntries = new ArrayList<>(1);
    long now = System.currentTimeMillis();
    final String tableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    LLCSegmentName newSegmentName = new LLCSegmentName(tableName, partitionId, seqNum, now);
    final String newSegmentNameStr = newSegmentName.getSegmentName();
    ZNRecord newZnRecord = makeZnRecordForNewSegment(realtimeTableName, numReplicas, startOffset,
        newSegmentName, partitionAssignment.getNumPartitions());

    final LLCRealtimeSegmentZKMetadata newSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata(newZnRecord);

    updateFlushThresholdForSegmentMetadata(newSegmentZKMetadata, partitionAssignment,
        getRealtimeTableFlushSizeForTable(realtimeTableName));
    newZnRecord = newSegmentZKMetadata.toZNRecord();

    final String newZnodePath = ZKMetadataProvider
        .constructPropertyStorePathForSegment(realtimeTableName, newSegmentNameStr);
    propStorePaths.add(newZnodePath);
    propStoreEntries.add(newZnRecord);

    writeSegmentsToPropertyStore(propStorePaths, propStoreEntries, realtimeTableName);

    updateIdealState(realtimeTableName, serverInstances, null, newSegmentNameStr);

    LOGGER.info("Successful auto-create of CONSUMING segment {}", newSegmentNameStr);
    _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.LLC_AUTO_CREATED_PARTITIONS, 1);
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
   * See {@link #createConsumingSegment(String, Set, List, TableConfig)}
   */
  @Deprecated
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
   * Update the kafka partitions as necessary to accommodate changes in number of replicas, number of tenants or
   * number of kafka partitions. As new segments are assigned, they will obey the new kafka partition assignment.
   *
   * @param tableConfig tableConfig from propertystore
   */
  @Deprecated
  public void updateKafkaPartitionsIfNecessary(TableConfig tableConfig) {

    RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig, _helixManager);

    final String realtimeTableName = tableConfig.getTableName();
    final PartitionAssignment partitionAssignment = getStreamPartitionAssignment(realtimeTableName);
    final Map<String, List<String>> partitionToServersMap = partitionAssignment.getPartitionToInstances();
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());

    String consumingServersTag = realtimeTagConfig.getConsumingServerTag();
    final List<String> currentInstances = getInstances(consumingServersTag);

    // Previous partition count is what we find in the Kafka partition assignment znode.
    // Get the current partition count from Kafka.
    final int prevPartitionCount = partitionToServersMap.size();
    int currentPartitionCount = -1;
    try {
      currentPartitionCount = getKafkaPartitionCount(streamMetadata);
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

    if (!prevInstances.equals(new HashSet<>(currentInstances))) {
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
      LOGGER.info("Not updating Kafka partition assignment for table {}", realtimeTableName);
      return;
    }

    // Generate new kafka partition assignment and update the znode
    if (currentInstances.size() < currentReplicaCount) {
      LOGGER.error("Cannot have {} replicas in {} instances for {}.Not updating partition assignment", currentReplicaCount, currentInstances.size(), realtimeTableName);
      long numOfInstancesNeeded = currentReplicaCount - currentInstances.size();
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.SHORT_OF_LIVE_INSTANCES, numOfInstancesNeeded);
      return;
    } else {
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.SHORT_OF_LIVE_INSTANCES, 0);
    }


    /**
     *
     * NOTE: There is a race condition here, between the validation manager and a new table creation,
     * as both the flows want to read all stream partitions in a tenant and write all stream partitions back (depending on the stream partition strategy of course)
     *
     * Scenario:
     * Lead controller is trying to update all stream partition assignments in a tenant, because it found a new partition for a table
     * Non-lead controller gets a call to add a new table, in the same tenant
     *
     * Steps:
     *
     *    Lead controller        |    Non-lead controller
     *    ----------------------------------------------------
     *    Validation manager     |
     *    reads all partitions   |
     *                           |
     *                           |New table creation
     *                           |reads all partitions
     *                           |
     *    Validation manager     |
     *    writes new partition   |
     *    assignments            |
     *                           |
     *                           |Table creation writes
     *                           |new partition assignments,
     *                           |overriding the new partition
     *                           |written by validation manager
     *                           |
     *   -------------------------------------------------------
     *   We have lost the new partition written by the validation manager.
     *   However, it will be corrected when the validation manager runs the next time, which is after 1 hour
     *   The partition will make it to the ideal state in the run after that.
     *   As we can see, although we have this race condition,
     *   it is costing us only an additional hour's delay in getting the partition to the ideal state
     *   For now, we will tolerate this delay.
     */
    List<String> realtimeTablesWithSameTenant =
        getRealtimeTablesWithServerTenant(realtimeTagConfig.getServerTenantName());
    Map<String, PartitionAssignment> newPartitionAssignment =
        _streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig, currentPartitionCount,
            currentInstances, realtimeTablesWithSameTenant);
    _streamPartitionAssignmentGenerator.writeStreamPartitionAssignment(newPartitionAssignment);
    LOGGER.info("Successfully updated Kafka partition assignment for table {}", realtimeTableName);
  }



  /**
   * Get all realtime tables with given tenant
   * @param serverTenantName
   * @return
   */
  protected List<String> getRealtimeTablesWithServerTenant(String serverTenantName) {
    List<String> realtimeTablesWithServerTenant = new ArrayList<>();
    for (String tableName : _helixResourceManager.getAllRealtimeTables()) {
      TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName);
      if (realtimeTableConfig.getTenantConfig().getServer().equals(serverTenantName)) {
        realtimeTablesWithServerTenant.add(tableName);
      }
    }
    return realtimeTablesWithServerTenant;
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

  public PartitionAssignment getStreamPartitionAssignment(String realtimeTableName) {
    return _streamPartitionAssignmentGenerator.getStreamPartitionAssignment(realtimeTableName);
  }

  protected List<String> getInstances(String tenantName) {
    return _helixAdmin.getInstancesInClusterWithTag(_clusterName, tenantName);
  }

  ////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////

  protected List<LLCRealtimeSegmentZKMetadata> getAllSegmentMetadata(String tableNameWithType) {
    return ZKMetadataProvider.getLLCRealtimeSegmentZKMetadataListForTable(_helixManager.getHelixPropertyStore(), tableNameWithType);
  }

  // Gets latest 2 metadata. We need only the 2 latest metadata for each partition
  private Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> getLatestMetadata(String tableNameWithType) {
    List<LLCRealtimeSegmentZKMetadata> metadatas = getAllSegmentMetadata(tableNameWithType);

    Comparator<LLCRealtimeSegmentZKMetadata> comparator = new Comparator<LLCRealtimeSegmentZKMetadata>() {
      @Override
      public int compare(LLCRealtimeSegmentZKMetadata o1, LLCRealtimeSegmentZKMetadata o2) {
        LLCSegmentName s1 = new LLCSegmentName(o1.getSegmentName());
        LLCSegmentName s2 = new LLCSegmentName(o2.getSegmentName());
        return s2.compareTo(s1);
      }
    };

    Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> partitionToLatestSegments = new HashMap<>();

    for (LLCRealtimeSegmentZKMetadata metadata : metadatas) {
      LLCSegmentName segmentName = new LLCSegmentName(metadata.getSegmentName());
      final int partitionId = segmentName.getPartitionId();
      MinMaxPriorityQueue latestSegments = partitionToLatestSegments.get(partitionId);
      if (latestSegments == null) {
        latestSegments = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(2).create();
        partitionToLatestSegments.put(partitionId, latestSegments);
      }
      latestSegments.offer(segmentName);
    }

    return partitionToLatestSegments;
  }

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

  private void updateIdealStateOnSegmentCompletion(@Nonnull  final TableConfig tableConfig, @Nonnull  final String currentSegmentId, @Nonnull final String newSegmentId, @Nonnull final PartitionAssignment partitionAssignment) {
    final String tableNameWithType = tableConfig.getTableName();

    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        return updateIdealStateOnSegmentCompletion(idealState, tableConfig, currentSegmentId, newSegmentId, partitionAssignment);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(10, 1000L, 1.2f));
  }


  private PartitionAssignment getPartitionAssignmentFromIdealState(String tableNameWithType,
      Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> latestMetadata,
      IdealState idealState) {
    PartitionAssignment partitionAssignment = new PartitionAssignment(tableNameWithType);
    for (Map.Entry<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> entry : latestMetadata.entrySet()) {
      int partition = entry.getKey();
      MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata> priorityQueue = entry.getValue();
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(priorityQueue.peekFirst().getSegmentName());
      if (instanceStateMap == null) {
        if (priorityQueue.size() > 1) {   // Expects size to be 2
          instanceStateMap = idealState.getInstanceStateMap(priorityQueue.peekLast().getSegmentName());
          // We can assert that instanceStateMap cannot be null here. metadata cannot be more than
          // one segment ahead of idealstate.
          partitionAssignment.addPartition(Integer.toString(partition), new ArrayList<String>(instanceStateMap.keySet()));
        }
      } else {
        partitionAssignment.addPartition(Integer.toString(partition), new ArrayList<String>(instanceStateMap.keySet()));
      }
      // TODO
      // May be that we had one metadata and no entry in idealstate (table just created). In that case, the partition
      // is not assigned yet, so we need to assign the partitions.
    }
    return partitionAssignment;
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
  private static int MAX_SEGMENT_COMPLETION_TIME_MINS = 10;
  private boolean isTooSoonToCorrect(String tableNameWithType, String segmentId) {
    long now = System.currentTimeMillis();
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
   * This method combines the logic in the previous completeCommitingSegmentsInternal()
   * and createConsumingSegment() methods.
   *
   * So, the method may:
   * - Add or modify one or more segment metadata znodes
   * - Update the idealstate, which may fail if some other process updated the idealstate.
   *
   * In case idealstate update fails, then we need to start over.
   *
   * TODO Verify right things happen in the following case:
   * - A segment completion is triggered, that manages to update metadata but ideal state update fails
   *   and so it is currently in back-off stage.
   * - ValidationManager starts to run, and finds the segment in improper state and manages to fix it in idealstate.
   * - The segment completion thread comes back to update the ideal state (should already find the same segment in there)
   *
   * TODO Can we re-use this method when creating a table as well?
   *
   */
  private IdealState validateLLCSegments(final TableConfig tableConfig, IdealState idealState, final int partitionCount) {
    final String tableNameWithType = tableConfig.getTableName();
    final StreamMetadata streamMetadata = new StreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    if (!idealState.isEnabled()) {
      LOGGER.info("Skipping validation for disabled table {}", tableNameWithType);
      return idealState;
    }

    // Get the metadata for the latest 2 segments of each partition
    Map<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> latestMetadata = getLatestMetadata(tableNameWithType);
    // Get the latest segments as a set to provide for the segment assignment logic.
    // alternatively, the segment assignment logic can scan through the idealstate to get the latest segments.
    // TBD
    {
      Set<String> latestSegmentIds = new HashSet<>();
      for (Map.Entry<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> entry : latestMetadata.entrySet()) {
        LLCRealtimeSegmentZKMetadata[] metadatas = new LLCRealtimeSegmentZKMetadata[entry.getValue().size()];
        entry.getValue().toArray(metadatas);
        for (LLCRealtimeSegmentZKMetadata metadata : metadatas) {
          latestSegmentIds.add(metadata.getSegmentName());
        }
      }
    }
    {
      /*
       * To merge with branch realtime_segment_assignment
       * This will get us the partitions
      PartitionAssignmentGenerator generator = new PartitionAsignmentGenerator(_helixAdmin)
      PartitionAssignment partitionAssignment = generator.generatePartitionAssignment(tableConfig, partitionCount);
      */
    }

    // Find partitions for which there is no metadata at all. These are new partitions that we need to start consuming.
    Set<Integer> newPartitions = new HashSet<>(partitionCount);
    for (int partition = 0; partition < partitionCount; partition++) {
      if (!latestMetadata.containsKey(partition)) {
        LOGGER.info("Found partition {} with no segments", partition);
        newPartitions.add(partition);
      }
    }

    // TODO Replace this with above partitionAssignment above (merge with realtime_segment_assignment).
    PartitionAssignment partitionAssignment = getPartitionAssignmentFromIdealState(tableNameWithType,
        latestMetadata, idealState);

    Map<String, String> oldToNewSegmentsMap = new HashMap<>();
    // Walk over all partitions that we have metadata for, and repair any partitions necessary.
    // Possible things to repair:
    // 1. The latest metadata is in DONE state, but the idealstate says segment is CONSUMING:
    //    a. Create metadata for next segment and find hosts to assign it to.
    //    b. update current segment in idealstate to ONLINE
    //    c. add new segment in idealstate to CONSUMING on the hosts.
    // 2. The latest metadata is IN_PROGRESS, but segment is not there in idealstate.
    //    a. change prev segment to ONLINE in idealstate
    //    b. add latest segment to CONSUMING in idealstate.
    // 3.
    for (Map.Entry<Integer, MinMaxPriorityQueue<LLCRealtimeSegmentZKMetadata>> entry : latestMetadata.entrySet()) {
      int partition = entry.getKey();
      LLCRealtimeSegmentZKMetadata metadata = entry.getValue().pollFirst();
      final String segmentId = metadata.getSegmentName();
      final LLCSegmentName segmentName = new LLCSegmentName(segmentId);

      if (isTooSoonToCorrect(tableNameWithType, segmentId)) {
        LOGGER.info("Skipping correction of segment segment {} (too soon to correct)", segmentId);
        continue;
      }

      if (idealState.getPartitionSet().contains(segmentId)) {
        // Latest segment of metadata is in idealstate.
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        if (instanceStateMap.values().contains(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
          if (metadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {
            // step-1 of commmitSegmentMetadata is done (i.e. marking old segment as DONE)
            // but step-2 is not done (i.e. adding new metadata for the next segment)
            // and ideal state update (i.e. marking old segment as ONLINE and new segment as CONSUMING) is not done either.
            LOGGER.info("{}:Repairing segment for partition {}. " + "Old segment metadata {} has status DONE, but segments are still in CONSUMING state in ideal STATE",
                tableNameWithType, partition, segmentId);

            final int newSeqNum = segmentName.getSequenceNumber() + 1;
            // TODO Need to create a method to get the time so that we can test it right.
            final long now = System.currentTimeMillis();
            LLCSegmentName newLLCSegmentName =
                new LLCSegmentName(segmentName.getTableName(), partition, newSeqNum, now);

            LOGGER.info("{}: Creating new segment metadata for {}", tableNameWithType, newLLCSegmentName.getSegmentName());

            // TODO
            // Change the lines below to do the following once segment assignment interfaces are ready
            // 1. get PartitionAssignment from idealstate (and the new segment?)
            // 2. use partition assignment to fill in the number of rows metadata
            // 3. add metadata to zk
            // 4. change idealstate.
            createNewSegmentMetadataZNRecord(tableNameWithType, newLLCSegmentName, metadata.getEndOffset(),
                partitionAssignment);

            oldToNewSegmentsMap.put(metadata.getSegmentName(), newLLCSegmentName.getSegmentName());
            // TODO make this a class method rather than static?
//            updateForNewRealtimeSegment(idealState, newInstances, metadata.getSegmentName(), newLLCSegmentName.getSegmentName());
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
          startOffset = getBetterStartOffsetIfNeeded(tableNameWithType, partition, segmentName, startOffset,
              nextSeqNum);
          // TODO Need to create a method for getting the time, so that we can test it with time moving forward.
          LLCSegmentName newLLCSegmentName = new LLCSegmentName(segmentName.getTableName(), partition, nextSeqNum, System.currentTimeMillis());

          // TODO Change the lines below to the following:
          // 1. get PartitionAssignment from idealstate (and the new segment?)
          // 2. use partition assignment to fill in the number of rows metadata
          // 3. add metadata to zk
          // 4. change idealstate.
          createNewSegmentMetadataZNRecord(tableNameWithType, newLLCSegmentName, startOffset,
              partitionAssignment);
          oldToNewSegmentsMap.put("_PINOT_NEW_" + String.valueOf(partition), newLLCSegmentName.getSegmentName());
          //updateForNewRealtimeSegment(idealState, newInstances, segmentId, newLLCSegmentName.getSegmentName());
        }
        // else It can be in ONLINE state, in which case there is no need to repair the segment.
      } else {
        // idealstate does not have an entry for the segment (but metadata is present)
        // controller has failed between step-2 and step-3 of commitSegmentMetadata.
        // i.e. after updating old segment metadata (old segment metadata state = DONE) and creating new segment metadata (new segment metadata state = IN_PROGRESS),
        // but before updating ideal state (new segment ideal missing from ideal state)

        // TODO Assert that the metadata is IN_PROGRESS
        LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", tableNameWithType,
            partition, segmentId);

//        LOGGER.info("{}: Assigning segment {} to {}", tableNameWithType, segmentId, newInstances);
        // TODO Re-write num-partitions in metadata if needed.
        // If there was a prev segment in the same partition, then we need to fix it to be ONLINE.

        LLCRealtimeSegmentZKMetadata prevMetadata = entry.getValue().pollLast();
        String prevSegmentId = "_PINOT_NEW" + String.valueOf(partition);
        if (prevMetadata != null) {
          prevSegmentId = prevMetadata.getSegmentName();
        }

        oldToNewSegmentsMap.put(prevSegmentId, segmentId);
//        updateForNewRealtimeSegment(idealState, newInstances, prevSegmentId, segmentId);
      }
    }

    for (int partition : newPartitions) {
      // We will surely not have partition assignment for these, because these are partitions not even in metadata
      // TODO Change this to get segment assignment for all partitions in one go after segment assignment code is done
      // For now, assume that the partitions have been assigned.
      // No segment yet in partition, Create a new one with a starting offset as per table config specification.
      int nextSeqNum = STARTING_SEQUENCE_NUMBER;
      LOGGER.info("Creating CONSUMING segment for {} partition {} with seq {}", tableNameWithType, partition,
          nextSeqNum);
      String consumerStartOffsetSpec = streamMetadata.getKafkaConsumerProperties()
          .get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
      long startOffset = getKafkaPartitionOffset(streamMetadata, consumerStartOffsetSpec, partition);
      LOGGER.info("Found kafka offset {} for table {} for partition {}", startOffset, tableNameWithType, partition);
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

      LLCSegmentName newLLCSegmentName = new LLCSegmentName(rawTableName, partition, nextSeqNum, System.currentTimeMillis());

      // TODO Need to create another method that we can use.
      createNewSegmentMetadataZNRecord(tableNameWithType, newLLCSegmentName, startOffset,
          partitionAssignment);
      oldToNewSegmentsMap.put("_PINOT_NEW_" + String.valueOf(partition), newLLCSegmentName.getSegmentName());
//      List<String> newInstances = partitionAssignment.getInstancesListForPartition(Integer.toString(partition));
//      updateForNewRealtimeSegment(idealState, newInstances, null, newLLCSegmentName.getSegmentName());
    }

    // TODO Call assignment interface here
    Map<String, List<String>> assignments = null;
    /*
      RealtimeSegmentAssignmentStrategy strategy = new ConsumingSegmentAssignmentStrategy();
      assignments = strategy.assign(newSegments, partitionAssignment);
     */

    for (Map.Entry<String, String> entry : oldToNewSegmentsMap.entrySet()) {
      String oldSeg = entry.getKey();
      String newSeg = entry.getValue();
      List<String> newInstances = assignments.get(newSeg);
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(newSeg);
      if (instanceStateMap != null) {
        instanceStateMap.clear();
      }
      for (String instance : newInstances) {
        idealState.setPartitionState(newSeg, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
      }
      if (!oldSeg.startsWith("_PINOT_NEW")) {
        Set<String> oldInstances = idealState.getInstanceSet(oldSeg);
        // TODO assert oldInstances is not null.
        for (String instance: oldInstances) {
          idealState.setPartitionState(oldSeg, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
        }
      }
    }

    return idealState;
  }

  private IdealState updateIdealStateOnSegmentCompletion(IdealState idealState,
      TableConfig tableConfig, @Nonnull  String currentSegmentId, String newSegmentId, PartitionAssignment partitionAssignment) {

    Map<String, List<String>> instanceAssignments = null;
    /*
    TODO make this call from commitSegmentMetadata()
      RealtimeSegmentAssignmentStrategy strategy = new ConsumingSegmentAssignmentStrategy();
      assignments = strategy.assign(newSegments, partitionAssignment);
     */

    List<String> newSegmentInstances = instanceAssignments.get(newSegmentId);
    Set<String> currentSegmentInstances = idealState.getInstanceSet(currentSegmentId);
    for (String instance : currentSegmentInstances) {
      idealState.setPartitionState(currentSegmentId, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    }

    // We may have (for whatever reason) a different instance list in the idealstate for the new segment.
    // If so, clear it, and then set the instance state for the set of instances that we know should be there.
//    Map<String, String> stateMap = idealState.getInstanceStateMap(newSegmentId);
//    if (stateMap != null) {
//      stateMap.clear();
//    }
    for (String instance : newSegmentInstances) {
      idealState.setPartitionState(newSegmentId, instance, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
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
